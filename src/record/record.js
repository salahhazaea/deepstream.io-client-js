const jsonPath = require('@nxtedition/json-path')
const utils = require('../utils/utils')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')
const cloneDeep = require('lodash.clonedeep')

class Record {
  static STATE = C.RECORD_STATE

  constructor(name, handler) {
    this._handler = handler

    this._name = name
    this._version = ''
    this._data = jsonPath.EMPTY
    this._state = C.RECORD_STATE.VOID
    this._refs = 0
    this._subscriptions = []
    this._emitting = false
    /** @type Map? */ this._updating = null
    /** @type Array? */ this._patching = null
    this._subscribed = this._sendMsg1(C.ACTIONS.SUBSCRIBE, this._name)
  }

  /** @type {string} */
  get name() {
    return this._name
  }

  /** @type {string} */
  get version() {
    return this._version
  }

  /** @type {Object} */
  get data() {
    return this._data
  }

  /** @type {Number} */
  get state() {
    return this._state
  }

  /** @type {Number} */
  get refs() {
    return this._refs
  }

  /**
   * @returns {Record}
   */
  ref() {
    this._refs += 1
    if (this._refs === 1) {
      this._handler._onPruning(this, false)
      this._subscribed = this._subscribed || this._sendMsg1(C.ACTIONS.SUBSCRIBE, this._name)
    }
    return this
  }

  /**
   * @returns {Record}
   */
  unref() {
    this._refs -= 1
    if (this._refs === 0) {
      this._handler._onPruning(this, true)
    }
    return this
  }

  /**
   * @param {*} fn
   * @param {*} opaque
   * @returns {Record}
   */
  subscribe(fn, opaque = null) {
    if (this._emitting) {
      this._subscriptions = this._subscriptions.slice()
      this._emitting = false
    }

    this._subscriptions.push(fn, opaque)

    return this
  }

  /**
   *
   * @param {*} fn
   * @param {*} opaque
   * @returns {Record}
   */
  unsubscribe(fn, opaque = null) {
    if (this._emitting) {
      this._subscriptions = this._subscriptions.slice()
      this._emitting = false
    }

    let idx = -1

    const arr = this._subscriptions
    const len = arr.length
    for (let n = 0; n < len; n += 2) {
      if (arr[n + 0] === fn && arr[n + 1] === opaque) {
        idx = n
        break
      }
    }

    if (idx !== -1) {
      this._subscriptions.splice(idx, 2)
    }

    return this
  }

  get(path) {
    return path ? jsonPath.get(this._data, path) : this._data
  }

  set(pathOrData, dataOrNil) {
    const prevData = this._data
    const prevVersion = this._version

    invariant(this._refs > 0, 'missing refs')

    if (this._version.charAt(0) === 'I' || this._name.startsWith('_')) {
      this._error(C.EVENT.USER_ERROR, 'cannot set')
      return
    }

    const path = arguments.length === 1 ? undefined : pathOrData
    const data = arguments.length === 1 ? pathOrData : dataOrNil

    if (path === undefined && !utils.isPlainObject(data)) {
      throw new Error('invalid argument: data')
    }
    if (path === undefined && Object.keys(data).some((prop) => prop.startsWith('_'))) {
      throw new Error('invalid argument: data')
    }
    if (
      path !== undefined &&
      (typeof path !== 'string' || path.length === 0 || path.startsWith('_')) &&
      (!Array.isArray(path) || path.length === 0 || path[0].startsWith('_'))
    ) {
      throw new Error('invalid argument: path')
    }

    if (this._state < C.RECORD_STATE.SERVER) {
      if (this._patching == null) {
        this._onPatching(true)
      } else if (path) {
        this._patching.splice(0)
      }

      if (this._patching) {
        this._patching.push(path, cloneDeep(data))
      } else {
        throw new Error('invalid state')
      }
    } else {
      this._update(jsonPath.set(this._data, path, data, false))
    }

    if (this._data !== prevData || this._version !== prevVersion) {
      this._emitUpdate()
    }
  }

  // TODO (fix): timeout + signal
  when(stateOrNull) {
    invariant(this._refs > 0, 'missing refs')

    const state = stateOrNull == null ? C.RECORD_STATE.SERVER : stateOrNull

    if (!Number.isFinite(state) || state < 0) {
      throw new Error('invalid argument: state')
    }

    return new Promise((resolve) => {
      if (this._state >= state) {
        resolve(null)
        return
      }

      const onUpdate = () => {
        if (this._state < state) {
          return
        }

        this.unref()
        this.unsubscribe(onUpdate)

        resolve(null)
      }

      this.ref()
      this.subscribe(onUpdate)
    })
  }

  update(pathOrUpdater, updaterOrNil) {
    invariant(this._refs > 0, 'missing refs')

    if (this._version.charAt(0) === 'I') {
      this._handler._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [
        this._name,
        this._version,
        this._state,
      ])
      return Promise.resolve()
    }

    const path = arguments.length === 1 ? undefined : pathOrUpdater
    const updater = arguments.length === 1 ? pathOrUpdater : updaterOrNil

    if (typeof updater !== 'function') {
      throw new Error('invalid argument: updater')
    }

    if (
      path !== undefined &&
      (typeof path !== 'string' || path.length === 0 || path.startsWith('_')) &&
      (!Array.isArray(path) || path.length === 0 || path[0].startsWith('_'))
    ) {
      throw new Error('invalid argument: path')
    }

    this.ref()
    return this.when(C.RECORD_STATE.SERVER)
      .then(() => {
        const prev = this.get(path)
        const next = updater(prev, this._version)
        this.set(path, next)
      })
      .finally(() => {
        this.unref()
      })
  }

  _$onMessage(message) {
    if (message.action === C.ACTIONS.UPDATE) {
      this._onUpdate(message.data)
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_HAS_PROVIDER) {
      this._onSubscriptionHasProvider(message.data)
    } else {
      return false
    }

    return true
  }

  _$onConnectionStateChange(connected) {
    if (connected) {
      this._subscribed = this._refs > 0 && this._sendMsg1(C.ACTIONS.SUBSCRIBE, this._name)

      if (this._updating) {
        for (const update of this._updating.values()) {
          this._sendMsg1(C.ACTIONS.UPDATE, update)
        }
      }
    } else {
      this._subscribed = false
    }

    if (this._state > C.RECORD_STATE.CLIENT) {
      this._state = C.RECORD_STATE.CLIENT
      this._emitUpdate()
    }
  }

  _$dispose() {
    invariant(!this._refs, 'must not have refs')
    invariant(!this._patching, 'must not have patches')
    invariant(!this._updating, 'must not have updates')

    if (this._subscribed) {
      this._sendMsg1(C.ACTIONS.UNSUBSCRIBE, this._name)
      this._subscribed = false
    }

    if (this._state > C.RECORD_STATE.CLIENT) {
      this._state = C.RECORD_STATE.CLIENT
      this._emitUpdate()
    }
  }

  _update(nextData) {
    invariant(this._version, 'must have version')

    const connection = this._handler._connection

    if (nextData === this._data) {
      return
    }

    const prevVersion = this._version
    const nextVersion = this._makeVersion(parseInt(prevVersion) + 1)

    const update = [this._name, nextVersion, jsonPath.stringify(nextData), prevVersion]

    if (!this._updating) {
      this._onUpdating(true)
    }

    if (this._updating) {
      this._updating.set(nextVersion, update)
    } else {
      throw new Error('invalid state')
    }

    connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)

    this._data = nextData
    this._version = nextVersion
  }

  _onUpdate([, version, data]) {
    const prevData = this._data
    const prevVersion = this._version
    const prevState = this._state

    if (this._updating && this._updating.delete(version) && this._updating.size === 0) {
      this._onUpdating(false)
    }

    if (
      version.charAt(0) === 'I'
        ? this._version !== version
        : utils.compareRev(version, this._version) > 0
    ) {
      this._data = jsonPath.set(this._data, null, jsonPath.parse(data), true)
      this._version = version
    }

    invariant(this._version, 'must have version')
    invariant(this._data, 'must have data')

    if (this._patching != null) {
      if (this._version.charAt(0) !== 'I') {
        let patchData = this._data
        for (let n = 0; n < this._patching.length; n += 2) {
          patchData = jsonPath.set(patchData, this._patching[n + 0], this._patching[n + 1], false)
        }
        this._update(patchData)
      }

      this._onPatching(false)
    }

    if (this._state < C.RECORD_STATE.SERVER) {
      this._state = this._version.charAt(0) === 'I' ? C.RECORD_STATE.STALE : C.RECORD_STATE.SERVER
    }

    if (this._state !== prevState || this._data !== prevData || this._version !== prevVersion) {
      this._emitUpdate()
    }
  }

  _onPatching(value) {
    invariant(this._refs > 0, 'missing refs')

    if (value) {
      this._patching = []
      this.ref()
    } else {
      this._patching = null
      this.unref()
    }

    this._handler._onPatching(this, value)
  }

  _onUpdating(value) {
    invariant(this._refs > 0, 'missing refs')

    if (value) {
      this._updating = new Map()
      this.ref()
    } else {
      this._updating = null
      this.unref()
    }

    this._handler._onUpdating(this, value)
  }

  _onSubscriptionHasProvider([, hasProvider]) {
    if (this._state < C.RECORD_STATE.SERVER) {
      return
    }

    const prevState = this._state

    this._state =
      hasProvider && messageParser.convertTyped(hasProvider, this._handler._client)
        ? C.RECORD_STATE.PROVIDER
        : this._version.charAt(0) === 'I'
        ? C.RECORD_STATE.STALE
        : C.RECORD_STATE.SERVER

    if (this._state !== prevState) {
      this._emitUpdate()
    }
  }

  _sendMsg1(action, data) {
    return this._handler._connection.sendMsg1(C.TOPIC.RECORD, action, data)
  }

  _error(event, msgOrError, data) {
    this._handler._client._$onError(C.TOPIC.RECORD, event, msgOrError, [
      ...(Array.isArray(data) ? data : []),
      this._name,
      this._version,
      this._state,
    ])
  }

  _makeVersion(start) {
    let revid = `${xuid()}-${this._handler._client.user || ''}`
    if (revid.length === 32 || revid.length === 16) {
      // HACK: https://github.com/apache/couchdb/issues/2015
      revid += '-'
    }
    return `${start}-${revid}`
  }

  _emitUpdate() {
    this._emitting = true

    const arr = this._subscriptions
    const len = arr.length

    for (let n = 0; n < len; n += 2) {
      arr[n + 0](this, arr[n + 1])
    }

    this._emitting = false
  }
}

// Compat

Record.prototype.acquire = Record.prototype.ref
Record.prototype.discard = Record.prototype.unref
Record.prototype.destroy = Record.prototype.unref

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'connected', {
  get: function connected() {
    return this._handler._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'empty', {
  get: function empty() {
    return Object.keys(this.data).length === 0
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'ready', {
  get: function ready() {
    return this._state >= C.RECORD_STATE.SERVER
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'provided', {
  get: function provided() {
    return this.state >= C.RECORD_STATE.PROVIDER
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'usages', {
  get: function usages() {
    return this._refs
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'stale', {
  get: function ready() {
    return !this.version
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'isReady', {
  get: function isReady() {
    return this._state >= C.RECORD_STATE.SERVER
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'hasProvider', {
  get: function hasProvider() {
    return this.state >= C.RECORD_STATE.PROVIDER
  },
})

module.exports = Record
