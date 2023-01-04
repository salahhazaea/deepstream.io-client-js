const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')

class Record {
  static STATE = C.RECORD_STATE

  constructor(name, handler) {
    this._handler = handler

    this._name = name
    this._version = ''
    this._data = jsonPath.EMPTY
    this._state = Record.STATE.VOID
    this._refs = 1
    this._subscribed = false
    this._subscriptions = []

    this._updating = null
    this._patches = null

    this._subscribe()
  }

  get name() {
    return this._name
  }

  get version() {
    return this._version
  }

  get data() {
    return this._data
  }

  get state() {
    return this._state
  }

  get refs() {
    return this._refs
  }

  ref() {
    this._refs += 1
    this._handler._onRef(this)
    if (this._refs === 1) {
      this._subscribe()
    }
  }

  unref() {
    invariant(this._refs > 0, this._name + ' missing refs')

    this._refs -= 1
    this._handler._onRef(this)
  }

  subscribe(fn) {
    this._subscriptions.push(fn)
  }

  unsubscribe(fn) {
    const idx = this._subscriptions.indexOf(fn)
    if (idx !== -1) {
      this._subscriptions.splice(idx, 1)
    }
  }

  get(path) {
    return jsonPath.get(this._data, path)
  }

  set(pathOrData, dataOrNil) {
    invariant(this._refs > 0, this._name + ' missing refs')

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

    const prevData = this._data
    const prevVersion = this._version
    const prevState = this._state

    if (this._state < Record.STATE.SERVER) {
      this._patches = this._patches && path ? this._patches : []
      this._patches.push(path, jsonPath.jsonClone(data))
      this._handler._patch.add(this)

      this._version = this._makeVersion(this._version ? parseInt(this._version) + 1 : 1)
      this._data = jsonPath.set(this._data, path, data, true)
    } else {
      this._update(path, jsonPath.jsonClone(data))
    }

    if (this._data !== prevData || this._version !== prevVersion || this._state !== prevState) {
      this._emitUpdate()
    }
  }

  when(stateOrNull) {
    invariant(this._refs > 0, this._name + ' missing refs')

    const state = stateOrNull == null ? Record.STATE.SERVER : stateOrNull

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

        this.unsubscribe(onUpdate)
        this.unref()

        resolve(null)
      }

      this.ref()
      this.subscribe(onUpdate)
    })
  }

  update(pathOrUpdater, updaterOrNil) {
    invariant(this._refs > 0, this._name + ' missing refs')

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
    return this.when(Record.STATE.SERVER)
      .then(() => {
        const prev = this.get(path)
        const next = updater(prev, this._version)
        this.set(path, next)
      })
      .finally(() => {
        this.unref()
      })
  }

  _emitUpdate() {
    for (const fn of this._subscriptions.slice()) {
      fn(this)
    }
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

  _$onConnectionStateChange() {
    const prevState = this._state

    const connection = this._handler._connection
    if (connection.connected) {
      if (this._refs > 0) {
        this._subscribe()
      }

      if (this._updating) {
        for (const update of this._updating.values()) {
          connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)
        }
      }
    } else {
      this._subscribed = false
      this._state = Record.STATE.CLIENT
    }

    if (this._state !== prevState) {
      this._emitUpdate()
    }
  }

  _unsubscribe() {
    invariant(!this._refs, this._name + ' must not have refs')
    invariant(!this._patches, this._name + ' must not have patch queue')

    const prevState = this._state

    const connection = this._handler._connection
    if (this._subscribed && connection.connected) {
      connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this._name)
    }

    this._subscribed = false
    this._state = Record.STATE.CLIENT

    if (this._state !== prevState) {
      this._emitUpdate()
    }
  }

  _subscribe() {
    invariant(this._refs, this._name + ' missing refs')

    const connection = this._handler._connection
    if (!this._subscribed && connection.connected) {
      connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.SUBSCRIBE, this._name)
      this._subscribed = true
    }
  }

  _update(path, data) {
    invariant(this._version, this._name + ' missing version')
    invariant(this._data, this._name + ' missing data')

    const connection = this._handler._connection

    const prevData = this._data
    const nextData = jsonPath.set(prevData, path, data, true)

    if (nextData !== prevData) {
      const prevVersion = this._version
      const nextVersion = this._makeVersion(parseInt(prevVersion) + 1)

      const update = [this._name, nextVersion, JSON.stringify(nextData), prevVersion]

      connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)

      this._version = nextVersion

      this._updating ??= new Map()
      this._updating.set(nextVersion, update)
      this._handler._stats.updating += 1
    }
  }

  _onUpdate([, version, data]) {
    const prevData = this._data
    const prevVersion = this._version
    const prevState = this._state

    if (this._updating?.delete(version)) {
      this._handler._stats.updating -= 1
    }

    const cmp = utils.compareRev(version, this._version)
    if (this._patches || cmp > 0 || (cmp !== 0 && version.charAt(0) === 'I')) {
      if (data === '{}') {
        this._data = jsonPath.EMPTY_OBJ
      } else if (data === '[]') {
        this._data = jsonPath.EMPTY_ARR
      } else {
        this._data = jsonPath.set(this._data, null, JSON.parse(data), true)
      }

      this._version = version
    }

    if (this._patches) {
      if (this._version.charAt(0) !== 'I') {
        for (let i = 0; i < this._patches.length; i += 2) {
          this._update(this._patches[i + 0], this._patches[i + 1])
        }
      } else if (this._patches.length) {
        this._error(C.EVENT.USER_ERROR, 'cannot patch provided value')
      }

      this._patches = null
      this._handler._patch.delete(this)
    }

    if (this._state < Record.STATE.SERVER) {
      this._state = this._version.charAt(0) === 'I' ? Record.STATE.STALE : Record.STATE.SERVER
    }

    if (this._data !== prevData || this._version !== prevVersion || this._state !== prevState) {
      this._emitUpdate()
    }
  }

  _onSubscriptionHasProvider([, hasProvider]) {
    if (this._state < Record.STATE.SERVER) {
      return
    }

    const provided = hasProvider && messageParser.convertTyped(hasProvider, this._handler._client)
    const state = provided
      ? Record.STATE.PROVIDER
      : this._version.charAt(0) === 'I'
      ? Record.STATE.STALE
      : Record.STATE.SERVER

    if (this._state !== state) {
      this._state = state
      this._emitUpdate()
    }
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
    return this._state >= Record.STATE.SERVER
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
    return this._state >= Record.STATE.SERVER
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'hasProvider', {
  get: function hasProvider() {
    return this.state >= C.RECORD_STATE.PROVIDER
  },
})

module.exports = Record
