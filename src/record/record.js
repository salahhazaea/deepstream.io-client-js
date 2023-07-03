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
    this._state = Record.STATE.VOID
    this._refs = 0
    this._subscriptions = []
    this._subscriptionsEmitting = false
    this._updating = null
    this._patches = null
    this._subscribed = false
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
    if (this._refs === 1) {
      this._handler._onPruning(this, false)
    }

    if (this._refs === 1 && this._handler._connection.connected && !this._subscribed) {
      this._subscribe()
    }

    return this
  }

  unref() {
    invariant(this._refs > 0, 'missing refs')
    invariant(this._refs > 1 || !this._patches, 'must not have patches')
    invariant(this._refs > 1 || this._state >= Record.STATE.SERVER, 'must be ready')

    this._refs -= 1
    if (this._refs === 0) {
      this._handler._onPruning(this, true)
    }

    return this
  }

  subscribe(fn) {
    if (this._subscriptionsEmitting) {
      this._subscriptions = this._subscriptions.slice()
      this._subscriptionsEmitting = false
    }

    this._subscriptions.push(fn)

    return this
  }

  unsubscribe(fn) {
    if (this._subscriptionsEmitting) {
      this._subscriptions = this._subscriptions.slice()
      this._subscriptionsEmitting = false
    }

    const idx = this._subscriptions.indexOf(fn)
    if (idx !== -1) {
      this._subscriptions.splice(idx, 1)
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

    if (this._state < Record.STATE.SERVER) {
      if (!this._patches) {
        this.ref()
        this._patches = []
      } else if (path) {
        this._patches.splice(0)
      }

      this._patches.push(path, cloneDeep(data))
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
    if (this._handler._connection.connected) {
      if (this._refs > 0) {
        this._subscribe()
      }

      if (this._updating) {
        for (const update of this._updating.values()) {
          this._handler._connection.connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)
        }
      }
    } else {
      this._subscribed = false
    }

    if (this._state > Record.STATE.CLIENT) {
      this._state = Record.STATE.CLIENT
      this._emitUpdate()
    }
  }

  _subscribe() {
    invariant(this._handler._connection.connected, 'must be connected')

    this._handler._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.SUBSCRIBE, this._name)
    this._subscribed = true

    this.ref()
    this._handler._onPending(this, true)
  }

  _$dispose() {
    invariant(!this._refs, 'must not have refs')
    invariant(!this._patches, 'must not have patches')
    invariant(!this._updating, 'must not have updates')
    invariant(this.state >= C.RECORD_STATE.SERVER, 'must not be pending')
    invariant(
      !this._subscribed || this._handler._connection.connected,
      'must be unsubscribed or connected'
    )

    if (this._subscribed) {
      this._handler._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this._name)
      this._subscribed = false
    }

    if (this._state > C.RECORD_STATE.CLIENT) {
      this._state = C.RECORD_STATE.CLIENT
      this._emitUpdate()
    }
  }

  _update(nextData) {
    invariant(this._version, 'must have version')

    if (nextData === this._data) {
      return
    }

    const prevVersion = this._version
    const nextVersion = this._makeVersion(parseInt(prevVersion) + 1)

    const update = [this._name, nextVersion, jsonPath.stringify(nextData), prevVersion]

    if (!this._updating) {
      this._updating = new Map()
      this.ref()
    }

    this._updating.set(nextVersion, update)
    this._handler._stats.updating += 1

    const connection = this._handler._connection
    if (connection.connected) {
      connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)
    }

    this._data = nextData
    this._version = nextVersion
  }

  _onUpdate([, version, data]) {
    const prevData = this._data
    const prevVersion = this._version
    const prevState = this._state

    if (this._updating?.delete(version)) {
      this._handler._stats.updating -= 1
      if (this._updating.size === 0) {
        this._updating = null
        this.unref()
      }
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

    if (this._patches) {
      if (this._version.charAt(0) !== 'I') {
        let patchData = this._data
        for (let n = 0; n < this._patches.length; n += 2) {
          patchData = jsonPath.set(patchData, this._patches[n + 0], this._patches[n + 1], false)
        }
        this._update(patchData)
      }

      this._patches = null
      this.unref()
    }

    if (this._state < C.RECORD_STATE.SERVER) {
      this._state = this._version.charAt(0) === 'I' ? C.RECORD_STATE.STALE : C.RECORD_STATE.SERVER
      this._handler._onPending(this, false)
      this.unref()
    }

    if (this._state !== prevState || this._data !== prevData || this._version !== prevVersion) {
      this._emitUpdate()
    }
  }

  _onSubscriptionHasProvider([, hasProvider]) {
    const prevState = this._state

    this._state =
      hasProvider &&
      messageParser.convertTyped(hasProvider, this._handler._client) &&
      this._state >= C.RECORD_STATE.SERVER
        ? C.RECORD_STATE.PROVIDER
        : this._version.charAt(0) === 'I'
        ? C.RECORD_STATE.STALE
        : C.RECORD_STATE.SERVER

    if (this._state !== prevState) {
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

  _emitUpdate() {
    this._subscriptionsEmitting = true
    try {
      for (const fn of this._subscriptions) {
        try {
          fn(this)
        } catch (err) {
          this._error(
            C.EVENT.USER_ERROR,
            Object.assign(new Error('user callback failed'), { cause: err })
          )
        }
      }
    } finally {
      this._subscriptionsEmitting = false
    }
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
