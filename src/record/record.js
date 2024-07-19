const jsonPath = require('@nxtedition/json-path')
const utils = require('../utils/utils')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')
const cloneDeep = require('lodash.clonedeep')
const timers = require('../utils/timers')

class Record {
  static STATE = C.RECORD_STATE

  constructor(name, handler) {
    const connection = handler._connection

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
    this._subscribed = connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.SUBSCRIBE, this._name)
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
    const connection = this._handler._connection

    this._refs += 1
    if (this._refs === 1) {
      this._handler._onPruning(this, false)
      this._subscribed =
        this._subscribed || connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.SUBSCRIBE, this._name)
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
    if (!path) {
      return this._data
    } else if (typeof path === 'string' || Array.isArray(path)) {
      return jsonPath.get(this._data, path)
    } else if (typeof path === 'function') {
      return path(this._data)
    } else {
      throw new Error('invalid argument: path')
    }
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

  when(stateOrNil, optionsOrNil) {
    invariant(this._refs > 0, 'missing refs')

    if (stateOrNil != null && stateOrNil === 'object') {
      optionsOrNil = stateOrNil
      stateOrNil = optionsOrNil?.state
    }

    const signal = optionsOrNil?.signal
    const state = stateOrNil ?? C.RECORD_STATE.SERVER
    const timeout = optionsOrNil?.timeout ?? 2 * 60e3

    if (signal?.aborted) {
      return Promise.reject(signal.reason || new utils.AbortError())
    }

    if (!Number.isFinite(state) || state < 0) {
      return Promise.reject(new Error('invalid argument: state'))
    }

    return new Promise((resolve, reject) => {
      if (this._state >= state) {
        resolve(this)
        return
      }

      let timeoutHandle

      const onDone = (err) => {
        if (err) {
          reject(err)
        } else {
          resolve(this)
        }

        this.unref()
        this.unsubscribe(onUpdate)

        if (timeoutHandle) {
          timers.clearTimeout(timeoutHandle)
          timeoutHandle = null
        }

        signal?.removeEventListener('abort', onAbort)
      }

      const onUpdate = () => {
        if (this._state >= state) {
          onDone(null)
        }
      }

      const onAbort = signal
        ? () => {
            onDone(signal.reason ?? new utils.AbortError())
          }
        : null

      if (timeout > 0) {
        timeoutHandle = timers.setTimeout(() => {
          const expected = C.RECORD_STATE_NAME[state]
          const current = C.RECORD_STATE_NAME[this._state]

          onDone(
            Object.assign(new Error(`timeout  ${this.name} [${current}<${expected}]`), {
              code: 'ETIMEDOUT',
            })
          )
        }, timeout)
      }

      signal?.addEventListener('abort', onAbort)

      this.ref()
      this.subscribe(onUpdate)
    })
  }

  update(...args) {
    invariant(this._refs > 0, 'missing refs')

    if (this._version.charAt(0) === 'I') {
      this._handler._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [
        this._name,
        this._version,
        this._state,
      ])
      return Promise.resolve()
    }

    const options = args.at(-1) != null && typeof args.at(-1) === 'object' ? args.pop() : null
    const path = args.length === 1 ? undefined : args[0]
    const updater = args.length === 1 ? args[0] : args[1]

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

    if (options?.signal?.aborted) {
      return Promise.reject(options.signal.reason || new utils.AbortError())
    }

    this.ref()
    return this.when(C.RECORD_STATE.SERVER, options)
      .then(() => {
        const prev = this.get(path)
        const next = updater(prev, this._version)
        if (prev !== next) {
          this.set(path, next)
        }
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
    const connection = this._handler._connection

    if (connected) {
      this._subscribed =
        this._refs > 0 && connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.SUBSCRIBE, this._name)

      if (this._updating) {
        for (const update of this._updating.values()) {
          connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, update)
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
    const connection = this._handler._connection

    invariant(!this._refs, 'must not have refs')
    invariant(!this._patching, 'must not have patches')
    invariant(!this._updating, 'must not have updates')

    if (this._subscribed) {
      connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this._name)
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
