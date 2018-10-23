const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')

const Record = function (handler) {
  this._handler = handler
  this._store = handler._store
  this._prune = handler._prune
  this._lz = handler._lz

  this.name = null
  this.usages = 0
  this.isReady = false
  this.hasProvider = false
  this.version = null
  this.data = null

  this._connection = handler._connection
  this._client = handler._client

  this._stale = null
  this._patchQueue = null
  this._updatePromise = null

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)
}

EventEmitter(Record.prototype)

Object.defineProperty(Record.prototype, '_isConnected', {
  get: function _isConnected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Object.defineProperty(Record.prototype, 'empty', {
  get: function empty () {
    return Object.keys(this.data).length === 0
  }
})

Object.defineProperty(Record.prototype, 'ready', {
  get: function ready () {
    return this.isReady
  }
})

Object.defineProperty(Record.prototype, 'provided', {
  get: function provided () {
    return this.hasProvider
  }
})

Record.prototype.init = function (name) {
  if (typeof name !== 'string' || name.length === 0 || name.includes('[object Object]')) {
    throw new Error('invalid argument name')
  }

  this.isReady = false
  this._ref()

  this.name = name
  this._store.get(name, (err, data, version) => {
    try {
      if (!err && data && version) {
        this.data = data
        this.version = version
        this.emit('update', this)
      }
      this._client.on('connectionStateChanged', this._handleConnectionStateChange)
      this._handleConnectionStateChange()
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err)
    }
  })
}

Record.prototype.get = function (path) {
  invariant(this.usages !== 0, `${this.name} "get" cannot use discarded record`)

  return jsonPath.get(this.data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  invariant(this.usages !== 0, `${this.name} "set" cannot use discarded record`)
  invariant(!this.hasProvider, `${this.name} "set" cannot be called on provided record`)

  if (this.usages === 0 || this.hasProvider) {
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrData
  const data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && (typeof data !== 'object' || data === null)) {
    throw new Error('invalid argument data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument path')
  }

  const newValue = jsonPath.set(this.data, path, data)

  if (this.isReady) {
    if (newValue === this.data) {
      return Promise.resolve()
    }
    this._sendUpdate(newValue)
  } else {
    this._patchQueue = (path && this._patchQueue) || []
    this._patchQueue.push(path, data)
  }

  const oldValue = this.data
  this.data = utils.deepFreeze(newValue)

  if (this.data !== oldValue) {
    this._handler.isAsync = false
    try {
      this.emit('update', this)
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err)
    } finally {
      this._handler.isAsync = true
    }
  }

  return this.whenReady()
}

Record.prototype.update = function (pathOrUpdater, updaterOrNil) {
  invariant(this.usages !== 0, `${this.name} "update" cannot use discarded record`)
  invariant(!this.hasProvider, `${this.name} "update" cannot be called on provided record`)

  if (this.usages === 0 || this.hasProvider) {
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrUpdater
  const updater = arguments.length === 1 ? pathOrUpdater : updaterOrNil

  if (typeof updater !== 'function') {
    throw new Error('invalid argument updater')
  }

  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument path')
  }

  // TODO (refactor)
  this._ref()
  const promise = (this._updatePromise || this.whenReady())
    .then(() => {
      const prev = this.get(path)
      return Promise.resolve(updater(prev)).then(next => [ prev, next ])
    })
    .then(([ prev, next ]) => {
      if (path) {
        this.set(path, next)
      } else if (next != null) {
        this.set(next)
      } else {
        next = prev
      }
      this._unref()
      if (this._updatePromise === promise) {
        this._updatePromise = null
      }
      return next
    })
    .catch(err => {
      this._unref()
      if (this._updatePromise === promise) {
        this._updatePromise = null
      }
      throw err
    })

  this._updatePromise = promise

  return this._updatePromise
}

Record.prototype.whenReady = function () {
  invariant(this.usages !== 0, `${this.name} "whenReady" cannot use discarded record`)

  if (this.usages === 0) {
    return Promise.reject(new Error('discarded'))
  }

  return this.isReady ? Promise.resolve() : new Promise(resolve => this.once('ready', resolve))
}

Record.prototype.ref = function () {
  this.usages += 1

  if (this.usages === 1) {
    this._prune.delete(this)
  }
}

Record.prototype.unref = function () {
  invariant(this.usages !== 0, `${this.name} "discard" cannot use discarded record`)

  this.usages = Math.max(0, this.usages - 1)

  if (this.usages === 0) {
    this._prune.set(this, Date.now())
  }
}

Record.prototype._ref = function () {
  this.ref()
  this._handler._$syncRef()
}

Record.prototype._unref = function () {
  this.unref()
  this._handler._$syncUnref()
}

Record.prototype.acquire = Record.prototype.ref
Record.prototype.discard = Record.prototype.unref
Record.prototype.destroy = Record.prototype.unref

Record.prototype._$destroy = function () {
  invariant(this.usages === 0 && this.isReady, `${this.name} destroy cannot use active or not ready record`)

  if (this._isConnected) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, [this.name])
  }

  this.name = null
  this.usages = 0
  this.isReady = false
  this.hasProvider = false
  this.version = null
  this.data = null

  this._stale = null
  this._patchQueue = null
  this._updatePromise = null

  this._client.off('connectionStateChanged', this._handleConnectionStateChange)

  this.off()
}

Record.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.UPDATE) {
    if (!this.isReady) {
      this._onRead(message.data)
    } else {
      this._onUpdate(message.data)
    }
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_HAS_PROVIDER) {
    this._updateHasProvider(messageParser.convertTyped(message.data[1], this._client))
  }
}

Record.prototype._updateHasProvider = function (hasProvider) {
  if (this.hasProvider !== hasProvider) {
    this.hasProvider = hasProvider
    try {
      this.emit('update', this)
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err)
    }
  }
}

Record.prototype._sendUpdate = function (newValue) {
  invariant(this.isReady, `${this.name}  cannot update non-ready record`)

  let [ start ] = this.version ? this.version.split('-') : [ '0' ]

  if (start === 'INF' || this.hasProvider) {
    return
  }

  start = parseInt(start, 10)
  start = start >= 0 ? start : 0

  const name = this.name
  const nextVersion = `${start + 1}-${xuid()}`
  const prevVersion = this.version || ''
  const connection = this._connection

  this._ref()
  // TODO (perf): Avoid closure allocation.
  this._lz.compress(newValue, (raw, err) => {
    try {
      if (!raw || err) {
        err = err || new Error(this.name)
        this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, newValue)
        return
      }

      connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
        name,
        nextVersion,
        raw,
        prevVersion
      ])
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err)
    } finally {
      this._unref()
    }
  })

  this.version = nextVersion
}

Record.prototype._onUpdate = function (data) {
  const version = data[1]

  if (typeof version !== 'string' || utils.isSameOrNewer(this.version, version)) {
    return
  }

  this._ref()
  // TODO (perf): Avoid closure allocation.
  this._lz.decompress(data[2], (value, err) => {
    try {
      if (!value || err) {
        err = err || new Error(this.name)
        this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, data)
        return
      }

      if (utils.isSameOrNewer(this.version, version)) {
        return
      }

      const oldVersion = this.version
      const oldValue = this.data

      this.version = version
      this.data = jsonPath.set(this.data, undefined, value)

      // NOTE: This should never happen.
      if (!this.version || this.version.indexOf('-') === -1) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, `invalid version`, data)
        this.version = '0-00000000000000'
      }

      if (this.data !== oldValue || this.version !== oldVersion) {
        this.emit('update', this)
      }
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err, data)
    } finally {
      this._unref()
    }
  })
}

Record.prototype._onRead = function (data) {
  if (data[1] == null || data[2] == null) {
    data = this._stale
    this._stale = null
  }

  this._ref()
  // TODO (perf): Avoid closure allocation.
  this._lz.decompress(data[2], (value, err) => {
    try {
      if (!value) {
        err = err || new Error(this.name)
        this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, data)
        return
      }

      const oldIsReady = this.isReady
      const oldVersion = this.version
      const oldValue = this.data

      if (utils.isSameOrNewer(this.version, data[1])) {
        value = this.data
      } else {
        this.version = data[1]
        this.data = value
      }

      // NOTE: This should never happen.
      if (!this.version || this.version.indexOf('-') === -1) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, `invalid version`, data)
        this.version = '0-00000000000000'
      }

      if (this._patchQueue) {
        for (let i = 0; i < this._patchQueue.length; i += 2) {
          this.data = jsonPath.set(this.data, this._patchQueue[i + 0], this._patchQueue[i + 1])
        }
        this._patchQueue = null
      }

      if (!this.isReady) {
        this._unref()
        this.isReady = true
        this.emit('ready')
      }

      if (this.data !== oldValue || this.version !== oldVersion || this.isReady !== oldIsReady) {
        this.emit('update', this)
      }

      if (this.data !== value) {
        this._sendUpdate(this.data)
      }
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.USER_ERROR, err, data)
    } finally {
      this._unref()
    }
  })
}

Record.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    if (this.version) {
      this._stale = [ this.name, this.version, this.data ]
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name, this.version ])
    } else {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name ])
    }
  } else {
    this._updateHasProvider(false)
  }
}

module.exports = Record
