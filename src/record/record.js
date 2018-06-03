const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')

const Record = function (handler) {
  this._handler = handler
  this._cache = handler._cache
  this._prune = handler._prune
  this._lz = handler._lz

  this.name = null
  this.usages = 0
  this.isReady = false
  this.hasProvider = false
  this.version = null
  this.timestamp = null

  this._connection = handler._connection
  this._client = handler._client
  this._changeEmitter = new EventEmitter()

  this._stale = null
  this._data = null
  this._patchQueue = null

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)
}

EventEmitter(Record.prototype)

Object.defineProperty(Record.prototype, '_isConnected', {
  get: function _isConnected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Record.prototype.init = function (name) {
  if (typeof name !== 'string' || name.length === 0 || name.includes('[object Object]')) {
    throw new Error('invalid argument name')
  }

  this.name = name
  this.acquire()
  this._cache.get(name, (err, data, version) => {
    this.discard()

    if (!err && data) {
      this._data = data
      this.version = version
      this._applyChange(this._data)
    }

    this._client.on('connectionStateChanged', this._handleConnectionStateChange)
    this._handleConnectionStateChange()
  })
}

Record.prototype.get = function (path) {
  invariant(this.usages !== 0, `${this.name} "get" cannot use discarded record`)

  return jsonPath.get(this._data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  invariant(this.usages !== 0, `${this.name} "set" cannot use discarded record`)
  invariant(!this.hasProvider, `${this.name} "set" cannot be called on provided record`)

  if (this.usages === 0 || this.hasProvider) {
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrData
  const data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && typeof data !== 'object') {
    throw new Error('invalid argument data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument path')
  }

  const newValue = jsonPath.set(this._data, path, data)

  if (this.isReady) {
    if (newValue === this._data) {
      return Promise.resolve()
    }
    this._sendUpdate(newValue)
  } else {
    this._patchQueue = (path && this._patchQueue) || []
    this._patchQueue.push(path, data)
  }

  const oldValue = this._data
  this._data = utils.deepFreeze(newValue)

  this._handler.isAsync = false
  this._applyChange(this._data, oldValue)
  this._handler.isAsync = true

  return this.whenReady()
}

Record.prototype.subscribe = function (path, callback, triggerNow) {
  invariant(this.usages !== 0, `${this.name} "subscribe" cannot use discarded record`)

  if (this.usages === 0) {
    return
  }

  const args = this._normalizeArguments(arguments)

  if (args.path !== undefined && (typeof args.path !== 'string' || args.path.length === 0)) {
    throw new Error('invalid argument path')
  }
  if (typeof args.callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  this._changeEmitter.on(args.path, args.callback)

  if (args.triggerNow && this._data) {
    args.callback(jsonPath.get(this._data, args.path))
  }
}

Record.prototype.unsubscribe = function (pathOrCallback, callback) {
  invariant(this.usages !== 0, `${this.name} "unsubscribe" cannot use discarded record`)

  if (this.usages === 0) {
    return
  }

  const args = this._normalizeArguments(arguments)

  if (args.path !== undefined && (typeof args.path !== 'string' || args.path.length === 0)) {
    throw new Error('invalid argument path')
  }
  if (args.callback !== undefined && typeof args.callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  this._changeEmitter.off(args.path, args.callback)
}

Record.prototype.whenReady = function () {
  invariant(this.usages !== 0, `${this.name} "whenReady" cannot use discarded record`)

  if (this.usages === 0) {
    return Promise.reject(new Error('discarded'))
  }

  return this.isReady ? Promise.resolve() : new Promise(resolve => this.once('ready', resolve))
}

Record.prototype.acquire = function () {
  this.usages += 1
}

Record.prototype.discard = function () {
  invariant(this.usages !== 0, `${this.name} "discard" cannot use discarded record`)

  this.usages = Math.max(0, this.usages - 1)

  if (this.usages === 0) {
    this.timestamp = Date.now()
    this._prune.add(this)
  }
}

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
  this.timestamp = null

  this._stale = null
  this._data = null
  this._patchQueue = null

  this._client.off('connectionStateChanged', this._handleConnectionStateChange)
  this._changeEmitter.off()

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
    this.emit('hasProviderChanged', this.hasProvider)
  }
}

Record.prototype._invariantVersion = function () {
  invariant(this.version && typeof this.version === 'string', `${this.name} invalid version ${this.version}`)

  if (!this.version || typeof this.version !== 'string') {
    this.version = '0-0000000000'
  }

  const [start, rev] = this.version.split('-')
  invariant((start === 'INF' || parseInt(start, 10) >= 0) && rev, `${this.name} invalid version ${this.version}`)
}

Record.prototype._sendUpdate = function (newValue) {
  invariant(this.isReady, `${this.name}  cannot update non-ready record`)

  this._invariantVersion()

  let [ start ] = this.version.split('-')

  if (start === 'INF' || this.hasProvider) {
    return
  }

  start = parseInt(start, 10)

  invariant(start >= 0, `invalid version ${this.version}`)

  start = start >= 0 ? start : 0

  const name = this.name
  const nextVersion = `${start + 1}-${xuid()}`
  const prevVersion = this.version || ''
  const connection = this._connection

  this._lz.compress(newValue, (raw, err) => {
    if (err) {
      console.error(err)
      return
    }

    connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
      name,
      nextVersion,
      raw,
      prevVersion
    ])
  })

  this.version = nextVersion

  this._invariantVersion()
}

Record.prototype._onUpdate = function (data) {
  const version = data[1]

  if (typeof version !== 'string' || utils.isSameOrNewer(this.version, version)) {
    return
  }

  this.acquire()
  this._lz.decompress(data[2], (value, err) => {
    this.discard()

    if (err) {
      console.error(err)
      return
    }

    if (utils.isSameOrNewer(this.version, version)) {
      return
    }

    this.version = version

    this._invariantVersion()

    const oldValue = this._data
    this._data = jsonPath.set(this._data, undefined, value)
    this._applyChange(this._data, oldValue)
  })
}

Record.prototype._onRead = function (data) {
  if (data[1] == null || data[2] == null) {
    data = this._stale
  }
  this._stale = null

  this.acquire()
  this._lz.decompress(data[2], (value, err) => {
    this.discard()

    if (err) {
      console.error(err)
      return
    }

    if (this.isReady && this._data && utils.isSameOrNewer(this.version, data[1])) {
      return
    }

    this.version = data[1]
    this._invariantVersion()

    const oldValue = this._data
    this._data = value

    if (this._patchQueue) {
      for (let i = 0; i < this._patchQueue.length; i += 2) {
        this._data = jsonPath.set(this._data, this._patchQueue[i + 0], this._patchQueue[i + 1])
      }
      this._patchQueue = null
    }

    this.isReady = true

    try {
      this.emit('ready')

      if (this._data !== oldValue) {
        this._applyChange(this._data, oldValue)
      }
    } catch (err) {
      console.error(err)
    }

    if (this._data !== value) {
      this._sendUpdate(this._data)
    }
  })
}

Record.prototype._applyChange = function (newData, oldData) {
  const paths = this._changeEmitter.eventNames()
  for (let i = 0; i < paths.length; i++) {
    const newValue = jsonPath.get(newData, paths[i])
    const oldValue = jsonPath.get(oldData, paths[i])

    if (newValue !== oldValue) {
      try {
        this._changeEmitter.emit(paths[i], newValue)
      } catch (err) {
        console.error(err)
      }
    }
  }
}

Record.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    if (this.version) {
      this._stale = [ this.name, this.version, this._data ]
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [this.name, this.version])
    } else {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [this.name])
    }
  }
}

Record.prototype._normalizeArguments = function (args) {
  const result = {}

  for (let i = 0; i < args.length; i++) {
    if (typeof args[i] === 'string') {
      result.path = args[i]
    } else if (typeof args[i] === 'function') {
      result.callback = args[i]
    } else if (typeof args[i] === 'boolean') {
      result.triggerNow = args[i]
    }
  }

  return result
}

module.exports = Record
