'use strict'

const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')

const Record = function (name, connection, client, cache, prune, lz) {
  invariant(connection, 'missing connection')
  invariant(client, 'missing client')
  invariant(cache, 'missing cache')
  invariant(prune, 'missing prune')
  invariant(lz, 'missing lz')

  if (typeof name !== 'string' || name.length === 0 || name.includes('[object Object]')) {
    throw new Error('invalid argument name')
  }

  this._lz = lz
  this._cache = cache
  this._prune = prune

  const [ version, _data ] = this._cache.get(name) || [null, null]

  this.name = name
  this.usages = 0
  this.isDestroyed = false
  this.isSubscribed = false
  this.isReady = false
  this.hasProvider = false
  this.version = version

  this._connection = connection
  this._client = client
  this._eventEmitter = new EventEmitter()

  this._stale = null
  this._data = _data
  this._patchQueue = null

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._sendRead()
}

EventEmitter(Record.prototype)

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
  this._applyChange(this._data, oldValue)

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

  this._eventEmitter.on(args.path, args.callback)

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

  this._eventEmitter.off(args.path, args.callback)
}

Record.prototype.whenReady = function () {
  invariant(this.usages !== 0, `${this.name} "whenReady" cannot use discarded record`)

  if (this.usages === 0) {
    return Promise.reject(new Error('discarded'))
  }

  if (this.isReady) {
    return Promise.resolve()
  }

  return new Promise(resolve => this.once('ready', resolve))
}

Record.prototype.acquire = function () {
  this.usages += 1
  if (this.usages === 1) {
    this._prune.delete(this)
  }
}

Record.prototype.discard = function () {
  invariant(this.usages !== 0, `${this.name} "discard" cannot use discarded record`)

  this.usages = Math.max(0, this.usages - 1)

  if (this.usages === 0) {
    this._prune.set(this, Date.now())
  }
}

Record.prototype._$destroy = function () {
  invariant(!this.isDestroyed, `${this.name} "destroy" cannot use destroyed record`)
  invariant(this.usages === 0 && this.isReady, `${this.name} destroy cannot use active or not ready record`)

  if (this._hasVersion()) {
    this._cache.set(this.name, [this.version, this._data])
  }

  if (this.isSubscribed) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, [this.name])
    this.isSubscribed = false
  }

  this.usages = 0
  this.isDestroyed = true

  this._client.off('connectionStateChanged', this._handleConnectionStateChange)
  this._eventEmitter.off()

  this.off()
}

Record.prototype._$onMessage = function (message) {
  invariant(!this.isDestroyed, `${this.name} "_$onMessage" cannot use destroyed record`)

  if (this.isDestroyed) {
    return
  }

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

Record.prototype._hasVersion = function () {
  const version = this.version && this.version.split('-', 1)[0]
  return version && (version === 'INF' || parseInt(version) > 0)
}

Record.prototype._invariantVersion = function () {
  invariant(this.version && typeof this.version === 'string', `${this.name} invalid version ${this.version}`)

  if (!this.version || typeof this.version !== 'string') {
    this.version = '0-0000000000'
  }

  const [start, rev] = this.version.split('-')
  invariant((start === 'INF' || parseInt(start, 10) >= 0) && rev, `${this.name} invalid version ${this.version}`)
}

Record.prototype._sendRead = function () {
  if (this.isSubscribed || this._connection.getState() !== C.CONNECTION_STATE.OPEN) {
    return
  }
  if (this._hasVersion()) {
    this._stale = this._data
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [this.name, this.version])
  } else {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [this.name])
  }
  this.isSubscribed = true
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
  if (data[1] == null) {
    data[1] = this.version
    data[2] = this._stale
  }

  this.acquire()
  this._lz.decompress(data[2], (value, err) => {
    this.discard()

    if (err) {
      console.error(err)
      return
    }

    this.version = data[1]
    this._invariantVersion()

    this._stale = null

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
  const paths = this._eventEmitter.eventNames()
  for (let i = 0; i < paths.length; i++) {
    const newValue = jsonPath.get(newData, paths[i])
    const oldValue = jsonPath.get(oldData, paths[i])

    if (newValue !== oldValue) {
      this._eventEmitter.emit(paths[i], newValue)
    }
  }
}

Record.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    this._sendRead()
  } else if (state === C.CONNECTION_STATE.RECONNECTING) {
    this.isSubscribed = false
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
