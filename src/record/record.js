'use strict'

const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')
const lz = require('@nxtedition/lz-string')

const Record = function (name, connection, client, cache) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }

  this._cache = cache

  const [ version, _data ] = this._cache.get(name) || [ null, null ]

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
  invariant(this.usages !== 0, `"get" cannot use discarded record ${this.name}`)

  return jsonPath.get(this._data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  invariant(this.usages !== 0, `"set" cannot use discarded record ${this.name}`)
  invariant(!this.hasProvider, `"set" cannot be called on provided record ${this.name}`)

  if (this.usages === 0) {
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

  if (newValue === this._data) {
    return Promise.resolve()
  }

  if (this.isReady) {
    this._sendUpdate(newValue)
  } else {
    this._patchQueue = (path && this._patchQueue) || []
    this._patchQueue.push(path, data)
  }

  this._applyChange(newValue)

  return this.whenReady()
}

Record.prototype.subscribe = function (path, callback, triggerNow) {
  invariant(this.usages !== 0, `"subscribe" cannot use discarded record ${this.name}`)

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
  invariant(this.usages !== 0, `"unsubscribe" cannot use discarded record ${this.name}`)

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
  invariant(this.usages !== 0, `"whenReady" cannot use discarded record ${this.name}`)

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
}

Record.prototype.discard = function () {
  invariant(this.usages !== 0, `"discard" cannot use discarded record ${this.name}`)

  this.usages = Math.max(0, this.usages - 1)

  if (this._hasVersion()) {
    this._cache.set(this.name, [ this.version, this._data ])
  }
}

Record.prototype._$destroy = function () {
  invariant(!this.isDestroyed, `"destroy" cannot use destroyed record ${this.name}`)
  invariant(this.usages === 0 && this.isReady, `destroy cannot use active or not ready record ${this.name}`)

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
  invariant(!this.isDestroyed, `"_$onMessage" cannot use destroyed record ${this.name}`)

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
    this.hasProvider = messageParser.convertTyped(message.data[1], this._client)
    this.emit('hasProviderChanged', this.hasProvider)
  }
}

Record.prototype._hasVersion = function () {
  return this.version && parseInt(this.version.split('-', 1)[0]) > 0
}

Record.prototype._sendRead = function () {
  if (this.isSubscribed || this._connection.getState() !== C.CONNECTION_STATE.OPEN) {
    return
  }
  if (this._hasVersion()) {
    this._stale = this._data
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name, this.version ])
  } else {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name ])
  }
  this.isSubscribed = true
}

Record.prototype._sendUpdate = function (newValue) {
  let start = this.version ? this.version.split('-', 1)[0] : 0

  invariant(start !== 'INF' && !this.hasProvider, `cannot update provided record ${this.name}`)

  if (start === 'INF' || this.hasProvider) {
    return
  }

  start = parseInt(start, 10)

  invariant(start >= 0, `invalid version ${start}`)

  start = start >= 0 ? start : 0

  const version = `${start + 1}-${xuid()}`

  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
    this.name,
    version,
    lz.compressToUTF16(JSON.stringify(newValue)),
    this.version || ''
  ])

  this.version = version
}

Record.prototype._onUpdate = function (data) {
  const version = data[1]

  if (!version || utils.isSameOrNewer(this.version, version)) {
    return
  }

  const value = typeof data[2] === 'string'
    ? JSON.parse(lz.decompressFromUTF16(data[2]))
    : data[2]

  this.version = version
  this._applyChange(jsonPath.set(this._data, undefined, value))
}

Record.prototype._onRead = function (data) {
  let oldValue
  if (data[1] == null) {
    oldValue = this._stale
    this._stale = null
  } else {
    oldValue = typeof data[2] === 'string'
      ? JSON.parse(lz.decompressFromUTF16(data[2]))
      : data[2]
    this.version = data[1]
  }

  let newValue = oldValue
  if (this._patchQueue) {
    for (let i = 0; i < this._patchQueue.length; i += 2) {
      newValue = jsonPath.set(newValue, this._patchQueue[i + 0], this._patchQueue[i + 1])
    }
    this._patchQueue = null
  }

  this.isReady = true

  if (newValue !== oldValue) {
    this._sendUpdate(newValue)
  }

  this._applyChange(newValue)

  this.emit('ready')
}

Record.prototype._applyChange = function (newData) {
  const oldData = this._data
  this._data = utils.deepFreeze(newData)

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
  const result = Object.create(null)

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
