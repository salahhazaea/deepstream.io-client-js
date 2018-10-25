const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')
const lz = require('@nxtedition/lz-string')

const EMPTY_OBJ = jsonPath.EMPTY
const EMPTY_BODY = lz.compressToUTF16(JSON.stringify(EMPTY_OBJ))

const Record = function (name, handler) {
  if (typeof name !== 'string' || name.length === 0 || name.includes('[object Object]')) {
    throw new Error('invalid argument name')
  }

  this._handler = handler
  this._prune = handler._prune
  this._lz = handler._lz

  this.name = name
  this.usages = 0
  this.hasProvider = false
  this.version = null
  this.data = null

  this._connection = handler._connection
  this._client = handler._client

  this._patchQueue = []
  this._updateQueue = []

  this._dispatchUpdates = this._dispatchUpdates.bind(this)
  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)
  this._handleConnectionStateChange()
}

EventEmitter(Record.prototype)

Object.defineProperty(Record.prototype, 'connected', {
  get: function connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Object.defineProperty(Record.prototype, 'empty', {
  get: function empty () {
    return !this.data || Object.keys(this.data).length === 0
  }
})

Object.defineProperty(Record.prototype, 'ready', {
  get: function ready () {
    return !this._patchQueue
  }
})

Object.defineProperty(Record.prototype, 'isReady', {
  get: function ready () {
    return !this._patchQueue
  }
})

Object.defineProperty(Record.prototype, 'provided', {
  get: function provided () {
    return this.hasProvider
  }
})

Record.prototype.get = function (path) {
  invariant(this.usages !== 0, `${this.name} "get" cannot use discarded record`)

  return jsonPath.get(this.data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  invariant(this.usages !== 0, `${this.name} "set" cannot use discarded record`)
  invariant(!this.hasProvider, `${this.name} "set" cannot be called on provided record`)

  if (this.usages === 0 || this.hasProvider) {
    return
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

  if (this._patchQueue) {
    this._patchQueue = path ? this._patchQueue : []
    this._patchQueue.push(path, data)
  }

  if (newValue === this.data) {
    return
  }

  if (!this._patchQueue) {
    this._sendUpdate(newValue)
  }

  this.data = utils.deepFreeze(newValue)

  this._handler.isAsync = false
  this.emit('update', this)
  this._handler.isAsync = true
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

  return new Promise((resolve, reject) => {
    this._updateQueue.push([ path, updater, resolve, reject ])
    if (this._updateQueue.length === 1) {
      this._ref()
      this._dispatchUpdates()
    }
  })
}

Record.prototype._dispatchUpdates = function () {
  if (this._updateQueue.length === 0) {
    this._unref()
    return
  }

  if (!this.isReady) {
    return this.whenReady().then(this._dispatchUpdates)
  }

  const [ path, updater, resolve, reject ] = this._updateQueue.shift()

  try {
    const prev = this.get(path)
    Promise
      .resolve(updater(prev))
      .then(next => {
        if (path) {
          this.set(path, next)
        } else if (next) {
          this.set(next)
        } else {
          next = this.get(path)
        }
        return next
      })
      .then(resolve)
      .catch(reject)
      .then(this._dispatchUpdates)
  } catch (err) {
    reject(err)
    this._dispatchUpdates()
  }
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

  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, [ this.name ])
  }

  this._client.off('connectionStateChanged', this._handleConnectionStateChange)
  this.off()
}

Record.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.UPDATE) {
    this._onUpdate(message.data)
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

  // TODO (perf): Avoid closure allocation.
  this._ref()
  this._lz.compress(newValue, (body, err) => {
    this._unref()

    if (!body || err) {
      this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, newValue)
      return
    }

    connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
      name,
      nextVersion,
      body,
      prevVersion
    ])
  })

  this.version = nextVersion
}

Record.prototype._onUpdate = function (data) {
  let [ version, body ] = data.slice(1)

  if (this.isReady && utils.isSameOrNewer(this.version, version)) {
    return
  }

  if (body === EMPTY_BODY) {
    body = EMPTY_OBJ
  }

  // TODO (perf): Avoid closure allocation.
  this._ref()
  this._lz.decompress(body, (newValue, err) => {
    this._unref()

    if (!newValue || err) {
      this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, data)
      return
    }

    if (this.isReady && utils.isSameOrNewer(this.version, version)) {
      return
    }

    const oldVersion = this.version
    const oldValue = this.data

    this.version = version
    this.data = newValue

    if (this._patchQueue) {
      for (let i = 0; i < this._patchQueue.length; i += 2) {
        this.data = jsonPath.set(this.data, this._patchQueue[i + 0], this._patchQueue[i + 1])
      }
      this._patchQueue = null

      this.emit('ready')
      this.emit('update', this)

      if (this.data !== newValue) {
        this._sendUpdate(newValue)
      }
    } else if (this.data !== oldValue || this.version !== oldVersion) {
      this.emit('update', this)
    }
  })
}

Record.prototype._handleConnectionStateChange = function () {
  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name ])
  } else {
    this._updateHasProvider(false)
  }
}

module.exports = Record
