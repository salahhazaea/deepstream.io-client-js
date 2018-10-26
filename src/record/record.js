const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')

const Record = function (handler) {
  this._handler = handler
  this._options = handler._options
  this._prune = handler._prune
  this._cache = handler._cache
  this._lz = handler._lz
  this._connection = handler._connection
  this._client = handler._client
  this._dispatchUpdates = this._dispatchUpdates.bind(this)
  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._reset()
}

EventEmitter(Record.prototype)

Record.prototype._reset = function () {
  this.name = null
  this.usages = 0
  this.provided = false
  this.version = null
  this.data = null

  this._readTimeout = null
  this._patchQueue = []
  this._updateQueue = []
}

Record.prototype._$construct = function (name) {
  if (this.usages !== 0) {
    throw new Error('invalid operation: cannot construct referenced record')
  }

  if (typeof name !== 'string' || name.length === 0 || name.includes('[object Object]')) {
    throw new Error('invalid argument: name')
  }

  this._readTimeout = setTimeout(() => {
    const err = new Error('timeout')
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, err, [ this.name ])
  }, this._options.readTimeout || 30000)

  this.name = name

  this._ref()
  this._cache.get(this.name, (err, entry) => {
    if (err && !err.notFound) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
    } else if (entry) {
      const [ version, data ] = entry
      this.version = version
      this.data = data
      this.emit('update', this)
    }
    this._client.on('connectionStateChanged', this._handleConnectionStateChange)
    this._handleConnectionStateChange()
  })

  return this
}

Record.prototype._$destroy = function () {
  if (this.usages !== 0) {
    throw new Error('invalid operation: cannot destroy referenced record')
  }

  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, [ this.name ])
  }

  this._reset()
  this._client.off('connectionStateChanged', this._handleConnectionStateChange)
  this.off()

  return this
}

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
  get: function isReady () {
    return !this._patchQueue
  }
})

Object.defineProperty(Record.prototype, 'hasProvider', {
  get: function hasProvider () {
    return this.provided
  }
})

Record.prototype.get = function (path) {
  return jsonPath.get(this.data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  if (this.usages === 0 || this.provided) {
    return
  }

  const path = arguments.length === 1 ? undefined : pathOrData
  const data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && (typeof data !== 'object' || data === null)) {
    throw new Error('invalid argument: data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument: path')
  }

  const newData = jsonPath.set(this.data, path, data)

  if (this._patchQueue) {
    this._patchQueue = path ? this._patchQueue : []
    this._patchQueue.push(path, data)
  }

  if (newData === this.data) {
    return
  }

  this.data = utils.deepFreeze(newData)

  if (!this._patchQueue) {
    this._sendUpdate()
  } else {
    let [ start ] = this.version ? this.version.split('-') : [ '0' ]
    this.version = `${start}-${xuid()}`
  }

  this._handler.isAsync = false
  this.emit('update', this)
  this._handler.isAsync = true
}

Record.prototype.update = function (pathOrUpdater, updaterOrNil) {
  if (this.usages === 0 || this.provided) {
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrUpdater
  const updater = arguments.length === 1 ? pathOrUpdater : updaterOrNil

  if (typeof updater !== 'function') {
    throw new Error('invalid argument: updater')
  }

  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument: path')
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

  if (!this.ready) {
    return this
      .whenReady()
      .then(this._dispatchUpdates)
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

Record.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.UPDATE) {
    this._onUpdate(message.data)
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_HAS_PROVIDER) {
    this._updateHasProvider(messageParser.convertTyped(message.data[1], this._client))
  }
}

Record.prototype._updateHasProvider = function (provided) {
  if (this.provided === provided) {
    return
  }

  this.provided = provided
  this.emit('update', this)
}

Record.prototype._onUpdate = function (data) {
  let [ version, body ] = data.slice(1)

  if (!this._patchQueue && utils.isSameOrNewer(this.version, version)) {
    return
  }

  if (version === this.version) {
    body = this.data
  }

  // TODO (perf): Avoid closure allocation.
  this._ref()
  this._lz.decompress(body, (data, err) => {
    this._unref()

    if (!data || err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.LZ_ERROR, err, data)
      return
    }

    if (!this._patchQueue && utils.isSameOrNewer(this.version, version)) {
      return
    }

    const oldValue = this.data

    this.version = version
    this.data = data = jsonPath.patch(this.data, data)

    if (this._patchQueue) {
      for (let i = 0; i < this._patchQueue.length; i += 2) {
        this.data = jsonPath.set(this.data, this._patchQueue[i + 0], this._patchQueue[i + 1])
      }

      if (this.data !== data) {
        this._sendUpdate()
      }

      this._unref()

      this._patchQueue = null
      clearTimeout(this._readTimeout)
      this._readTimeout = null

      this.emit('ready')
      this.emit('update', this)
    } else if (this.data !== oldValue) {
      this.emit('update', this)
    }
  })
}

Record.prototype._sendUpdate = function () {
  let [ start ] = this.version ? this.version.split('-') : [ '0' ]

  if (start === 'INF' || this.provided) {
    return
  }

  start = parseInt(start, 10)
  start = start >= 0 ? start : 0

  const nextVersion = `${start + 1}-${xuid()}`
  const prevVersion = this.version || ''

  // TODO (perf): Avoid closure allocation.
  this._ref()
  this._lz.compress(this.data, (body, err) => {
    this._unref()

    if (!body || err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.LZ_ERROR, err)
      return
    }

    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
      this.name,
      nextVersion,
      body,
      prevVersion
    ])
  })

  this.version = nextVersion
}

Record.prototype._handleConnectionStateChange = function () {
  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.READ, [ this.name ])
  } else {
    this._updateHasProvider(false)
  }
}

module.exports = Record
