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
  this._client = handler._client
  this._lz = handler._lz
  this._connection = handler._connection
  this._client = handler._client
  this._dispatchUpdates = this._dispatchUpdates.bind(this)

  this._reset()
}

Record.STATE = C.RECORD_STATE

EventEmitter(Record.prototype)

Record.prototype._reset = function () {
  this.name = null
  this.version = null
  this.data = null

  // TODO (fix): Make private
  this.usages = 0
  this.provided = false

  this._loading = true
  this._stale = null
  this._dirty = true
  this._patchQueue = []
  this._updateQueue = []
}

Record.prototype._$construct = function (name) {
  if (this.usages !== 0) {
    throw new Error('invalid operation: cannot construct referenced record')
  }

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument: name')
  }

  this.name = name

  this._ref()
  this._cache.get(this.name, (err, entry) => {
    if (err && !err.notFound) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
    } else if (entry) {
      const [ version, data ] = entry
      this.version = version
      this.data = utils.deepFreeze(data)
      this._dirty = false
      this.emit('update', this)
    }

    this._loading = false
    if (this.connected) {
      this._read()
    }
  })

  return this
}

Record.prototype._$destroy = function () {
  if (this._dirty) {
    this._cache.set(this.name, this.version, this.data)
  }

  if (this.connected) {
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this.name)
  }

  this._reset()
  this.off()

  return this
}

Object.defineProperty(Record.prototype, 'state', {
  get: function state () {
    if (!this.data) {
      return Record.STATE.VOID
    }

    if (!this.connected) {
      return Record.STATE.CLIENT
    }

    if (this.provided) {
      return Record.STATE.PROVIDER
    }

    if (this.ready) {
      return Record.STATE.SERVER
    }

    return Record.STATE.CLIENT
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'connected', {
  get: function connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'empty', {
  get: function empty () {
    return !this.data || Object.keys(this.data).length === 0
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'ready', {
  get: function ready () {
    return !this._patchQueue
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'stale', {
  get: function ready () {
    return this.data == null
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'isReady', {
  get: function isReady () {
    return !this._patchQueue
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'hasProvider', {
  get: function hasProvider () {
    return this.provided
  }
})

Record.prototype.get = function (path) {
  return jsonPath.get(this.data, path)
}

Record.prototype._makeVersion = function (start) {
  let revid = `${xuid()}-${this._client.user || ''}`
  if (revid.length === 32 || revid.length === 16) {
    // HACK: https://github.com/apache/couchdb/issues/2015
    revid += '-'
  }
  return `${start}-${revid}`
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  if (this.usages === 0 || this.provided) {
    return
  }

  let path = arguments.length === 1 ? undefined : pathOrData
  let data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && !utils.isPlainObject(data)) {
    throw new Error('invalid argument: data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument: path')
  }

  // NOTE: This ensure that data is JSON.stringify/parse compatible.
  data = data != null && typeof data === 'object'
    ? JSON.parse(JSON.stringify(data))
    : data

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
    const [ start ] = this.version ? this.version.split('-') : [ '0' ]
    this.version = this._makeVersion(start)
  }

  this._handler.isAsync = false
  this._dirty = true
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
      ._whenReady()
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

Record.prototype._whenReady = function () {
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
    this.provided = messageParser.convertTyped(message.data[1], this._client)
    this.emit('update', this)
  }
}

Record.prototype._onUpdate = function (data) {
  let [ version, body ] = data.slice(1)

  if (this._stale) {
    if (!body || !version) {
      body = this._stale.data
      version = this._stale.version
    }
    this._stale = null
  }

  if (!version || !body) {
    return
  }

  if (utils.isSameOrNewer(this.version, version)) {
    if (!this._patchQueue) {
      return
    } else if (this.version.startsWith('INF')) {
      this._unref()
      this._patchQueue = null
      this.emit('ready')
      this._dirty = true
      this.emit('update', this)
      return
    }
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
    this.data = data = jsonPath.set(this.data, null, data)

    if (this._patchQueue) {
      if (this.data !== oldValue && !this.version.startsWith('INF')) {
        for (let i = 0; i < this._patchQueue.length; i += 2) {
          this.data = jsonPath.set(this.data, this._patchQueue[i + 0], this._patchQueue[i + 1])
        }
      }

      if (this.data !== data) {
        this._sendUpdate()
      }

      this.data = utils.deepFreeze(this.data)

      this._unref()
      this._patchQueue = null
      this.emit('ready')
      this._dirty = true
      this.emit('update', this)
    } else if (this.data !== oldValue) {
      this.data = utils.deepFreeze(this.data)
      this._dirty = true
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

  const nextVersion = this._makeVersion(start + 1)
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

Record.prototype._read = function () {
  if (this.version) {
    this._stale = { version: this.version, data: this.data }
    this._connection.sendMsg2(C.TOPIC.RECORD, C.ACTIONS.READ, this.name, this.version)
  } else {
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.READ, this.name)
  }
}

Record.prototype._$handleConnectionStateChange = function () {
  if (this._loading) {
    return
  }

  if (this.connected) {
    this._read()
  } else {
    this.provided = false
  }

  this.emit('update', this)
}

module.exports = Record
