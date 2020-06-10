const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const lz = require('@nxtedition/lz-string')

const Record = function (handler) {
  this._handler = handler
  this._stats = handler._stats
  this._prune = handler._prune
  this._pending = handler._pending
  this._cache = handler._cache
  this._client = handler._client
  this._connection = handler._connection

  this._reset()
}

Record.STATE = C.RECORD_STATE

EventEmitter(Record.prototype)

Record.prototype._reset = function () {
  this.name = null
  this.version = null
  this.data = jsonPath.EMPTY

  // TODO (fix): Make private
  this.usages = 0

  this._provided = false
  this._timeout = false
  this._stale = null
  this._dirty = true
  this._patchQueue = []
  this.off()
}

Record.prototype._$construct = function (name) {
  if (this.usages !== 0) {
    throw new Error('invalid operation: cannot construct referenced record')
  }

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument: name')
  }

  this.name = name

  this._pending.set(this, Date.now())
  this.ref()
  this._cache.get(this.name, (err, entry) => {
    if (err && !err.notFound) {
      this._stats.misses += 1
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err, [ this.name, this.version, this.state ])
    } else if (entry) {
      this._stats.hits += 1

      const [ version, data ] = entry

      if (utils.isSameOrNewer(this.version, version)) {
        this.data = utils.deepFreeze(Object.keys(data).length === 0 ? jsonPath.EMPTY : data)
        this._dirty = false
        this.version = version
        this.emit('update', this)
      }
    }

    if (this.connected) {
      this._read()
    }
  })
  this._stats.reads += 1

  return this
}

Record.prototype._$destroy = function () {
  if (this.version && this._dirty) {
    this._dirty = false
    this._cache.set(this.name, this.version, this.data)
  }

  if (this.connected) {
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this.name)
  }

  this._reset()

  return this
}

Object.defineProperty(Record.prototype, 'state', {
  enumerable: true,
  get: function state () {
    if (!this.version) {
      return Record.STATE.VOID
    }

    if (!this.connected) {
      return Record.STATE.CLIENT
    }

    if (!this._patchQueue) {
      return this._provided ? Record.STATE.PROVIDER : Record.STATE.SERVER
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
    return Object.keys(this.data).length === 0
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'ready', {
  get: function ready () {
    return !this._patchQueue
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'provided', {
  get: function provided () {
    return Boolean(this._provided && !this._patchQueue)
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'stale', {
  get: function ready () {
    return !this.version
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
  if (this.usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set record', [ this.name, this.version, this.state ])
    return Promise.resolve()
  }

  if (this.version && this.version.startsWith('INF')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set record', [ this.name, this.version, this.state ])
    return Promise.resolve()
  }

  let path = arguments.length === 1 ? undefined : pathOrData
  let data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && !utils.isPlainObject(data)) {
    throw new Error('invalid argument: data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0)) {
    throw new Error('invalid argument: path')
  }

  const jsonData = jsonPath.jsonClone(data)
  const newData = jsonPath.set(this.data, path, jsonData, true)

  if (this._patchQueue) {
    this._patchQueue = path ? this._patchQueue : []
    this._patchQueue.push(path, jsonData)
  }

  if (newData === this.data) {
    return Promise.resolve()
  }

  this.data = utils.deepFreeze(newData)
  this._dirty = true

  if (!this._patchQueue) {
    this._sendUpdate()
  } else {
    const [ start ] = this.version ? this.version.split('-') : [ '0' ]
    this.version = this._makeVersion(start)
  }

  this._handler._syncCount += 1
  this.emit('update', this)
  this._handler._syncCount -= 1

  return this.isReady
    ? Promise.resolve()
    : new Promise(resolve => this.once('ready', resolve))
}

Record.prototype.update = function (pathOrUpdater, updaterOrNil) {
  if (this.usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update record', [ this.name, this.version, this.state ])
    return Promise.resolve()
  }

  if (this.version && this.version.startsWith('INF')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update record', [ this.name, this.version, this.state ])
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

  const doUpdate = () => {
    try {
      const prev = this.get(path)
      const next = updater(prev)
      this.set(path, next)
    } catch (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, err, [ this.name, this.version, this.state ])
    }
    this.unref()
  }

  this.ref()
  if (this.isReady) {
    doUpdate()
  } else {
    this.once('ready', doUpdate)
  }

  return this.isReady
    ? Promise.resolve()
    : new Promise(resolve => this.once('ready', resolve))
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

Record.prototype.acquire = Record.prototype.ref
Record.prototype.discard = Record.prototype.unref
Record.prototype.destroy = Record.prototype.unref

Record.prototype._$onTimeout = function () {
  if (this._timeout) {
    return
  }
  this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'read timeout', [ this.name, this.version, this.state ])
  this._timeout = true

  // TODO(fix): Is this the best we can do?
  if (this.version) {
    this._onReady()
  } else {
    this._pending.delete(this)
  }
}

Record.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.UPDATE) {
    this._onUpdate(message.data)
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_HAS_PROVIDER) {
    this._onSubscriptionHasProvider(message.data)
  }
}

Record.prototype._onSubscriptionHasProvider = function (data) {
  const provided = messageParser.convertTyped(data[1], this._client)

  if (this.connected && this._provided !== provided) {
    this._provided = provided
    // Depending on timing a buffered has SUBSCRIPTION_HAS_PROVIDER message
    // can arrive the UPDATE message.
    if (!this._patchQueue) {
      this.emit('update', this)
    }
  }
}

Record.prototype._onReady = function () {
  this._patchQueue = null
  this._pending.delete(this)
  this.emit('ready')
  this.emit('update', this)
  this.unref()
}

Record.prototype._onUpdate = function ([name, version, data]) {
  if (this._stale) {
    if (!data || !version) {
      data = this._stale.data
      version = this._stale.version
    }
    this._stale = null
  }

  if (!version || !data) {
    // This is not an error since a full broadcast UPDATE might have arrived before
    // a partial stale UPDATE.
    // TODO: Refactor logic and add some guards for possible failure cases.
    return
  }

  if (utils.isSameOrNewer(this.version, version)) {
    if (!this._patchQueue) {
      return
    } else if (this.version.startsWith('INF')) {
      this._onReady()
      return
    }
  }

  try {
    data = typeof data === 'string' ? JSON.parse(lz.decompressFromUTF16(data)) : data
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LZ_ERROR, err, [ this.name, this.version, this.state, version, data ])
    return
  }

  const oldValue = this.data

  this.version = version
  this.data = data = jsonPath.set(this.data, null, data, true)

  if (this._patchQueue) {
    if (!this.version.startsWith('INF')) {
      for (let i = 0; i < this._patchQueue.length; i += 2) {
        this.data = jsonPath.set(this.data, this._patchQueue[i + 0], this._patchQueue[i + 1], true)
      }
      if (this.data !== data) {
        this._sendUpdate()
      }
    }

    if (this.data !== oldValue) {
      this.data = utils.deepFreeze(this.data)
      this._dirty = true
    }

    this._onReady()
  } else if (this.data !== oldValue) {
    this.data = utils.deepFreeze(this.data)
    this._dirty = true

    this.emit('update', this)
  }
}

Record.prototype._sendUpdate = function () {
  let [ start ] = this.version ? this.version.split('-') : [ '0' ]

  if (start === 'INF' || this._provided) {
    return
  }

  start = parseInt(start, 10)
  start = start >= 0 ? start : 0

  const nextVersion = this._makeVersion(start + 1)
  const prevVersion = this.version || ''

  let body
  try {
    body = lz.compressToUTF16(JSON.stringify(this.data))
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LZ_ERROR, err, [ this.name, this.version, this.state, nextVersion ])
    return
  }

  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
    this.name,
    nextVersion,
    body,
    prevVersion,
  ])

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
  if (this.connected) {
    this._read()
  } else {
    this._provided = false
  }

  this.emit('update', this)
}

module.exports = Record
