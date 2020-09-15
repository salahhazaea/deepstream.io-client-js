const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const lz = require('@nxtedition/lz-string')
const invariant = require('invariant')

const Record = function (name, handler) {
  this._handler = handler
  this._stats = handler._stats
  this._prune = handler._prune
  this._pending = handler._pending
  this._cache = handler._cache
  this._client = handler._client
  this._connection = handler._connection

  this.name = name
  this.version = null
  this.data = jsonPath.EMPTY

  // TODO (fix): Make private
  this._$usages = 0
  this._$pruneTimestamp = null
  this._$readTimestamp = null

  this._provided = null
  this._patchQueue = []
  this._staleDirty = false
  this._staleVersion = null
  this._staleData = null

  this.ref()
  this._cache.get(this.name, (err, entry) => {
    this.unref()

    if (err && !err.notFound) {
      this._stats.misses += 1
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err, [this.name, this.version, this.state])
    } else if (entry) {
      this._stats.hits += 1

      const [version, data] = entry

      // TODO (fix): What if version is newer than this.version?
      if (!this.version) {
        this.version = version
        this.data = utils.deepFreeze(Object.keys(data).length === 0 ? jsonPath.EMPTY : data)
        this.emit('update', this)
      }

      this._staleVersion = version
      this._staleData = data
    }

    if (this.connected) {
      this._read()
    }
  })
  this._stats.reads += 1
}

Record.STATE = C.RECORD_STATE

EventEmitter(Record.prototype)

Record.prototype._$destroy = function () {
  invariant(this._$usages === 0, 'must have no refs')
  invariant(this.version, 'must have version to destroy')
  invariant(this.isReady, 'must be ready to destroy')

  if (this._staleDirty) {
    this._cache.set(this.name, this._staleVersion, this._staleData)
  }

  this._provided = null
  this._patchQueue = []

  // TODO (fix): Ensure unsubscribe is acked.
  this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this.name)

  this._prune.delete(this)

  return this
}

Object.defineProperty(Record.prototype, 'state', {
  enumerable: true,
  get: function state () {
    if (!this.version) {
      return Record.STATE.VOID
    }

    if (this._patchQueue) {
      return Record.STATE.CLIENT
    }

    invariant(this.connected, 'must be connected when no patch queue')

    if (this._provided && utils.isSameOrNewer(this.version, this._provided)) {
      return Record.STATE.PROVIDER
    }

    return Record.STATE.SERVER
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
  if (this._$usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [this.name, this.version, this.state])
    return Promise.resolve()
  }

  if (this.version && this.version.startsWith('INF')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [this.name, this.version, this.state])
    return Promise.resolve()
  }

  if (this.name.startsWith('_')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [this.name, this.version, this.state])
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrData
  const data = arguments.length === 1 ? pathOrData : dataOrNil

  if (path === undefined && !utils.isPlainObject(data)) {
    throw new Error('invalid argument: data')
  }
  if (path === undefined && Object.keys(data).some(prop => prop.startsWith('_'))) {
    throw new Error('invalid argument: data')
  }
  if (path !== undefined && (typeof path !== 'string' || path.length === 0 || path.startsWith('_'))) {
    throw new Error('invalid argument: path')
  }

  // TODO (perf): Avoid clone
  const jsonData = jsonPath.jsonClone(data)

  const newData = jsonPath.set(this.data, path, jsonData, true)

  if (this._patchQueue) {
    this._patchQueue = path ? this._patchQueue : []
    this._patchQueue.push(path, jsonData)
    this._pending.add(this)
  }

  if (newData === this.data) {
    return Promise.resolve()
  }

  this.data = utils.deepFreeze(newData)

  if (!this._patchQueue) {
    this._sendUpdate()
  } else {
    const [start] = this.version ? this.version.split('-') : ['0']
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
  if (this._$usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [this.name, this.version, this.state])
    return Promise.resolve()
  }

  if (this.version && this.version.startsWith('INF')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [this.name, this.version, this.state])
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
    const update = () => {
      try {
        const prev = this.get(path)
        const next = updater(prev, this.version)
        this.set(path, next)
        resolve(next)
      } catch (err) {
        reject(err)
      } finally {
        this.off('ready', update)
        this.unref()
      }
    }

    this.ref()
    if (this.isReady) {
      update()
    } else {
      // TODO: Timeout?
      this.on('ready', update)
      this.unref()
    }
  })
}

Record.prototype.ref = function () {
  this._$usages += 1

  if (this._$usages === 1) {
    this._$pruneTimestamp = null
    this._prune.delete(this)

    // TODO: resubscribe?
  }
}

Record.prototype.unref = function () {
  if (this._$usages === 0) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.REF_ERROR, 'cannot unref', [this.name])
    return
  }

  this._$usages -= 1
  if (this._$usages === 0) {
    this._$pruneTimestamp = Date.now()
    this._prune.add(this)
  }
}

Record.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.UPDATE) {
    this._onUpdate(message.data)
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_HAS_PROVIDER) {
    this._onSubscriptionHasProvider(message.data)
  } else {
    return false
  }
  return true
}

Record.prototype._onSubscriptionHasProvider = function (data) {
  const provided = messageParser.convertTyped(data[1], this._client) || null

  if (this._provided !== provided) {
    invariant(provided === null || typeof provided === 'string', 'provided must be null or string')
    this._provided = provided
    this.emit('update', this)
  }
}

Record.prototype._onUpdate = function ([name, version, data]) {
  try {
    if (!version) {
      throw new Error('missing version')
    }

    if (utils.isSameOrNewer(this.version, version)) {
      // TODO (fix): What to do when client version is newer than server version?

      if (!this._patchQueue) {
        return
      }

      if (this.version.startsWith('INF')) {
        // TODO (fix): This is weird...
        this._patchQueue = null
        this._pending.delete(this)
        this.emit('ready') // TODO: Deprecate
        this.emit('update', this)
        return
      }
    }

    if (this._staleVersion === version) {
      data = this._staleData
    } else if (!data) {
      throw new Error('missing data')
    } else {
      if (typeof data === 'string') {
        data = JSON.parse(/^\{.*\}$/.test(data) ? data : lz.decompressFromUTF16(data))
      }

      this._staleDirty = true
      this._staleVersion = version
      this._staleData = data
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
      } else if (this._patchQueue.length > 0) {
        // TODO (fix): Warning?
      }

      if (this.data !== oldValue) {
        this.data = utils.deepFreeze(this.data)
      }

      this._patchQueue = null
      this._pending.delete(this)
      this.emit('ready') // TODO: Deprecate
    } else if (this.data !== oldValue) {
      this.data = utils.deepFreeze(this.data)
    }
    this.emit('update', this)
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, err, [this.name, this.version, this.state, version, data])
  }
}

Record.prototype._sendUpdate = function () {
  let [start] = this.version ? this.version.split('-') : ['0']

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
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LZ_ERROR, err, [this.name, this.version, this.state, nextVersion])
    return
  }

  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
    this.name,
    nextVersion,
    body,
    prevVersion
  ])

  this.version = nextVersion
}

Record.prototype._read = function () {
  if (this._staleVersion) {
    this._connection.sendMsg2(C.TOPIC.RECORD, C.ACTIONS.READ, this.name, this._staleVersion)
  } else {
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.READ, this.name)
  }
  this._$readTimestamp = Date.now()
}

Record.prototype._$handleConnectionStateChange = function () {
  this._provided = null

  if (this.connected) {
    // TODO (fix): Limit number of reads.
    this._read()
  } else if (!this._patchQueue) {
    this._patchQueue = []
  }

  this.emit('update', this)
}

// Compat

Record.prototype.acquire = Record.prototype.ref
Record.prototype.discard = Record.prototype.unref
Record.prototype.destroy = Record.prototype.unref

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
    return this._provided && utils.isSameOrNewer(this.version, this._provided)
  }
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'usages', {
  get: function provided () {
    return this._$usages
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

module.exports = Record
