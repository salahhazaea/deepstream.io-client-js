const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')
const messageParser = require('../message/message-parser')
const xuid = require('xuid')
const invariant = require('invariant')

const Record = function (name, handler) {
  this._handler = handler
  this._stats = handler._stats
  this._prune = handler._prune
  this._pendingWrite = handler._pendingWrite
  this._cache = handler._cache
  this._client = handler._client
  this._connection = handler._connection

  this.name = name
  this.version = null
  this.data = jsonPath.EMPTY

  this._subscribed = false
  this._provided = null
  this._patchQueue = []
  this._staleDirty = false
  this._staleEntry = null

  this._usages = 1 // Start with 1 for cache unref without subscribe.
  this._cache.get(this.name, (err, entry) => {
    this.unref()

    if (err && !err.notFound) {
      this._stats.misses += 1
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err, [
        this.name,
        this.version,
        this.state,
      ])
    } else if (entry) {
      invariant(
        typeof entry[0] === 'string' && entry[1] && typeof entry[1] === 'object',
        'entry must be [string, object]'
      )

      this._stats.hits += 1

      if (this.version) {
        // TODO (fix): What if this.version is older than version?
        return
      }

      invariant(!this._staleEntry, 'no version no entry')

      this._staleEntry = entry
      this.version = entry[0]
      this.data = utils.deepFreeze(Object.keys(entry[1]).length === 0 ? jsonPath.EMPTY : entry[1])
      this.emit('update', this)
    }

    this._subscribe()
  })
  this._stats.reads += 1
}

Record.STATE = C.RECORD_STATE

EventEmitter(Record.prototype)

Record.prototype._$destroy = function () {
  invariant(this._usages === 0, 'must have no refs')
  invariant(this.version, 'must have version to destroy')
  invariant(this.isReady, 'must be ready to destroy')
  invariant(!this._patchQueue, 'must not have patch queue')

  if (this._staleDirty && this._staleEntry) {
    this._cache.set(this.name, this._staleEntry[0], this._staleEntry[1])
    this._staleDirty = false
  }

  if (this._subscribed) {
    // TODO (fix): Ensure unsubscribe is acked.
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.UNSUBSCRIBE, this.name)
  }

  this._subscribed = false
  this._provided = null
  this._patchQueue = this._patchQueue || []

  return this
}

Object.defineProperty(Record.prototype, 'state', {
  enumerable: true,
  get: function state() {
    if (!this.version) {
      return Record.STATE.VOID
    }

    if (this._patchQueue) {
      return this.version[0] === '0' ? Record.STATE.EMPTY : Record.STATE.CLIENT
    }

    invariant(this.connected, 'must be connected when no patch queue')

    if (this._provided) {
      return this.version[0] === 'I' ? Record.STATE.PROVIDER : Record.STATE.SERVER
    }

    return Record.STATE.SERVER
  },
})

Record.prototype.get = function (path) {
  invariant(this._usages > 0, 'must have refs')

  return jsonPath.get(this.data, path)
}

Record.prototype.set = function (pathOrData, dataOrNil) {
  invariant(this._usages > 0, 'must have refs')

  if (this._usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [
      this.name,
      this.version,
      this.state,
    ])
    return Promise.resolve()
  }

  if (this.version && this.version.charAt(0) === 'I') {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [
      this.name,
      this.version,
      this.state,
    ])
    return Promise.resolve()
  }

  if (this.name.startsWith('_')) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [
      this.name,
      this.version,
      this.state,
    ])
    return Promise.resolve()
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

  // TODO (perf): Avoid clone
  const jsonData = jsonPath.jsonClone(data)

  const newData = jsonPath.set(this.data, path, jsonData, true)

  if (this._patchQueue) {
    this._patchQueue = path ? this._patchQueue : []
    this._patchQueue.push(path, jsonData)

    if (!this._pendingWrite.has(this)) {
      this.ref()
      this._pendingWrite.add(this)
    }
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

  return this.when()
}

Record.prototype.when = function (stateOrNull) {
  invariant(this._usages > 0, 'must have refs')

  const state = stateOrNull == null ? Record.STATE.SERVER : stateOrNull

  if (!Number.isFinite(state) || state < 0) {
    throw new Error('invalid argument: state')
  }

  return new Promise((resolve, reject) => {
    if (this.state >= state) {
      resolve()
      return
    }

    const onUpdate = () => {
      if (this.state < state) {
        return
      }

      // clearTimeout(timeout)

      this.off('update', onUpdate)
      this.unref()

      resolve()
    }

    // const timeout = setTimeout(() => {
    //   this.off('update', onUpdate)
    //   this.unref()

    //   reject(new Error('when timeout'))
    // }, 2 * 60e3)

    this.ref()
    this.on('update', onUpdate)
  })
}

Record.prototype.update = function (pathOrUpdater, updaterOrNil) {
  invariant(this._usages > 0, 'must have refs')

  if (this._usages === 0 || this._provided) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [
      this.name,
      this.version,
      this.state,
    ])
    return Promise.resolve()
  }

  if (this.version && this.version.charAt(0) === 'I') {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot update', [
      this.name,
      this.version,
      this.state,
    ])
    return Promise.resolve()
  }

  const path = arguments.length === 1 ? undefined : pathOrUpdater
  const updater = arguments.length === 1 ? pathOrUpdater : updaterOrNil

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

  this.ref()
  return this.when(Record.STATE.SERVER)
    .then(() => {
      const prev = this.get(path)
      const next = updater(prev, this.version)
      this.set(path, next)
    })
    .finally(() => {
      this.unref()
    })
}

Record.prototype.ref = function () {
  this._usages += 1
  if (this._usages === 1) {
    this._prune.delete(this)
    this._subscribe()
  }
}

Record.prototype.unref = function () {
  invariant(this._usages > 0, 'must have refs')

  this._usages -= 1
  if (this._usages === 0) {
    this._prune.set(this, Date.now())
  }
}

Record.prototype._$onMessage = function (message) {
  if (!this.connected) {
    this._client._$onError(
      C.TOPIC.RECORD,
      C.EVENT.NOT_CONNECTED,
      new Error('received message while not connected'),
      message
    )
    return
  }

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
  invariant(this.connected, 'must be connected')

  const provided = Boolean(messageParser.convertTyped(data[1], this._client))

  if (this._provided === provided) {
    return
  }

  this._provided = provided
  try {
    this.emit('update', this)
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [
      this.name,
      this.version,
      this.state,
    ])
  }
}

Record.prototype._onUpdate = function ([name, version, data]) {
  invariant(this.connected, 'must be connected')

  try {
    if (!version) {
      throw new Error('missing version')
    }

    if (version.charAt(0) !== 'I' && utils.isSameOrNewer(this.version, version)) {
      if (!this._patchQueue || this.version.charAt(0) === 'I') {
        return
      }
      // TODO (fix): What to do when client version is newer than server version?
    }

    if (this._staleEntry && this._staleEntry[0] === version) {
      data = this._staleEntry[1]
      data = jsonPath.set(this.data, null, data, true)
    } else if (!data) {
      data = this.data
      version = this.version
    } else {
      if (typeof data === 'string') {
        data = JSON.parse(data)
      }
      data = jsonPath.set(this.data, null, data, true)

      this._staleDirty = true
      this._staleEntry = [version, data]
    }

    invariant(data, 'missing data')
    invariant(version, 'missing version')

    const oldValue = this.data

    this.version = version
    this.data = data

    if (this._patchQueue) {
      if (this.version.charAt(0) !== 'I') {
        for (let i = 0; i < this._patchQueue.length; i += 2) {
          this.data = jsonPath.set(
            this.data,
            this._patchQueue[i + 0],
            this._patchQueue[i + 1],
            true
          )
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
      if (this._pendingWrite.delete(this)) {
        this.unref()
      }
      this.emit('ready')
    } else if (this.data !== oldValue) {
      this.data = utils.deepFreeze(this.data)
    }
    this.emit('update', this)
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, err, [
      this.name,
      this.version,
      this.state,
      this._staleEntry,
      version,
      data,
    ])
  }
}

Record.prototype._sendUpdate = function () {
  invariant(this.connected, 'must be connected')

  let [start] = this.version ? this.version.split('-') : ['0']

  if (start.charAt(0) === 'I' || this._provided) {
    return
  }

  start = parseInt(start, 10)
  start = start >= 0 ? start : 0

  const nextVersion = this._makeVersion(start + 1)
  const prevVersion = this.version || ''

  const body = JSON.stringify(this.data)

  // TODO (fix): This might never make it to server?
  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
    this.name,
    nextVersion,
    body,
    prevVersion,
  ])

  this.version = nextVersion
}

Record.prototype._subscribe = function () {
  if (!this.connected || this._subscribed || this._usages === 0) {
    return
  }

  // TODO (fix): Limit number of reads.

  if (this._staleEntry && this._staleEntry[0]) {
    this._connection.sendMsg2(C.TOPIC.RECORD, C.ACTIONS.READ, this.name, this._staleEntry[0])
  } else {
    this._connection.sendMsg1(C.TOPIC.RECORD, C.ACTIONS.READ, this.name)
  }

  this._subscribed = true
}

Record.prototype._$handleConnectionStateChange = function () {
  if (this.connected) {
    this._subscribe()
  } else {
    this._subscribed = false
    this._provided = null
    this._patchQueue = this._patchQueue || []
  }

  try {
    this.emit('update', this)
  } catch (err) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.UPDATE_ERROR, 'cannot set', [
      this.name,
      this.version,
      this.state,
    ])
  }
}

Record.prototype._makeVersion = function (start) {
  let revid = `${xuid()}-${this._client.user || ''}`
  if (revid.length === 32 || revid.length === 16) {
    // HACK: https://github.com/apache/couchdb/issues/2015
    revid += '-'
  }
  return `${start}-${revid}`
}

// Compat

Record.prototype.acquire = Record.prototype.ref
Record.prototype.discard = Record.prototype.unref
Record.prototype.destroy = Record.prototype.unref

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'connected', {
  get: function connected() {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
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
    return !this._patchQueue
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
    return this._usages
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
    return !this._patchQueue
  },
})

// TODO (fix): Remove
Object.defineProperty(Record.prototype, 'hasProvider', {
  get: function hasProvider() {
    return this.state >= C.RECORD_STATE.PROVIDER
  },
})

module.exports = Record
