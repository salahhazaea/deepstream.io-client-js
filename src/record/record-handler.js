const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const { Observable } = require('rxjs')
const invariant = require('invariant')
const EventEmitter = require('component-emitter2')
const RecordCache = require('./record-cache')
const jsonPath = require('./json-path')
const utils = require('../utils/utils')

const RecordHandler = function (options, connection, client) {
  this.STATE = C.RECORD_STATE

  Object.assign(this, C.RECORD_STATE)

  this._syncCount = 0
  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._prune = new Map()
  this._pending = new Set()

  this._syncEmitter = new EventEmitter()
  this._syncCounter = 0

  this._stats = {
    reads: 0,
    hits: 0,
    misses: 0
  }

  this._schedule = options.schedule

  const cacheFactory = typeof options.cache === 'function'
    ? options.cache
    : options => new RecordCache(options, err => {
      if (err) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
      }
    })

  this._cache = cacheFactory(options, err => {
    if (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
    }
  })

  this._client.on('connectionStateChanged', state => {
    if (state === C.CONNECTION_STATE.OPEN) {
      this._handleConnectionStateChange(true)
    } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
      this._handleConnectionStateChange(false)
    }
  })

  const prune = () => {
    this._cache.flush(err => {
      if (err) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
      }
    })

    if (this.connected) {
      const now = Date.now()

      for (const [rec, timestamp] of this._prune) {
        if (!rec.isReady) {
          continue
        }

        // TODO (fix): This should move to userland.
        const minAge = rec.version && rec.version.startsWith('I')
          ? 1000
          : 10000

        if (now - timestamp <= minAge) {
          continue
        }

        this._records.delete(rec.name)
        this._prune.delete(rec)
        rec._$destroy()
      }
    }

    setTimeout(() => this._schedule ? this._schedule(prune) : prune(), 1000)
  }

  prune()
}

Object.defineProperty(RecordHandler.prototype, 'isAsync', {
  get: function isAsync () {
    return this._syncCount === 0
  }
})

Object.defineProperty(RecordHandler.prototype, 'connected', {
  get: function connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Object.defineProperty(RecordHandler.prototype, 'stats', {
  get: function stats () {
    return Object.assign({}, this._stats, {
      listeners: this._listeners.size,
      records: this._records.size
    })
  }
})

RecordHandler.prototype.getRecord = function (name) {
  invariant(typeof name === 'string' && name.length > 0 && !name.includes('[object Object]'), `invalid name ${name}`)

  let record = this._records.get(name)

  if (!record) {
    record = new Record(name, this)
    this._records.set(name, record)
  }

  record.ref()

  return record
}

RecordHandler.prototype.provide = function (pattern, callback, recursive = false) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (this._listeners.has(pattern)) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LISTENER_EXISTS, new Error(pattern))
    return
  }

  const listener = new Listener(
    C.TOPIC.RECORD,
    pattern,
    callback,
    this,
    recursive
  )

  this._listeners.set(pattern, listener)
  return () => {
    listener._$destroy()
    this._listeners.delete(pattern)
  }
}

RecordHandler.prototype.sync = function () {
  // TODO (perf): Optimize

  const pending = []
  for (const rec of this._pending) {
    rec.ref()
    pending.push(new Promise(resolve => rec.once('ready', resolve)))
  }

  return new Promise(resolve => {
    let token
    let timeout

    const onTimeout = () => {
      if (!this.connected) {
        timeout = setTimeout(onTimeout, 2 * 60e3)
        return
      }

      for (const rec of pending) {
        if (!rec.isReady) {
          this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'record timeout', [rec.name])
        }
        rec.unref()
      }

      if (token) {
        this._syncEmitter.off(token)
      }

      this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'sync timeout', [token])

      resolve(false)
    }

    timeout = setTimeout(onTimeout, 2 * 60e3)

    return Promise
      .all(pending)
      .then(() => {
        token = this._syncCounter.toString(16)
        this._syncCounter = (this._syncCounter + 1) & 2147483647

        this._syncEmitter.once(token, () => {
          clearTimeout(timeout)
          resolve(true)
        })

        if (this.connected) {
          this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
        }
      })
  })
}

RecordHandler.prototype.get = function (name, pathOrState, stateOrNil) {
  if (arguments.length === 2 && typeof pathOrState === 'number') {
    stateOrNil = pathOrState
    pathOrState = undefined
  }
  const path = pathOrState
  const state = stateOrNil == null ? 2 : stateOrNil

  return this
    .observe(name, path, state)
    .first()
    .timeout(2 * 60e3)
    .toPromise()
}

RecordHandler.prototype.set = function (name, pathOrData, dataOrNil) {
  const record = this.getRecord(name)
  try {
    return arguments.length === 2
      ? record.set(pathOrData)
      : record.set(pathOrData, dataOrNil)
  } finally {
    record.unref()
  }
}

RecordHandler.prototype.update = function (name, pathOrUpdater, updaterOrNil) {
  try {
    const record = this.getRecord(name)
    try {
      return arguments.length === 2
        ? record.update(pathOrUpdater)
        : record.update(pathOrUpdater, updaterOrNil)
    } finally {
      record.unref()
    }
  } catch (err) {
    return Promise.reject(err)
  }
}

RecordHandler.prototype.observe = function (name, pathOrState, stateOrNil) {
  if (arguments.length === 2 && typeof pathOrState === 'number') {
    stateOrNil = pathOrState
    pathOrState = undefined
  }
  const path = pathOrState
  const state = stateOrNil == null ? 2 : stateOrNil

  if (!name) {
    return Observable.of(jsonPath.EMPTY)
  }

  return new Observable(o => {
    const onUpdate = record => {
      if (!state || record.state >= state) {
        o.next(record.get(path))
      }
    }
    const record = this.getRecord(name)
    if (record.version) {
      onUpdate(record)
    }
    record.on('update', onUpdate)
    return () => {
      record.off('update', onUpdate)
      record.unref()
    }
  })
    .distinctUntilChanged()
}

RecordHandler.prototype.observe2 = function (name) {
  if (!name) {
    return Observable.of(utils.deepFreeze({
      version: '0-00000000000000',
      data: jsonPath.EMPTY,
      state: C.RECORD_STATE.SERVER
    }))
  }

  return new Observable(o => {
    const onUpdate = ({ version, data, state }) => o.next({
      version,
      data,
      state
    })
    const record = this.getRecord(name)
    if (record.version) {
      onUpdate(record)
    }
    record.on('update', onUpdate)
    return () => {
      record.off('update', onUpdate)
      record.unref()
    }
  })
}

RecordHandler.prototype._$handle = function (message) {
  let name
  if (message.action === C.ACTIONS.ERROR) {
    name = message.data[1]
  } else {
    name = message.data[0]
  }

  if (message.action === C.ACTIONS.SYNC) {
    this._syncEmitter.emit(message.data[0])
    return true
  }

  const listener = this._listeners.get(name)
  if (listener && listener._$onMessage(message)) {
    return true
  }

  const record = this._records.get(name)
  if (record && record._$onMessage(message)) {
    return true
  }

  return false
}

RecordHandler.prototype._handleConnectionStateChange = function (connected) {
  for (const listener of this._listeners.values()) {
    listener._$handleConnectionStateChange(connected)
  }

  for (const record of this._records.values()) {
    record._$handleConnectionStateChange(connected)
  }

  if (connected) {
    // TODO (fix): This should wait until all records are ready.
    for (const token of this._syncEmitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
    }
  }
}

module.exports = RecordHandler
