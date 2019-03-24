const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const { Observable } = require('rxjs')
const invariant = require('invariant')
const LZ = require('./lz')
const EventEmitter = require('component-emitter2')
const RecordCache = require('./record-cache')
const jsonPath = require('./json-path')
const utils = require('../utils/utils')

const RecordHandler = function (options, connection, client) {
  this.STATE = C.RECORD_STATE

  this.isAsync = true
  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._pool = []
  this._prune = new Map()
  this._syncRef = 0
  this._syncSend = new Set()
  this._syncEmit = new Set()
  this._syncEmitter = new EventEmitter()
  this._syncTimeout = null
  this._syncCounter = 0

  this._schedule = options.schedule
  this._lz = options.lz || new LZ()
  this._cache = new RecordCache(options, err => {
    if (err) {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
    }
  })

  Observable
    .fromEvent(this._client, 'connectionStateChanged')
    .map(state => state === C.CONNECTION_STATE.OPEN)
    .distinctUntilChanged()
    .subscribe(connected => this._handleConnectionStateChange(connected))

  const prune = () => {
    const now = Date.now()

    this._cache.flush(err => {
      if (err) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
      }
    })

    for (const [ rec, timestamp ] of this._prune) {
      if (rec.usages !== 0) {
        this._prune.delete(rec)
        continue
      }

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
      if (this._pool.length < 65536) {
        this._pool.push(rec._$destroy())
      }
      this._prune.delete(rec)
    }

    setTimeout(() => this._schedule ? this._schedule(prune) : prune(), 1000)
  }

  prune()
}

Object.defineProperty(RecordHandler.prototype, 'connected', {
  get: function connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

RecordHandler.prototype.getRecord = function (name) {
  invariant(typeof name === 'string' && name.length > 0 && !name.includes('[object Object]'), `invalid name ${name}`)

  let record = this._records.get(name)

  if (!record) {
    record = (this._pool.pop() || new Record(this))._$construct(name)
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

RecordHandler.prototype._$syncRef = function () {
  this._syncRef += 1
}

RecordHandler.prototype._$syncUnref = function () {
  this._syncRef = Math.max(0, this._syncRef - 1)
  this._syncFlush()
}

RecordHandler.prototype._syncFlush = function () {
  if (this._syncRef > 0) {
    return
  }

  if (this._syncEmit.size > 0) {
    for (const token of this._syncEmit) {
      this._syncEmitter.emit(token)
    }
    this._syncEmit.clear()
  }

  if (!this._syncTimeout && this._syncSend.size > 0 && this.connected) {
    const syncSend = this._syncSend
    this._syncSend = new Set()
    this._syncCounter = (this._syncCounter + 1) & 2147483647

    this._syncTimeout = setTimeout(() => {
      this._syncTimeout = null
      for (const token of syncSend) {
        this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
      }
      this._syncFlush()
    }, this._options.syncDelay || 5)
  }
}

RecordHandler.prototype.sync = function () {
  const token = this._syncCounter.toString(16)
  this._syncSend.add(token)
  this._syncFlush()
  return new Promise(resolve => this._syncEmitter.once(token, resolve))
}

RecordHandler.prototype.get = function (name, pathOrNil, optionsOrNil) {
  if (arguments.length === 2 && typeof pathOrNil === 'object') {
    optionsOrNil = pathOrNil
    pathOrNil = undefined
  }

  const path = pathOrNil
  const options = optionsOrNil

  let state = Record.STATE.SERVER
  let timeout = 30e3

  if (options != null && typeof options === 'object') {
    state = options.state != null ? options.state : state
    timeout = options.timeout != null ? options.timeout : timeout
  } else if (options != null) {
    state = options
  }

  return this
    .observe(name, state)
    .first()
    .timeout(timeout)
    .map(data => jsonPath.get(data, path))
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

RecordHandler.prototype.observe = function (name, state) {
  if (!name) {
    return Observable.of(jsonPath.EMPTY)
  }

  if (typeof state === 'string') {
    state = state.toUpperCase()
    state = C.RECORD_STATE[state]
    if (state == null) {
      throw new Error('invalid argument state')
    }
  }

  return Observable
    .create(o => {
      const onUpdate = record => {
        if (!state || record.state >= state) {
          o.next(record.get())
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
      data: {},
      state: C.RECORD_STATE.SERVER,
      // TODO (fix): Remove
      name,
      connected: true, // This is not true...
      empty: true,
      ready: true,
      stale: false,
      isReady: true,
      hasProvider: false
    }))
  }

  return Observable
    .create(o => {
      const onUpdate = record => o.next(record)
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
    this._syncEmit.add(message.data[0])
    this._syncFlush()
    return
  }

  const record = this._records.get(name)
  if (record) {
    record._$onMessage(message)
  }

  const listener = this._listeners.get(name)
  if (listener) {
    listener._$onMessage(message)
  }
}

RecordHandler.prototype._handleConnectionStateChange = function (connected) {
  if (this.connected) {
    for (const token of this._syncEmitter.eventNames()) {
      this._syncSend.add(token)
    }
    this._syncFlush()
  } else {
    this._syncSend.clear()
    clearTimeout(this._syncTimeout)
    this._syncTimeout = null
  }

  for (const record of this._records.values()) {
    record._$handleConnectionStateChange(connected)
  }

  for (const listener of this._listeners.values()) {
    listener._$handleConnectionStateChange(connected)
  }
}

module.exports = RecordHandler
