const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const rxjs = require('rxjs')
const invariant = require('invariant')
const EventEmitter = require('component-emitter2')
const RecordCache = require('./record-cache')
const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const rx = require('rxjs/operators')

const RecordHandler = function (options, connection, client) {
  this.STATE = C.RECORD_STATE
  this.JSON = jsonPath

  Object.assign(this, C.RECORD_STATE)

  this._syncCount = 0
  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._prune = new Map()
  this._pendingWrite = new Set()

  this._syncEmitter = new EventEmitter()
  this._syncCounter = 0

  this._stats = {
    reads: 0,
    hits: 0,
    misses: 0,
  }

  this._schedule = options.schedule

  if (options.cache) {
    this._cache = options.cache.on('error', (err) => {
      this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
    })
  } else {
    // Legacy
    this._cache = new RecordCache(options, (err) => {
      if (err) {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
      }
    })
    const interval = setInterval(() => {
      this._cache.flush((err) => {
        if (err) {
          this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
        }
      })
    }, 1e3)
    if (interval.unref) {
      interval.unref()
    }
  }

  this._client.on('connectionStateChanged', (state) => {
    if (state === C.CONNECTION_STATE.OPEN) {
      this._handleConnectionStateChange(true)
    } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
      this._handleConnectionStateChange(false)
    }
  })

  const prune = () => {
    if (this.connected) {
      const now = Date.now()

      for (const [rec, timestamp] of this._prune) {
        if (!rec.isReady) {
          continue
        }

        if (now - timestamp <= 1e3) {
          continue
        }

        this._records.delete(rec.name)
        this._prune.delete(rec)
        rec._$destroy()
      }
    }

    const timeout = setTimeout(() => (this._schedule ? this._schedule(prune) : prune()), 1e3)
    if (timeout.unref) {
      timeout.unref()
    }
  }

  prune()
}

Object.defineProperty(RecordHandler.prototype, 'isAsync', {
  get: function isAsync() {
    return this._syncCount === 0
  },
})

Object.defineProperty(RecordHandler.prototype, 'connected', {
  get: function connected() {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  },
})

Object.defineProperty(RecordHandler.prototype, 'stats', {
  get: function stats() {
    return Object.assign({}, this._stats, {
      listeners: this._listeners.size,
      records: this._records.size,
    })
  },
})

RecordHandler.prototype.getRecord = function (name) {
  invariant(
    typeof name === 'string' && name.length > 0 && !name.includes('[object Object]'),
    `invalid name ${name}`
  )

  let record = this._records.get(name)

  if (!record) {
    record = new Record(name, this)
    this._records.set(name, record)
  }

  record.ref()

  return record
}

RecordHandler.prototype.provide = function (
  pattern,
  callback,
  recursive = false,
  stringify = null
) {
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

  const listener = new Listener(C.TOPIC.RECORD, pattern, callback, this, recursive, stringify)

  this._listeners.set(pattern, listener)
  return () => {
    listener._$destroy()
    this._listeners.delete(pattern)
  }
}

RecordHandler.prototype.sync = function () {
  // TODO (perf): Optimize

  const pending = []
  const records = []
  for (const rec of this._pendingWrite) {
    rec.ref()
    pending.push(rec.when())
    records.push(rec)
  }

  return new Promise((resolve) => {
    let token
    let timeout

    const onTimeout = () => {
      if (!this.connected) {
        timeout = setTimeout(onTimeout, 2 * 60e3)
        return
      }

      for (const rec of records) {
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

    return Promise.all(pending).then(() => {
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

RecordHandler.prototype.get = function (name, ...args) {
  let path
  let state = C.RECORD_STATE.SERVER
  let signal
  let timeout = 2 * 60e3
  let first

  let idx = 0

  if (idx < args.length && (args[idx] == null || typeof args[idx] === 'string')) {
    path = args[idx++]
  }

  if (idx < args.length && (args[idx] == null || typeof args[idx] === 'number')) {
    state = args[idx++]
  }

  if (idx < args.length && (args[idx] == null || typeof args[idx] === 'object')) {
    const options = args[idx++] || {}

    signal = options.signal

    if (options.timeout != null) {
      timeout = options.timeout
    }

    if (options.path != null) {
      path = options.path
    }

    if (options.state != null) {
      state = options.state
    }

    if (options.first != null) {
      first = options.first
    }
  }

  let x$ = this.observe2(name, path, state)

  if (signal != null) {
    x$ = signal.aborted ? rxjs.EMPTY : x$.pipe(rx.takeUntil(rxjs.fromEvent(signal, 'abort')))
    x$ = x$.pipe(rx.throwIfEmpty(() => new utils.AbortError()))
  }

  return x$.pipe(rx.first(first), rx.pluck('data'), rx.timeout(timeout)).toPromise()
}

RecordHandler.prototype.set = function (name, pathOrData, dataOrNil) {
  const record = this.getRecord(name)
  try {
    return arguments.length === 2 ? record.set(pathOrData) : record.set(pathOrData, dataOrNil)
  } finally {
    record.unref()
  }
}

RecordHandler.prototype.update = function (name, ...args) {
  try {
    const record = this.getRecord(name)
    try {
      return record.update(...args)
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
  let state = stateOrNil == null ? C.RECORD_STATE.SERVER : stateOrNil

  if (typeof state === 'string') {
    state = C.RECORD_STATE[state.toUpperCase()]
  }

  if (!name) {
    return rxjs.of(jsonPath.EMPTY)
  }

  return new rxjs.Observable((o) => {
    const onUpdate = (record) => {
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
  }).pipe(rx.distinctUntilChanged())
}

RecordHandler.prototype.observe2 = function (name, pathOrState, stateOrNil) {
  if (arguments.length === 2 && typeof pathOrState === 'number') {
    stateOrNil = pathOrState
    pathOrState = undefined
  }

  const path = pathOrState
  let state = stateOrNil

  if (typeof state === 'string') {
    state = C.RECORD_STATE[state.toUpperCase()]
  }

  if (!name) {
    return rxjs.of(
      utils.deepFreeze({
        name,
        version: '0-00000000000000',
        data: path ? undefined : jsonPath.EMPTY,
        state: Number.isFinite(state) ? state : C.RECORD_STATE.SERVER,
      })
    )
  }

  return new rxjs.Observable((o) => {
    const onUpdate = (record) => {
      if (!state || record.state >= state) {
        o.next({
          name,
          version: record.version,
          data: record.get(path),
          state: record.state,
        })
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
    for (const token of this._syncEmitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
    }
  }
}

module.exports = RecordHandler
