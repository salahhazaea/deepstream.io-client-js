const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const rxjs = require('rxjs')
const invariant = require('invariant')
const EventEmitter = require('component-emitter2')
const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const rx = require('rxjs/operators')
const fastJson = require('fast-json-stringify')

const RecordHandler = function (options, connection, client) {
  this.STATE = C.RECORD_STATE
  this.JSON = jsonPath
  this.defaultTimeout = 2 * 60e3

  Object.assign(this, C.RECORD_STATE)

  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._prune = new Map()
  this._pendingWrite = new Set()
  this._now = Date.now()
  this._pruning = false

  this._syncEmitter = new EventEmitter()
  this._syncCounter = 0

  this._stats = {
    reads: 0,
    hits: 0,
    misses: 0,
  }

  this._schedule = options.schedule ?? utils.schedule

  if (options.cache) {
    this._cache = options.cache
    if (typeof this._cache.on === 'function') {
      this._cache.on('error', (err) => {
        this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
      })
    }
  } else {
    this._cache = {
      get(name, callback) {
        callback(null, null)
      },
    }
  }

  this._client.on('connectionStateChanged', (state) => {
    if (state === C.CONNECTION_STATE.OPEN) {
      this._handleConnectionStateChange(true)
    } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
      this._handleConnectionStateChange(false)
    }
  })

  const prune = (deadline) => {
    if (!this.connected) {
      this._pruning = false
      return
    }

    const batch =
      this._cache && typeof this._cache.batch === 'function' ? this._cache.batch() : null

    try {
      let n = 0
      for (const [rec, timestamp] of this._prune) {
        if (!rec.isReady) {
          continue
        }

        const ttl =
          rec.state >= C.RECORD_STATE.PROVIDER || Object.keys(rec.data).length === 0 ? 1e3 : 10e3

        if (rec._dirty) {
          rec._dirty = false
          const value = [rec.version, rec.data]
          if (batch) {
            batch.put(rec.name, value)
          } else if (this._cache.put) {
            this._cache.put(rec.name, value)
          } else if (this._cache.set) {
            this._cache.set(rec.name, value)
          }
        }

        if (this._now - timestamp <= ttl) {
          continue
        }

        this._records.delete(rec.name)
        this._prune.delete(rec)
        rec._$destroy()

        if (n++ > 256) {
          this._schedule(prune)
          return
        }

        if (deadline && !deadline.timeRemaining() && !deadline.didTimeout) {
          this._schedule(prune)
          return
        }
      }
    } finally {
      if (batch) {
        batch.write((err) => {
          if (err) {
            this._client._$onError(C.TOPIC.RECORD, C.EVENT.CACHE_ERROR, err)
          }
        })
      }
    }

    this._pruning = false
  }

  setInterval(() => {
    this._now = Date.now()
    if (!this._pruning) {
      this._pruning = true
      this._schedule(prune)
    }
  }, 1e3)
}

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

RecordHandler.prototype.provide = function (pattern, callback, recursive = false, schema = null) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  let options

  if (recursive && typeof recursive === 'object') {
    options = recursive
  } else {
    options = {
      recursive,
      schema,
    }
  }

  const stringify = options.schema ? fastJson(options.schema) : null

  if (this._listeners.has(pattern)) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LISTENER_EXISTS, new Error(pattern))
    return
  }

  const listener = new Listener(
    C.TOPIC.RECORD,
    pattern,
    callback,
    this,
    options.recursive,
    stringify
  )

  this._listeners.set(pattern, listener)
  return () => {
    listener._$destroy()
    this._listeners.delete(pattern)
  }
}

RecordHandler.prototype.sync = function (options) {
  // TODO (perf): Optimize

  const signal = options && options.signal

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

    const onDone = (val) => {
      clearTimeout(timeout)

      if (signal) {
        signal.removeEventListener('abort', onAbort)
      }

      if (token) {
        this._syncEmitter.off(token, onToken)
      }

      resolve(val)
    }

    const onToken = () => {
      onDone(true)
    }

    const onAbort = () => {
      onDone(Promise.reject(new utils.AbortError()))
    }

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

      this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'sync timeout', [token])

      onDone(false)
    }

    timeout = setTimeout(onTimeout, 2 * 60e3)

    if (signal) {
      signal.addEventListener('abort', onAbort)
    }

    return Promise.all(pending).then(() => {
      token = this._syncCounter.toString(16)
      this._syncCounter = (this._syncCounter + 1) & 2147483647

      this._syncEmitter.once(token, onToken)

      if (this.connected) {
        this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
      }
    })
  })
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

RecordHandler.prototype.get = function (...args) {
  return new Promise((resolve, reject) => {
    this._observe(
      {
        state: C.RECORD_STATE.SERVER,
        timeout: 2 * 60e3,
      },
      ...args
    )
      .pipe(rx.pluck('data'), rx.first())
      .subscribe({
        next: resolve,
        error: reject,
      })
  })
}

RecordHandler.prototype.observe = function (...args) {
  return this._observe(
    {
      state: C.RECORD_STATE.SERVER,
      timeout: this.defaultTimeout ?? 2 * 60e3,
    },
    ...args
  ).pipe(rx.pluck('data'), rx.distinctUntilChanged())
}

RecordHandler.prototype.observe2 = function (...args) {
  return this._observe({}, ...args)
}

RecordHandler.prototype._observe = function (defaults, name, ...args) {
  let path
  let state = defaults.state
  let signal
  let timeout = defaults.timeout

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
  }

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

  let x$ = new rxjs.Observable((o) => {
    let timeoutHandle

    const onUpdate = (record) => {
      if (state && record.state < state) {
        return
      }

      if (timeoutHandle) {
        clearTimeout(timeoutHandle)
        timeoutHandle = null
      }

      o.next({
        name,
        version: record.version,
        data: record.get(path),
        state: record.state,
      })
    }

    const record = this.getRecord(name)

    if (timeout && state && record.state < state) {
      timeoutHandle = setTimeout(() => {
        const expected = C.RECORD_STATE_NAME[state]
        const current = C.RECORD_STATE_NAME[record.state]
        o.error(new Error(`timeout after ${timeout / 1e3}s: ${name} [${current}<${expected}]`))
      }, timeout)
    }

    if (record.version) {
      onUpdate(record)
    }

    record.on('update', onUpdate)
    return () => {
      record.off('update', onUpdate)
      record.unref()
    }
  })

  if (signal != null) {
    // TODO (perf): This a slow way to implement.
    x$ = signal.aborted ? rxjs.EMPTY : x$.pipe(rx.takeUntil(rxjs.fromEvent(signal, 'abort')))
    x$ = x$.pipe(rx.throwIfEmpty(() => new utils.AbortError()))
  }

  return x$
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
