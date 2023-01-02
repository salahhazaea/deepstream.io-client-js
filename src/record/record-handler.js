const Record = require('./record')
const MulticastListener = require('../utils/multicast-listener')
const UnicastListener = require('../utils/unicast-listener')
const C = require('../constants/constants')
const rxjs = require('rxjs')
const invariant = require('invariant')
const EventEmitter = require('component-emitter2')
const jsonPath = require('./json-path')
const utils = require('../utils/utils')
const rx = require('rxjs/operators')
const xuid = require('xuid')

const kEmpty = Symbol('kEmpty')

const RecordHandler = function (options, connection, client) {
  this.STATE = C.RECORD_STATE
  this.JSON = jsonPath

  Object.assign(this, C.RECORD_STATE)

  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._prune = new Map()
  this._purge = new Set()
  this._pending = new Set()
  this._now = Date.now()
  this._pruning = null
  this._purging = null
  this._connected = 0
  this._stats = {
    updating: 0,
  }

  this._purgeCapacity = options.cacheSize

  this._syncEmitter = new EventEmitter()

  this.set = this.set.bind(this)
  this.get = this.get.bind(this)
  this.update = this.update.bind(this)
  this.observe = this.observe.bind(this)
  this.observe2 = this.observe2.bind(this)
  this.sync = this.sync.bind(this)
  this.provide = this.provide.bind(this)
  this.getRecord = this.getRecord.bind(this)

  this._schedule = options.schedule ?? utils.schedule

  this._client.on(C.EVENT.CONNECTED, this._onConnectionStateChange.bind(this))

  const prune = () => {
    const now = this._now
    const it = this._pruning

    for (let n = 0; n < 512; n++) {
      const { done, value } = it.next()

      if (done) {
        this._pruning = null
        return
      }

      const [rec, timestamp] = value

      if (!rec.isReady) {
        continue
      }

      const ttl = rec.state >= C.RECORD_STATE.PROVIDER || rec.data === jsonPath.EMPTY ? 1e3 : 10e3

      if (now - timestamp <= ttl) {
        continue
      }

      if (rec.refs === 0) {
        rec._unsubscribe()
        this._purge.add(this)
      }

      this._prune.delete(this)
    }

    this._schedule(prune)
  }

  const purge = () => {
    const it = this._purging

    for (let n = 0; n < 512; n++) {
      const { done, value: rec } = it.next()

      if (done) {
        this._purging = null
        return
      }

      if (rec.refs === 0) {
        this._records.delete(rec.name)
      }

      this._purge.delete(this)
    }

    this._schedule(purge)
  }

  const pruneInterval = setInterval(() => {
    this._now = Date.now()

    if (!this._pruning) {
      this._pruning = this._prune[Symbol.iterator]()
      this._schedule(prune)
    }

    if (!this._purging) {
      this._purging = this._purge[Symbol.iterator]()
      this._schedule(purge)
    }
  }, 1e3)
  pruneInterval.unref?.()
}

Object.defineProperty(RecordHandler.prototype, 'connected', {
  get: function connected() {
    return Boolean(this._connected)
  },
})

Object.defineProperty(RecordHandler.prototype, 'stats', {
  get: function stats() {
    return {
      ...this._stats,
      listeners: this._listeners.size,
      records: this._records.size,
    }
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
  } else {
    record.ref()
  }

  return record
}

RecordHandler.prototype.provide = function (pattern, callback, options) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (!options) {
    options = { recursive: false, stringify: null }
  } else if (options === true) {
    options = { recursive: true, stringify: null }
  }

  if (this._listeners.has(pattern)) {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LISTENER_EXISTS, new Error(pattern))
    return
  }

  const listener =
    options.mode?.toLowerCase() === 'unicast'
      ? new UnicastListener(C.TOPIC.RECORD, pattern, callback, this, options)
      : new MulticastListener(C.TOPIC.RECORD, pattern, callback, this, options)

  this._listeners.set(pattern, listener)
  return () => {
    listener._$destroy()
    this._listeners.delete(pattern)
  }
}

RecordHandler.prototype.sync = function (options) {
  return new Promise((resolve) => {
    let done = false
    let token
    let timeout

    const timeoutValue = 2 * 60e3
    const signal = options?.signal
    const records = [...this._pending]

    const onDone = (val) => {
      if (done) {
        return
      }

      done = true

      signal?.removeEventListener('abort', onAbort)

      if (timeout) {
        clearTimeout(timeout)
        timeout = null
      }

      if (token) {
        this._syncEmitter.off(token, onToken)
        token = null
      }

      for (const rec of records) {
        rec.unref()
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
      const elapsed = Date.now() - this._connected
      if (elapsed < timeoutValue) {
        timeout = setTimeout(onTimeout, timeoutValue - elapsed)
        timeout.unref?.()
      } else {
        for (const rec of records.filter((rec) => !rec.isReady)) {
          this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'record timeout', [
            rec.name,
            rec.version,
            rec.state,
            ...(rec._entry ?? []),
          ])
        }

        this._client._$onError(C.TOPIC.RECORD, C.EVENT.TIMEOUT, 'sync timeout', [token])

        onDone(false)
      }
    }

    for (const rec of records) {
      rec.ref()
    }

    timeout = setTimeout(onTimeout, 2 * 60e3)
    timeout.unref?.()

    signal?.addEventListener('abort', onAbort)

    Promise.all(records.map((rec) => rec.when())).then(
      () => {
        if (done) {
          return
        }

        token = xuid()
        this._syncEmitter.once(token, onToken)

        if (this._connected) {
          this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
        }
      },
      (err) => onDone(Promise.reject(err))
    )
  })
}

RecordHandler.prototype.set = function (name, ...args) {
  const record = this.getRecord(name)
  try {
    return record.set(...args)
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

RecordHandler.prototype.observe = function (...args) {
  return this._observe(
    {
      state: C.RECORD_STATE.SERVER,
      timeout: 2 * 60e3,
      dataOnly: true,
    },
    ...args
  )
}

RecordHandler.prototype.get = function (...args) {
  return new Promise((resolve, reject) => {
    this.observe(...args)
      .pipe(rx.first())
      .subscribe({
        next: resolve,
        error: reject,
      })
  })
}

RecordHandler.prototype.observe2 = function (...args) {
  return this._observe(null, ...args)
}

RecordHandler.prototype._observe = function (defaults, name, ...args) {
  let path
  let state = defaults ? defaults.state : undefined
  let signal
  let timeout = defaults ? defaults.timeout : undefined
  let dataOnly = defaults ? defaults.dataOnly : undefined

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

    if (options.dataOnly != null) {
      dataOnly = options.dataOnly
    }
  }

  if (typeof state === 'string') {
    state = C.RECORD_STATE[state.toUpperCase()]
  }

  if (!name) {
    const data = path ? undefined : jsonPath.EMPTY
    return rxjs.of(
      dataOnly
        ? data
        : utils.deepFreeze({
            name,
            version: '0-00000000000000',
            data,
            state: Number.isFinite(state) ? state : C.RECORD_STATE.SERVER,
          })
    )
  }

  let x$ = new rxjs.Observable((o) => {
    let timeoutHandle
    let prevData = kEmpty

    const onUpdate = (record) => {
      if (state && record.state < state) {
        return
      }

      if (timeoutHandle) {
        clearTimeout(timeoutHandle)
        timeoutHandle = null
      }

      if (dataOnly) {
        const nextData = record.get(path)
        if (nextData !== prevData) {
          prevData = nextData
          o.next(nextData)
        }
      } else {
        o.next({
          name: record.name,
          version: record.version,
          data: record.get(path),
          state: record.state,
        })
      }
    }

    const record = this.getRecord(name)

    record.subscribe(onUpdate)

    if (timeout && state && record.state < state) {
      timeoutHandle = setTimeout(() => {
        const expected = C.RECORD_STATE_NAME[state]
        const current = C.RECORD_STATE_NAME[record.state]
        o.error(
          Object.assign(
            new Error(`timeout after ${timeout / 1e3}s: ${name} [${current}<${expected}]`),
            { code: 'ETIMEDOUT' }
          )
        )
      }, timeout)
      timeoutHandle.unref?.()
    }

    if (record.version) {
      onUpdate(record)
    }

    return () => {
      record.unsubscribe(onUpdate)
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

RecordHandler.prototype._onConnectionStateChange = function (connected) {
  for (const listener of this._listeners.values()) {
    listener._$onConnectionStateChange(connected)
  }

  for (const record of this._records.values()) {
    record._$onConnectionStateChange(connected)
  }

  if (connected) {
    this._connected = Date.now()
    for (const token of this._syncEmitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
    }
  } else {
    this._connected = 0
  }
}

module.exports = RecordHandler
