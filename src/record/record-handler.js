const Record = require('./record')
const MulticastListener = require('../utils/multicast-listener')
const UnicastListener = require('../utils/unicast-listener')
const C = require('../constants/constants')
const rxjs = require('rxjs')
const invariant = require('invariant')
const EventEmitter = require('component-emitter2')
const jsonPath = require('@nxtedition/json-path')
const utils = require('../utils/utils')
const rx = require('rxjs/operators')
const xuid = require('xuid')
const timers = require('../utils/timers')

const kEmpty = Symbol('kEmpty')

class RecordHandler {
  constructor(options, connection, client) {
    this.JSON = jsonPath
    this.STATE = C.RECORD_STATE
    Object.assign(this, C.RECORD_STATE)

    this._options = options
    this._connection = connection
    this._client = client
    this._records = new Map()
    this._listeners = new Map()
    this._prune = new Map()
    this._patch = new Set()
    this._now = Date.now()
    this._pruning = false
    this._purging = false
    this._connected = 0
    this._stats = {
      updating: 0,
    }

    this._syncEmitter = new EventEmitter()
    this._syncQueue = null

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

    const _prune = () => {
      let counter = 0
      for (const [rec, timestamp] of this._prune) {
        if (rec.refs > 0) {
          this._prune.delete(rec)
          continue
        }

        if (rec.pending) {
          continue
        }

        if (this._now - timestamp < 1e3) {
          return
        }

        rec._unsubscribe()

        this._records.delete(rec.name)
        this._prune.delete(rec)

        if (counter++ > 2048) {
          this._schedule(_prune)
          return
        }
      }

      this._pruning = false
      this._now = Date.now()
    }

    this._pruneInterval = utils.setInterval(() => {
      if (!this._pruning) {
        this._pruning = true
        this._schedule(_prune)
      }
    }, 1e3)
    this._pruneInterval.unref?.()

    this._syncAll = this._syncAll.bind(this)
  }

  _onRef(rec) {
    if (rec.refs === 0) {
      this._prune.set(rec, this._now)
    }
  }

  get connected() {
    return Boolean(this._connected)
  }

  get stats() {
    return {
      ...this._stats,
      listeners: this._listeners.size,
      records: this._records.size,
      pruning: this._prune.size,
    }
  }

  getRecord(name) {
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

  provide(pattern, callback, options) {
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

  sync(options) {
    return new Promise((resolve, reject) => {
      const timeoutValue = options?.timeout ?? 2 * 60e3
      const signal = options?.signal

      if (signal?.aborted) {
        reject(new utils.AbortError())
        return
      }

      let done = false
      let token
      let timeoutHandle

      const records = [...this._patch]

      const onDone = (val) => {
        if (done) {
          return
        }

        done = true

        signal?.removeEventListener('abort', onAbort)

        if (timeoutHandle) {
          timers.clearTimeout(timeoutHandle)
          timeoutHandle = null
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
          timeoutHandle = timers.setTimeout(onTimeout, timeoutValue - elapsed)
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

      if (timeoutValue) {
        timeoutHandle = timers.setTimeout(onTimeout, timeoutValue)
      }

      signal?.addEventListener('abort', onAbort)

      Promise.all(records.map((rec) => rec.when())).then(
        () => {
          if (done) {
            return
          }

          if (!this._syncQueue) {
            this._syncQueue = []
            queueMicrotask(this._syncAll)
          }

          this._syncQueue.push(onToken)
        },
        (err) => onDone(Promise.reject(err))
      )
    })
  }

  _syncAll() {
    if (!this._syncQueue) {
      return
    }

    const syncQueue = this._syncQueue
    const token = xuid()

    this._syncQueue = null
    this._syncEmitter.once(token, () => {
      for (const callback of syncQueue) {
        callback()
      }
    })

    if (this._connected) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
    }
  }

  set(name, ...args) {
    const record = this.getRecord(name)
    try {
      return record.set(...args)
    } finally {
      record.unref()
    }
  }

  update(name, ...args) {
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

  observe(...args) {
    return this._observe(
      {
        state: C.RECORD_STATE.SERVER,
        timeout: 10 * 60e3,
        dataOnly: true,
      },
      ...args
    )
  }

  get(...args) {
    return new Promise((resolve, reject) => {
      this.observe(...args)
        .pipe(rx.first())
        .subscribe({
          next: resolve,
          error: reject,
        })
    })
  }

  observe2(...args) {
    return this._observe(null, ...args)
  }

  _observe(defaults, name, ...args) {
    let path
    let state = defaults ? defaults.state : undefined
    let signal
    let timeoutValue = defaults ? defaults.timeout : undefined
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

      if (options.signal != null) {
        signal = options.signal
      }

      if (options.timeout != null) {
        timeoutValue = options.timeout
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

    if (signal?.aborted) {
      return rxjs.throwError(() => new utils.AbortError())
    }

    return new rxjs.Observable((o) => {
      let timeoutHandle
      let prevData = kEmpty

      const onUpdate = (record) => {
        if (state && record.state < state) {
          return
        }

        if (timeoutHandle) {
          timers.clearTimeout(timeoutHandle)
          timeoutHandle = null
        }

        const nextData = path ? record.get(path) : record.data

        if (dataOnly) {
          if (nextData !== prevData) {
            prevData = nextData
            o.next(nextData)
          }
        } else {
          o.next({
            name: record.name,
            version: record.version,
            data: nextData,
            state: record.state,
          })
        }
      }

      const record = this.getRecord(name).subscribe(onUpdate)

      if (timeoutValue && state && record.state < state) {
        timeoutHandle = timers.setTimeout(() => {
          const expected = C.RECORD_STATE_NAME[state]
          const current = C.RECORD_STATE_NAME[record.state]
          o.error(
            Object.assign(
              new Error(`timeout after ${timeoutValue / 1e3}s: ${name} [${current}<${expected}]`),
              { code: 'ETIMEDOUT' }
            )
          )
        }, timeoutValue)
      }

      if (record.version) {
        onUpdate(record)
      }

      const abort = signal ? () => o.error(new utils.AbortError()) : null

      utils.addAbortListener(signal, abort)

      return () => {
        record.unsubscribe(onUpdate).unref()
        utils.removeAbortListener(signal, abort)
      }
    })
  }

  _$handle(message) {
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

  _onConnectionStateChange(connected) {
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
}

module.exports = RecordHandler
