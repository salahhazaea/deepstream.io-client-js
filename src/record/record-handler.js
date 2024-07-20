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

function onUpdate(record, subscription) {
  if (subscription.state && record.state < subscription.state) {
    return
  }

  if (subscription.timeout) {
    timers.clearTimeout(subscription.timeout)
    subscription.timeout = null
  }

  const data = subscription.path ? record.get(subscription.path) : record.data

  if (subscription.dataOnly) {
    if (data !== subscription.data) {
      subscription.data = data
      subscription.subscriber.next(data)
    }
  } else {
    subscription.subscriber.next({
      name: record.name,
      version: record.version,
      state: record.state,
      data,
    })
  }
}

function onTimeout(subscription) {
  const expected = C.RECORD_STATE_NAME[subscription.state]
  const current = C.RECORD_STATE_NAME[subscription.record.state]

  subscription.subscriber.error(
    Object.assign(
      new Error(`timeout state: ${subscription.record.name} [${current}<${expected}]`),
      {
        code: 'ETIMEDOUT',
      }
    )
  )
}

function onUpdateFast(rec, opaque) {
  const { timeout, resolve, synced, state } = opaque

  if (rec.state >= state && synced) {
    timers.clearTimeout(timeout)
    rec.unsubscribe(onUpdateFast, opaque)
    rec.unref()
    resolve(rec.data)
  }
}

function onSyncFast(opaque) {
  opaque.synced = true
  onUpdateFast(opaque.rec, opaque)
}

function onTimeoutFast(opaque) {
  const { rec, synced, resolve } = opaque
  rec.unsubscribe(onUpdateFast, opaque)
  rec.unref()

  let err
  if (rec.state < opaque.state) {
    err = new Error(`timeout state: ${opaque.rec.name} [${opaque.rec.state}<${opaque.state}]`)
  } else if (!synced) {
    err = new Error(`timeout sync: ${opaque.rec.name} `)
  } else {
    err = new Error('timeout')
  }

  resolve(Promise.reject(Object.assign(err, { code: 'ETIMEDOUT' })))
}

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
    this._pruning = new Set()
    this._patching = new Map()
    this._updating = new Map()

    this._connected = 0
    this._stats = {
      updating: 0,
      created: 0,
      destroyed: 0,
      records: 0,
      pruning: 0,
      patching: 0,
    }

    this._syncQueue = []
    this._syncEmitter = new EventEmitter()
    this._readyEmitter = new EventEmitter()

    this.set = this.set.bind(this)
    this.get = this.get.bind(this)
    this.update = this.update.bind(this)
    this.observe = this.observe.bind(this)
    this.observe2 = this.observe2.bind(this)
    this.sync = this.sync.bind(this)
    this.provide = this.provide.bind(this)
    this.getRecord = this.getRecord.bind(this)

    this._client.on(C.EVENT.CONNECTED, this._onConnectionStateChange.bind(this))

    const _prune = () => {
      const pruning = this._pruning
      this._pruning = new Set()

      for (const rec of pruning) {
        rec._$dispose()
        this._records.delete(rec.name)
      }

      this._stats.pruning -= pruning.size
      this._stats.records -= pruning.size
      this._stats.destroyed += pruning.size

      this._pruningTimeout.refresh()
    }

    this._pruningTimeout = timers.setTimeout(_prune, 1e3)
  }

  _onPruning(rec, value) {
    if (value) {
      this._stats.pruning += 1
    } else {
      this._stats.pruning -= 1
    }

    if (value) {
      this._pruning.add(rec)
    } else {
      this._pruning.delete(rec)
    }
  }

  _onUpdating(rec, value) {
    if (value) {
      this._stats.updating += 1
      this._updating.set(rec, [])
    } else {
      this._stats.updating -= 1

      const callbacks = this._updating.get(rec)
      this._updating.delete(rec)
      for (const callback of callbacks) {
        callback()
      }
    }
  }

  _onPatching(rec, value) {
    if (value) {
      this._stats.patching += 1
      this._patching.set(rec, [])
    } else {
      this._stats.patching -= 1

      const callbacks = this._patching.get(rec)
      this._patching.delete(rec)
      for (const callback of callbacks) {
        callback()
      }
    }
  }

  get connected() {
    return Boolean(this._connected)
  }

  get stats() {
    let subscriptions = 0
    for (const listener of this._listeners.values()) {
      subscriptions += listener.subscriptions ?? 0
    }

    return {
      ...this._stats,
      subscriptions,
    }
  }

  /**
   * @param {string} name
   * @returns {Record}
   */
  getRecord(name) {
    invariant(
      typeof name === 'string' && name.length > 0 && name !== '[object Object]',
      `invalid name ${name}`
    )

    let record = this._records.get(name)

    if (!record) {
      record = new Record(name, this)
      this._stats.records += 1
      this._stats.created += 1
      this._records.set(name, record)
    }

    return record.ref()
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

    this._stats.listeners += 1
    this._listeners.set(pattern, listener)

    return () => {
      listener._$destroy()

      this._stats.listeners -= 1
      this._listeners.delete(pattern)
    }
  }

  async sync(opts) {
    // TODO (fix): Sync pending? What about VOID state?

    let onAbort

    const signal = opts?.signal
    const timeout = opts?.timeout

    const signalPromise = signal
      ? new Promise((resolve, reject) => {
          onAbort = () => {
            reject(signal.reason ?? new utils.AbortError())
          }
          signal.addEventListener('abort', onAbort)
        })
      : null
    signalPromise?.catch(() => {})

    try {
      if (this._patching.size) {
        let patchingTimeout
        const patching = [...this._patching.values()]
        await Promise.race([
          Promise.all(
            patching.map((callbacks) => new Promise((resolve) => callbacks.push(resolve)))
          ),
          new Promise((resolve) => {
            patchingTimeout = timers.setTimeout(() => {
              this._client._$onError(
                C.TOPIC.RECORD,
                C.EVENT.TIMEOUT,
                Object.assign(new Error('sync patching timeout'), { data: { patching, timeout } })
              )
              resolve(null)
            }, timeout ?? 2 * 60e3)
          }),
          signalPromise,
        ]).finally(() => {
          timers.clearTimeout(patchingTimeout)
        })
      }

      if (this._updating.size) {
        let updatingTimeout
        const updating = [...this._updating.values()]
        await Promise.race([
          Promise.all(
            updating.map((callbacks) => new Promise((resolve) => callbacks.push(resolve)))
          ),
          new Promise((resolve) => {
            updatingTimeout = timers.setTimeout(() => {
              this._client._$onError(
                C.TOPIC.RECORD,
                C.EVENT.TIMEOUT,
                Object.assign(new Error('sync updating timeout'), { data: { updating, timeout } })
              )
              resolve(null)
            }, timeout ?? 2 * 60e3)
          }),
          signalPromise,
        ]).finally(() => {
          timers.clearTimeout(updatingTimeout)
        })
      }

      let serverTimeout
      const token = xuid()
      return await Promise.race([
        await new Promise((resolve) => {
          this._syncEmitter.once(token, resolve)
          this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token])
        }),
        new Promise((resolve) => {
          serverTimeout = timers.setTimeout(() => {
            this._client._$onError(
              C.TOPIC.RECORD,
              C.EVENT.TIMEOUT,
              Object.assign(new Error('sync server timeout'), { data: { token, timeout } })
            )
            resolve(null)
          }, timeout ?? 2 * 60e3)
        }),
        signalPromise,
      ]).finally(() => {
        timers.clearTimeout(serverTimeout)
      })
    } finally {
      if (onAbort) {
        signal?.removeEventListener('abort', onAbort)
      }
    }
  }

  _sync(callback, opaque) {
    this._syncQueue.push(callback, opaque)

    if (this._syncQueue.length > 2) {
      return
    }

    setTimeout(() => {
      // Token must be universally unique until deepstream properly separates
      // sync requests from different sockets.
      const token = xuid()
      const queue = this._syncQueue.splice(0)
      this._syncEmitter.once(token, () => {
        for (let n = 0; n < queue.length; n += 2) {
          queue[n](queue[n + 1])
        }
      })
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [token, 'WEAK'])
    }, 1)
  }

  set(name, ...args) {
    const record = this.getRecord(name)
    try {
      return record.set(...args)
    } finally {
      record.unref()
    }
  }

  /**
   *
   * @param {*} name
   * @param  {...any} args
   * @returns {Promise}
   */
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

  /**
   * @param  {...any} args
   * @returns {rxjs.Observable}
   */
  observe(...args) {
    return this._observe(
      {
        state: C.RECORD_STATE.SERVER,
        timeout: 2 * 60e3,
        dataOnly: true,
      },
      ...args
    )
  }

  /**
   * @param  {...any} args
   * @returns {Promise}
   */
  get(...args) {
    if (args.length === 1 || (args.length === 2 && typeof args[1] === 'number')) {
      return new Promise((resolve) => {
        const rec = this.getRecord(args[0])
        const state = args.length === 2 ? args[1] : C.RECORD_STATE.SERVER
        // TODO (perf): We could also skip sync if state is less than SERVER. However,
        // there is a potential race where we receive an UPDATE for a previous SUBSCRIBE.
        // Unsure how to avoid that. Keep it simple for now and always sync regardless of
        // current state.
        const synced = state < C.RECORD_STATE.SERVER

        if (rec.state >= state && synced) {
          rec.unref()
          resolve(rec.data)
        } else {
          const opaque = {
            rec,
            state,
            resolve,
            timeout: null,
            synced,
          }
          opaque.timeout = timers.setTimeout(onTimeoutFast, 2 * 60e3, opaque)
          rec.subscribe(onUpdateFast, opaque)

          if (!opaque.synced) {
            this._sync(onSyncFast, opaque)
          }
        }
      })
    } else {
      // Slow path...
      // TODO (fix): Missing sync..
      return new Promise((resolve, reject) => {
        this.observe(...args)
          .pipe(rx.first())
          .subscribe({
            next: resolve,
            error: reject,
          })
      })
    }
  }

  /**
   * @param  {...any} args
   * @returns {Promise}
   */
  get2(...args) {
    return new Promise((resolve, reject) => {
      this.observe2(...args)
        .pipe(rx.first())
        .subscribe({
          next: resolve,
          error: reject,
        })
    })
  }

  /**
   * @param  {...any} args
   * @returns {rxjs.Observable<{ name: string, version: string, state: Number, data: any}>}
   */
  observe2(...args) {
    return this._observe(
      {
        timeout: 2 * 60e3,
      },
      ...args
    )
  }

  /**
   * @returns {rxjs.Observable}
   */
  // TODO (perf): Avoid rest parameters.
  _observe(defaults, name, ...args) {
    let path
    let state = defaults ? defaults.state : undefined
    let signal
    let timeout = defaults ? defaults.timeout : undefined
    let dataOnly = defaults ? defaults.dataOnly : undefined

    let idx = 0

    if (
      idx < args.length &&
      (args[idx] == null ||
        typeof args[idx] === 'string' ||
        Array.isArray(args[idx]) ||
        typeof args[idx] === 'function')
    ) {
      path = args[idx++]
    }

    if (idx < args.length && (args[idx] == null || typeof args[idx] === 'number')) {
      state = args[idx++]
    }

    if (idx < args.length && (args[idx] == null || typeof args[idx] === 'object')) {
      const options = args[idx++] || {}

      if (options.signal !== undefined) {
        signal = options.signal
      }

      if (options.timeout !== undefined) {
        timeout = options.timeout
      }

      if (options.path !== undefined) {
        path = options.path
      }

      if (options.state !== undefined) {
        state = options.state
      }

      if (options.dataOnly !== undefined) {
        dataOnly = options.dataOnly
      }
    }

    if (typeof state === 'string') {
      state = C.RECORD_STATE[state.toUpperCase()]
    }

    // TODO (perf): Avoid subscribe closure allocation.
    return new rxjs.Observable((subscriber) => {
      const subscription = {
        subscriber,
        path,
        state,
        signal,
        dataOnly,
        data: kEmpty,
        timeout: null,
        /** @type {Record?} */ record: null,
        /** @type {Function?} */ abort: null,
        unsubscribe() {
          if (this.timeout) {
            timers.clearTimeout(this.timeout)
            this.timeout = null
          }

          if (this.signal) {
            utils.removeAbortListener(this.signal, this.abort)
            this.signal = null
            this.abort = null
          }

          if (this.record) {
            this.record.unsubscribe(onUpdate, this)
            this.record.unref()
            this.record = null
          }
        },
      }

      const record = (subscription.record = this.getRecord(name).subscribe(onUpdate, subscription))

      if (timeout > 0 && state && record.state < state) {
        // TODO (perf): Avoid Timer allocation.
        subscription.timeout = timers.setTimeout(onTimeout, timeout, subscription)
      }

      if (record.version) {
        onUpdate(record, subscription)
      }

      if (signal) {
        // TODO (perf): Avoid abort closure allocation.
        subscription.abort = () => subscriber.error(new utils.AbortError())
        utils.addAbortListener(signal, subscription.abort)
      }

      return subscription
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
      this._syncEmitter.emit(message.data[0], message.data[1]?.toString())
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
