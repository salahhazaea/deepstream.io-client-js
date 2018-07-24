const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const { Observable } = require('rxjs')
const invariant = require('invariant')
const LZ = require('./lz')
const utils = require('../utils/utils')
const EventEmitter = require('component-emitter2')
const RecordStore = require('./record-store')

const RecordHandler = function (options, connection, client) {
  this.isAsync = true
  this._pool = []
  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._store = new RecordStore(options, this)
  this._prune = new Set()
  this._syncEmitter = new EventEmitter()
  this._syncTimeout = null
  this._syncCounter = 0
  this._lz = options.lz || new LZ()

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._handleConnectionStateChange()

  const prune = () => {
    const now = Date.now()

    for (const rec of this._prune) {
      if (rec.usages !== 0) {
        this._prune.delete(rec)
        continue
      }

      if (!rec.isReady) {
        continue
      }

      const minAge = rec.version && rec.version.startsWith('I')
        ? 1000
        : 10000

      if (now - rec.timestamp <= minAge) {
        continue
      }

      this._store.set(rec.name, rec._data, rec.version)
      this._records.delete(rec.name)

      rec._$destroy()

      this._prune.delete(rec)
      this._pool.push(rec)
    }

    setTimeout(() => utils.schedule(prune, { timeout: 1000 }), 1000)
  }

  prune()
}

Object.defineProperty(RecordHandler.prototype, '_isConnected', {
  get: function _isConnected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

RecordHandler.prototype.getRecord = function (name) {
  invariant(typeof name === 'string' && name.length > 0 && !name.includes('[object Object]'), `invalid name ${name}`)

  let record = this._records.get(name)

  if (!record) {
    record = this._pool.pop() || new Record(this)
    record.init(name)
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
    this._options,
    this._client,
    this._connection,
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
  const token = this._syncCounter.toString(16)

  if (!this._syncTimeout) {
    this._syncTimeout = setTimeout(() => {
      if (this._isConnected) {
        this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
      }

      this._syncTimeout = null
      this._syncCounter = (this._syncCounter + 1) & 2147483647
    }, 1)
  }

  return new Promise(resolve => this._syncEmitter.once(token, resolve))
}

RecordHandler.prototype.get = function (name, pathOrNil) {
  const record = this.getRecord(name)
  return record
    .whenReady()
    .then(() => {
      const val = record.get(pathOrNil)
      record.unref()
      return val
    })
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
  const record = this.getRecord(name)
  try {
    return arguments.length === 2
      ? record.update(pathOrUpdater)
      : record.update(pathOrUpdater, updaterOrNil)
  } finally {
    record.unref()
  }
}

RecordHandler.prototype.observe = function (name) {
  return Observable
    .create(o => {
      const record = this.getRecord(name)
      const onUpdate = () => o.next(record.get())
      record.on('data', onUpdate)
      if (record.version) {
        onUpdate()
      }
      return () => {
        record.off('data', onUpdate)
        record.unref()
      }
    })
}

RecordHandler.prototype.observe2 = function (name) {
  return Observable
    .create(o => {
      const record = this.getRecord(name)
      const onUpdate = () => o.next({
        data: record.get(),
        ready: record.isReady,
        empty: Object.keys(record.get()).length === 0,
        provided: record.hasProvider,
        version: record.version
      })
      record.on('data', onUpdate)
      record.on('ready', onUpdate)
      record.on('hasProviderChanged', onUpdate)
      if (record.version) {
        onUpdate()
      }
      return () => {
        record.off('data', onUpdate)
        record.off('ready', onUpdate)
        record.off('hasProviderChanged', onUpdate)
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

RecordHandler.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    for (const token of this._syncEmitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
    }
  } else {
    clearTimeout(this._syncTimeout)
  }
}


module.exports = RecordHandler
