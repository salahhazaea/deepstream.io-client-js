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
  this._prune = new Map()
  this._syncRef = 0
  this._syncSendQueue = []
  this._syncEmitQueue = []
  this._syncEmitter = new EventEmitter()
  this._syncTimeout = null
  this._syncCounter = 0
  this._lz = options.lz || new LZ()

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._handleConnectionStateChange()

  const prune = () => {
    const now = Date.now()

    for (const [ rec, timestamp ] of this._prune) {
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

      if (now - timestamp <= minAge) {
        continue
      }

      this._store.set(rec.name, rec.data, rec.version)
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

RecordHandler.prototype._$syncRef = function () {
  this._syncRef += 1
}

RecordHandler.prototype._$syncUnref = function () {
  this._syncRef = Math.max(0, this._syncRef - 1)
  this._syncFlush()
}

RecordHandler.prototype._syncFlush = function () {
  if (
    this._syncRef !== 0 ||
    (this._syncEmitQueue.length === 0 && this._syncSendQueue.length === 0)
  ) {
    return
  }

  for (const sync of this._syncEmitQueue) {
    this._syncEmitter.emit(sync)
  }
  this._syncEmitQueue = []

  for (const token of this._syncSendQueue) {
    this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
  }
  this._syncSendQueue = []
}

RecordHandler.prototype.sync = function () {
  const token = this._syncCounter.toString(16)

  if (!this._syncTimeout) {
    this._syncTimeout = setTimeout(() => {
      if (!this._isConnected) {
        return
      }

      this._syncSendQueue.push(token)
      this._syncFlush()

      this._syncCounter = (this._syncCounter + 1) & 2147483647
      this._syncTimeout = null
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
    .catch(err => {
      record.unref()
      throw err
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

RecordHandler.prototype.invalidate = function (name) {
  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.INVALIDATE, [ name ])
}

RecordHandler.prototype.observe = function (name) {
  return Observable
    .create(o => {
      const onUpdate = record => o.next(record.data)
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
    this._syncEmitQueue.push(message.data[0])
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

RecordHandler.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    for (const token of this._syncEmitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
    }
    this._syncCounter = (this._syncCounter + 1) & 2147483647
    this._syncTimeout = null
  } else {
    clearTimeout(this._syncTimeout)
  }
}

module.exports = RecordHandler
