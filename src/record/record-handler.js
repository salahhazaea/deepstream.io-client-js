const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const { Observable } = require('rxjs')
const LRU = require('lru-cache')
const invariant = require('invariant')
const lz = require('@nxtedition/lz-string')
const utils = require('../utils/utils')

const schedule = utils.isNode ? cb => cb() : (cb, options) => window.requestIdleCallback(cb, options)

const RecordHandler = function (options, connection, client) {
  const cache = new LRU({ max: options.cacheSize || 512 })
  const db = options.cacheDb

  this.isAsync = true
  this._pool = []
  this._options = options
  this._connection = connection
  this._client = client
  this._records = new Map()
  this._listeners = new Map()
  this._cache = {
    get (name, callback) {
      const val = cache.get(name)
      if (val) {
        callback(null, val[0], val[1])
      } else if (db) {
        // TODO (perf): allDocs
        db.get(name, (err, doc) => {
          if (err) {
            callbac(err)
          } else {
            const version = doc._rev
            delete doc._id
            delete doc._rev
            callback(null, doc, version)
          }
        })
      } else {
        callback(null, null)
      }
    }
  }
  this._prune = new Set()
  this._sync = new Map()
  this._syncGen = 0
  this._lz = {
    compress (obj, cb) {
      try {
        cb(lz.compressToUTF16(JSON.stringify(obj)))
      } catch (err) {
        cb(null, err)
      }
    },
    decompress (raw, cb) {
      try {
        cb(typeof raw === 'string' ? JSON.parse(lz.decompressFromUTF16(raw)) : raw)
      } catch (err) {
        cb(null, err)
      }
    }
  }

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._handleConnectionStateChange()

  const prune = () => {
    const now = Date.now()
    const docs = []

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

      if (db && /^[^I0]/.test(rec.version)) {
        docs.push({
          _id: rec.name,
          _rev: rec.version,
          ...rec._data
        })
      }

      cache.set(rec.name, [ rec._data, rec.version ])

      rec._$destroy()

      this._prune.delete(rec)
      this._records.delete(rec.name)
      this._pool.push(rec)
    }

    if (db && docs.length > 0) {
      this
        .sync()
        .then(() => schedule(() => db
          .bulkDocs(docs, { new_edits: false }, err => {
            if (err) {
              console.error(err)
            }
          })
        ))
    }

    setTimeout(() => schedule(prune, { timeout: 1000 }), 1000)
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

  record.acquire()

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
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.LISTENER_EXISTS, pattern)
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
  // TODO (perf): setTimeout and share sync.
  return new Promise(resolve => {
    this._syncGen = (this._syncGen + 1) & 2147483647

    const token = this._syncGen.toString(16)

    if (this._isConnected) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
    }

    this._sync.set(token, resolve)
  })
}

RecordHandler.prototype.get = function (name, pathOrNil) {
  const record = this.getRecord(name)
  return record
    .whenReady()
    .then(() => {
      const val = record.get(pathOrNil)
      record.discard()
      return val
    })
}

RecordHandler.prototype.set = function (name, pathOrData, dataOrNil) {
  const record = this.getRecord(name)
  const promise = arguments.length === 2
    ? record.set(pathOrData)
    : record.set(pathOrData, dataOrNil)
  record.discard()
  return promise
}

RecordHandler.prototype.update = function (name, pathOrUpdater, updaterOrNil) {
  const path = arguments.length === 2 ? undefined : pathOrUpdater
  const updater = arguments.length === 2 ? pathOrUpdater : updaterOrNil

  const record = this.getRecord(name)
  return record
    .whenReady()
    .then(() => updater(record.get(path)))
    .then(val => {
      if (path) {
        record.set(path, val)
      } else {
        record.set(val)
      }
      record.discard()
      return val
    })
    .catch(err => {
      record.discard()
      throw err
    })
}

RecordHandler.prototype.observe = function (name) {
  return Observable
    .create(o => {
      try {
        const record = this.getRecord(name)
        const onValue = value => o.next(value)
        record.subscribe(onValue, true)
        return () => {
          record.unsubscribe(onValue)
          record.discard()
        }
      } catch (err) {
        o.error(err)
      }
    })
}

RecordHandler.prototype.observe2 = function (name) {
  return Observable
    .create(o => {
      try {
        const record = this.getRecord(name)
        const onUpdate = () => o.next({
          data: record.get(),
          ready: record.isReady,
          empty: Object.keys(record.get()).length === 0,
          provided: record.isReady && record.hasProvider,
          version: record.version
        })
        record.subscribe(onUpdate)
        record.on('ready', onUpdate)
        record.on('hasProviderChanged', onUpdate)
        onUpdate()
        return () => {
          record.unsubscribe(onUpdate)
          record.off('ready', onUpdate)
          record.off('hasProviderChanged', onUpdate)
          record.discard()
        }
      } catch (err) {
        o.error(err)
      }
    })
}

// TODO deprecate
RecordHandler.prototype.isReady = function (name) {
  return Observable
    .create(o => {
      try {
        const record = this.getRecord(name)
        const onReady = value => o.next(value)
        record.on('ready', onReady)
        onReady(record.isReady)
        return () => {
          record.off('ready', onReady)
          record.discard()
        }
      } catch (err) {
        o.error(err)
      }
    })
}

// TODO deprecate
RecordHandler.prototype.hasProvider = function (name) {
  return Observable
    .create(o => {
      try {
        const record = this.getRecord(name)
        const onValue = value => o.next(value)
        record.on('hasProviderChanged', onValue)
        onValue(record.hasProvider)
        return () => {
          record.off('hasProviderChanged', onValue)
          record.discard()
        }
      } catch (err) {
        o.error(err)
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
    const resolve = this._sync.get(message.data[0])
    if (resolve) {
      resolve()
    }
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
    for (const token of this._sync.keys()) {
      this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.SYNC, [ token ])
    }
  }
}


module.exports = RecordHandler
