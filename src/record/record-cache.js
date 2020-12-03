const levelup = require('levelup')
const encodingdown = require('encoding-down')

/* global FinalizationRegistry, WeakRef */

const RecordCache = function (options, callback) {
  this._cache = null
  this._finalizer = null
  this._db = options.cacheDb ? levelup(encodingdown(options.cacheDb, { valueEncoding: 'json' }), callback) : null
  this._filter = options.cacheFilter
  this._batch = null

  if (global.FinalizationRegistry && global.WeakRef) {
    this._cache = new Map()
    this._finalizer = new FinalizationRegistry(name => {
      const ref = this._cache.get(name)
      if (ref && !ref.deref()) {
        this._cache.delete(name)
      }
    })
  }
}

RecordCache.prototype.get = function (name, callback) {
  const ref = this._cache && this._cache.get(name)
  const entry = ref && ref.deref()
  if (entry) {
    callback(null, entry)
  } else if (this._db && this._db.isOpen()) {
    this._db.get(name, callback)
  } else {
    callback(null)
  }
}

RecordCache.prototype.set = function (name, entry) {
  if (this._filter && !this._filter(name, entry[0], entry[1])) {
    return
  }

  if (this._cache) {
    this._cache.set(name, new WeakRef(entry))
    this._finalizer.register(entry, name)
  }

  if (this._db && this._db.isOpen()) {
    if (!this._batch) {
      this._batch = this._db.batch()
    }
    this._batch.put(name, entry)
  }
}

RecordCache.prototype.flush = function (callback) {
  if (this._batch) {
    this._batch.write(callback)
    this._batch = null
  }
}

module.exports = RecordCache
