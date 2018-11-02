const LRU = require('lru-cache')
const levelup = require('levelup')
const encodingdown = require('encoding-down')

const RecordCache = function (options, handler) {
  this._handler = handler
  this._lru = new LRU({ max: options.cacheSize || 1024 })
  this._db = options.cacheDb ? levelup(encodingdown(options.cacheDb, { valueEncoding: 'json' })) : null
  this._filter = options.cacheFilter
  this._batch = null
}

RecordCache.prototype.get = function (name, callback) {
  const entry = this._lru.get(name)
  if (entry) {
    callback(null, entry)
  } else if (this._db) {
    this._db.get(name, callback)
  } else {
    callback(null)
  }
}

RecordCache.prototype.set = function (name, version, data) {
  if (!this._filter || !this._filter(name, version, data)) {
    return
  }

  const entry = [ version, data ]
  this._lru.set(name, entry)
  if (this._db) {
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
