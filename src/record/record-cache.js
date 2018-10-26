const LRU = require('lru-cache')
const levelup = require('levelup')

const RecordCache = function (options, handler) {
  this._handler = handler
  this._lru = new LRU({ max: options.cacheSize || 512 })
  this._db = options.cacheDb ? levelup(options.cacheDb) : null
  this._batch = this._db ? this._db.batch() : null
}

RecordCache.prototype.get = function (name, callback) {
  const entry = this._lru.get(name)
  if (entry) {
    callback(null, entry)
  } else if (this._db) {
    this._db.get(callback)
  } else {
    callback(null)
  }
}

RecordCache.prototype.set = function (name, version, data) {
  const entry = [ version, data ]
  this._lru.set(name, entry)
  if (this._batch && /^[^0I]/.test(version)) {
    this._batch.put(name, entry)
  }
}

RecordCache.prototype.flush = function (callback) {
  if (this._batch) {
    this._batch.write(callback)
    this._batch = this._db.batch()
  }
}

module.exports = RecordCache
