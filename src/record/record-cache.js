const LRU = require('lru-cache')
const levelup = require('levelup')
const encodingdown = require('encoding-down')

function defaultFilter (name, version, data) {
  return /^[^{]/.test(name) && /^[^0]/.test(version)
}

const RecordCache = function (options, callback) {
  if (options.cache && typeof cache === 'object') {
    options = options.cache
  } else {
    options = {
      size: options.cacheSize,
      db: options.cacheDb,
      filter: options.cacheFilter,
      ...options
    }
  }

  const size = options.size || 1024
  const db = options.db || null
  const filter = options.filter || defaultFilter

  this._lru = new LRU({ max: size })
  this._db = db ? levelup(encodingdown(db, { valueEncoding: 'json' }), callback) : null
  this._filter = filter
  this._batch = null
}

RecordCache.prototype.get = function (name, callback) {
  const entry = this._lru.get(name)
  if (entry) {
    callback(null, entry)
  } else if (this._db && this._db.isOpen()) {
    this._db.get(name, callback)
  } else {
    callback(null)
  }
}

RecordCache.prototype.set = function (name, version, data) {
  if (this._filter && !this._filter(name, version, data)) {
    return
  }

  const entry = [version, data]
  this._lru.set(name, entry)
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
