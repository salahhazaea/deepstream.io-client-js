const LRU = require('lru-cache')
const utils = require('../utils/utils')

const EMPTY = {}

function RecordStore (options, handler) {
  this._lru = options.lru || new LRU({ max: options.cacheSize || 512 })
  this._db = options.db
  this._handler = handler

  this._docs = []
  this._sync = null
}

RecordStore.prototype.get = function (name, callback) {
  const val = this._lru.get(name)
  if (val) {
    callback(null, val[0], val[1])
  } else if (this._db) {
    // TODO (perf): allDocs
    this._db.get(name, (err, doc) => {
      if (err) {
        callback(err)
      } else {
        const version = doc._rev
        if (doc._deleted) {
          doc = EMPTY
        } else {
          delete doc._id
          delete doc._rev
        }
        callback(null, doc, version)
      }
    })
  } else {
    callback(null, null)
  }
}

RecordStore.prototype.set = function (name, data, version) {
  if (this._db && /^[^I0]/.test(version)) {
    this._docs.push(Object.assign({ _id: name, _rev: version }, data))
    this._flush()
  }

  this._lru.set(name, [ data, version ])
}

RecordStore.prototype._flush = function () {
  if (this._sync || this._docs.length === 0) {
    return
  }

  const docs = this._docs.splice(0, 256)
  this._sync = this._handler
    .sync()
    .then(() => utils.schedule(() => {
      this._sync = null
      this._db
        .bulkDocs(docs, { new_edits: false }, err => {
          if (err) {
            console.error(err)
          }
          this._flush()
        })
    }))
}

module.exports = RecordStore
