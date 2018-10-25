const LRU = require('lru-cache')
const utils = require('../utils/utils')

function RecordStore (options, handler) {
  this._lru = options.lru || new LRU({ max: options.cacheSize || 512 })
  this._db = options.db
  this._handler = handler

  this._write = []
  this._writing = null

  this._read = []
  this._reading = null
}

RecordStore.prototype.get = function (name, callback) {
  const entry = this._lru.get(name)
  if (entry) {
    const [ data, version ] = entry
    callback(null, data, version)
  } else if (this._db) {
    this._read.push({ name, callback })
    this._flushRead()
  } else {
    callback(null)
  }
}

RecordStore.prototype.set = function (name, data, version) {
  if (!version || !data) {
    return
  }

  if (this._db && /^[^I0]/.test(version)) {
    this._write.push(Object.assign({ _id: name, _rev: version }, data))
    this._flushWrite()
  }

  this._lru.set(name, [ data, version ])
}

RecordStore.prototype._flushRead = function () {
  if (this._reading || this._read.length === 0) {
    return
  }

  this._reading = setTimeout(() => {
    const read = this._read.splice(0, 256)
    this._readSync = this._db
      .allDocs({
        keys: read.map(({ name }) => name),
        include_docs: true
      })
      .then(({ rows }) => {
        for (let n = 0; n < read.length; ++n) {
          const { doc } = rows[n]
          const { callback } = read[n]

          rows[n] = null
          read[n] = null

          if (doc && !doc._deleted) {
            const version = doc._rev
            delete doc._id
            delete doc._rev
            callback(null, doc, version)
          } else {
            callback(null)
          }
        }
      })
      .catch(err => {
        // TODO
        console.error(err)

        for (let n = 0; n < read.length; ++n) {
          const { callback } = read[n]
          if (callback) {
            callback(err)
          }
        }
      })
      .then(() => {
        this._reading = null
        this._flushRead()
      })
  }, 1)
}

RecordStore.prototype._flushWrite = function () {
  if (this._writing || this._write.length === 0) {
    return
  }

  this._writing = setTimeout(() => {
    const docs = this._write.splice(0, 256)
    this._handler
      .sync()
      .then(() => utils.schedule(() => this._db
        .bulkDocs(docs, { new_edits: false })
        .catch(err => {
          // TODO
          console.error(err)
        })
        .then(() => {
          this._writing = null
          this._flushWrite()
        })
      ))
  }, 1)
}

module.exports = RecordStore
