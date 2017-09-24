'use strict'

const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const Rx = require('rxjs')
const utils = require('../utils/utils')
const LRU = require('lru-cache')

const RecordHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._recordsMap = new Map()
  this._recordsVec = []
  this._listeners = new Map()
  this._cache = new LRU({ max: 2e3 })

  this._prune = this._prune.bind(this)
  this._prune()
}

RecordHandler.prototype._prune = function () {
  utils.requestIdleCallback(() => {
    let vec = this._recordsVec
    let map = this._recordsMap

    let i = 0
    let j = vec.length
    while (i < j) {
      if (vec[i].usages === 0 && vec[i].isReady) {
        if (map.delete(vec[i].name)) {
          vec[i]._$destroy()
        }
        vec[i] = vec[--j]
      } else {
        ++i
      }
    }
    vec.splice(i)

    setTimeout(this._prune, 2000)
  })
}

RecordHandler.prototype.getRecord = function (name) {
  let record = this._recordsMap.get(name)

  if (!record) {
    record = new Record(name, this._connection, this._client, this._cache)
    this._recordsMap.set(name, record)
    this._recordsVec.push(record)
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

RecordHandler.prototype.get = function (name, pathOrNil) {
  const record = this.getRecord(name)
  return record
    .whenReady()
    .then(() => record.get(pathOrNil))
    .then(val => {
      record.discard()
      return val
    })
    .catch(err => {
      record.discard()
      throw err
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

RecordHandler.prototype.merge = function (name, data) {
  const record = this.getRecord(name)
  const promise = record.merge(data)
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

RecordHandler.prototype.observe = function (name, waitForProvider) {
  let value$ = Rx.Observable
    .create(o => {
      const record = this.getRecord(name)
      const onValue = value => o.next(value)
      record.subscribe(onValue, true)
      return () => {
        record.unsubscribe(onValue)
        record.discard()
      }
    })

  if (waitForProvider) {
    return value$.combineLatest(this.hasProvider(name).first(x => x), value => value)
  }

  return value$
}

RecordHandler.prototype.hasProvider = function (name) {
  return Rx.Observable
    .create(o => {
      const record = this.getRecord(name)
      const onValue = value => o.next(value)
      record.on('hasProviderChanged', onValue)
      onValue(record.hasProvider)
      return () => {
        record.off('hasProviderChanged', onValue)
        record.discard()
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

  const record = this._recordsMap.get(name)
  if (record) {
    record._$onMessage(message)
  }

  const listener = this._listeners.get(name)
  if (listener) {
    listener._$onMessage(message)
  }
}

module.exports = RecordHandler
