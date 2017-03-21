'use strict'

const Record = require('./record')
const Listener = require('../utils/listener')
const C = require('../constants/constants')
const Rx = require('rxjs')
const utils = require('../utils/utils')

const RecordHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._recordsMap = new Map()
  this._recordsVec = []
  this._listeners = new Map()

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

    setTimeout(this._prune, 5000)
  })
}

RecordHandler.prototype.getRecord = function (recordName) {
  let record = this._recordsMap.get(recordName)

  if (!record) {
    record = new Record(recordName, this._connection, this._client)
    this._recordsMap.set(recordName, record)
    this._recordsVec.push(record)
  }

  record.usages += 1

  return record
}

RecordHandler.prototype.listen = function (pattern, callback) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  const listener = this._listeners.get(pattern)

  if (listener && !listener.destroyPending) {
    return this._client._$onError(C.TOPIC.RECORD, C.EVENT.LISTENER_EXISTS, pattern)
  }

  if (listener) {
    listener.destroy()
  }

  this._listeners.set(pattern, new Listener(C.TOPIC.RECORD, pattern, callback, this._options, this._client, this._connection, this))
}

RecordHandler.prototype.unlisten = function (pattern) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }

  const listener = this._listeners.get(pattern)
  if (listener && !listener.destroyPending) {
    listener.sendDestroy()
  } else if (listener) {
    listener.destroy()
    this._listeners.delete(pattern)
  } else {
    this._client._$onError(C.TOPIC.RECORD, C.EVENT.NOT_LISTENING, pattern)
  }
}

RecordHandler.prototype.get = function (recordName, pathOrNil) {
  const record = this.getRecord(recordName)
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

RecordHandler.prototype.set = function (recordName, pathOrData, dataOrNil) {
  const record = this.getRecord(recordName)
  const promise = arguments.length === 2
    ? record.set(pathOrData)
    : record.set(pathOrData, dataOrNil)
  record.discard()

  return promise
}

RecordHandler.prototype.update = function (recordName, pathOrUpdater, updaterOrNil) {
  const path = arguments.length === 2 ? undefined : pathOrUpdater
  const updater = arguments.length === 2 ? pathOrUpdater : updaterOrNil

  const record = this.getRecord(recordName)
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

RecordHandler.prototype.observe = function (recordName) {
  return Rx.Observable
    .create(o => {
      const record = this.getRecord(recordName)
      const onValue = value => o.next(value)
      record.subscribe(onValue, true)
      return () => {
        record.unsubscribe(onValue)
        record.discard()
      }
    })
}

RecordHandler.prototype.provide = function (pattern, provider) {
  const subscriptions = new Map()
  const callback = (match, isSubscribed, response) => {
    if (isSubscribed) {
      const onNext = value => {
        if (value) {
          response.set(value)
        } else {
          response.reject()
        }
      }
      const onError = err => {
        this._client._$onError(C.TOPIC.RECORD, pattern, err.message)
        response.reject()
      }
      try {
        Promise
          .resolve(provider(match))
          .then(data$ => {
            if (!data$) {
              response.reject()
            } else {
              response.accept()
              subscriptions.set(match, data$.subscribe(onNext, onError))
            }
          })
          .catch(onError)
      } catch (err) {
        onError(err)
      }
    } else {
      const subscription = subscriptions.get(match)
      subscription && subscription.unsubscribe()
      subscriptions.delete(match)
    }
  }
  this.listen(pattern, callback)
  return () => this.unlisten(pattern, callback)
}

RecordHandler.prototype._$handle = function (message) {
  if (message.action === C.ACTIONS.ERROR && message.data[0] !== C.EVENT.MESSAGE_DENIED) {
    message.processedError = true
    this._client._$onError(C.TOPIC.RECORD, message.data[0], message.data[1])
    return
  }

  let recordName
  if (message.action === C.ACTIONS.ACK || message.action === C.ACTIONS.ERROR) {
    recordName = message.data[1]
  } else {
    recordName = message.data[0]
  }

  const record = this._recordsMap.get(recordName)
  if (record) {
    record._$onMessage(message)
  }

  const listener = this._listeners.get(recordName)
  if (listener) {
    if (message.action === C.ACTIONS.ACK && message.data[0] === C.ACTIONS.UNLISTEN && listener.destroyPending) {
      listener.destroy()
      this._listeners.delete(recordName)
    } else {
      listener._$onMessage(message)
    }
  }
}

module.exports = RecordHandler
