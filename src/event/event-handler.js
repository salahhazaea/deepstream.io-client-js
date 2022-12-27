const messageBuilder = require('../message/message-builder')
const messageParser = require('../message/message-parser')
const C = require('../constants/constants')
const MulticastListener = require('../utils/multicast-listener')
const UnicastListener = require('../utils/unicast-listener')
const EventEmitter = require('component-emitter2')
const rxjs = require('rxjs')

const EventHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._emitter = new EventEmitter()
  this._listeners = new Map()
  this._stats = {
    emitted: 0,
  }

  this.subscribe = this.subscribe.bind(this)
  this.unsubscribe = this.unsubscribe.bind(this)
  this.observe = this.observe.bind(this)
  this.provide = this.provide.bind(this)
  this.emit = this.emit.bind(this)

  this._connection.on(C.EVENT.CONNECTED, (connected) => {
    for (const listener of this._listeners.values()) {
      listener._$handleConnectionStateChange(connected)
    }

    if (connected) {
      for (const eventName of this._emitter.eventNames()) {
        this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.SUBSCRIBE, [eventName])
      }
    }
  })
}

Object.defineProperty(EventHandler.prototype, 'stats', {
  get: function stats() {
    return {
      ...this._stats,
      listeners: this._listeners.size,
      events: this._emitter.eventNames().length,
    }
  },
})

EventHandler.prototype.subscribe = function (name, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (!this._emitter.hasListeners(name)) {
    this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.SUBSCRIBE, [name])
  }

  this._emitter.on(name, callback)
}

EventHandler.prototype.unsubscribe = function (name, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }
  if (callback !== undefined && typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (!this._emitter.hasListeners(name)) {
    this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.UNSUBSCRIBE, [name])
  }

  this._emitter.off(name, callback)
}

EventHandler.on = function (name, callback) {
  this.subscribe(name, callback)
  return this
}

EventHandler.once = function (name, callback) {
  const fn = (...args) => {
    this.unsubscribe(fn)
    callback(...args) // eslint-disable-line
  }
  this.subscribe(name, fn)
  return this
}

EventHandler.off = function (name, callback) {
  this.unsubscribe(name, callback)
  return this
}

EventHandler.prototype.observe = function (name) {
  return new rxjs.Observable((o) => {
    const onValue = (value) => o.next(value)
    this.subscribe(name, onValue)
    return () => {
      this.unsubscribe(name, onValue)
    }
  })
}

EventHandler.prototype.emit = function (name, data) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }

  this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.EVENT, [name, messageBuilder.typed(data)])
  this._emitter.emit(name, data)
  this._stats.emitted += 1
}

EventHandler.prototype.provide = function (pattern, callback, options) {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new Error('invalid argument pattern')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (this._listeners.has(pattern)) {
    this._client._$onError(C.TOPIC.EVENT, C.EVENT.LISTENER_EXISTS, pattern)
    return
  }

  const listener =
    options.mode?.toLowerCase() === 'unicast'
      ? new UnicastListener(C.TOPIC.EVENT, pattern, callback, this, options)
      : new MulticastListener(C.TOPIC.EVENT, pattern, callback, this, options)

  this._listeners.set(pattern, listener)
  return () => {
    listener._$destroy()
    this._listeners.delete(pattern)
  }
}

EventHandler.prototype._$handle = function (message) {
  const [name, data] =
    message.action !== C.ACTIONS.ERROR
      ? message.data
      : message.data.slice(1).concat(message.data.slice(0, 1))

  if (message.action === C.ACTIONS.EVENT) {
    if (message.data && message.data.length === 2) {
      this._emitter.emit(name, messageParser.convertTyped(data, this._client))
    } else {
      this._emitter.emit(name)
    }
  } else {
    const listener = this._listeners.get(name)
    if (listener) {
      listener._$onMessage(message)
    }
  }
}

module.exports = EventHandler
