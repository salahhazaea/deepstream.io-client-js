const messageBuilder = require('../message/message-builder')
const messageParser = require('../message/message-parser')
const C = require('../constants/constants')
const Listener = require('../utils/listener')
const EventEmitter = require('component-emitter2')
const { Observable } = require('rxjs')

const EventHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._emitter = new EventEmitter()
  this._listeners = new Map()
  this._stats = {
    emitted: 0,
  }

  this._client.on('connectionStateChanged', (state) => {
    if (state === C.CONNECTION_STATE.OPEN) {
      this._handleConnectionStateChange(true)
    } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
      this._handleConnectionStateChange(false)
    }
  })
}

Object.defineProperty(EventHandler.prototype, 'connected', {
  get: function connected() {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  },
})

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

  if (!this._emitter.hasListeners(name) && this.connected) {
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

  this._emitter.off(name, callback)

  if (!this._emitter.hasListeners(name) && this.connected) {
    this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.UNSUBSCRIBE, [name])
  }
}

EventHandler.prototype.observe = function (name) {
  return Observable.create((o) => {
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

EventHandler.prototype.provide = function (pattern, callback) {
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

  const listener = new Listener(
    C.TOPIC.EVENT,
    pattern,
    callback,
    this._options,
    this._client,
    this._connection,
    this
  )

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

EventHandler.prototype._handleConnectionStateChange = function (connected) {
  if (connected) {
    for (const eventName of this._emitter.eventNames()) {
      this._connection.sendMsg(C.TOPIC.EVENT, C.ACTIONS.SUBSCRIBE, [eventName])
    }
  }
}

module.exports = EventHandler
