'use strict'

const EventEmitter = require('component-emitter2')
const C = require('../constants/constants')

const PresenceHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._emitter = new EventEmitter()
  this._isSubscribed = true

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)
}

PresenceHandler.prototype.getAll = function (callback) {
  if (!this._emitter.hasListeners(C.ACTIONS.QUERY)) {
    this._connection.sendMsg(C.TOPIC.PRESENCE, C.ACTIONS.QUERY, [ C.ACTIONS.QUERY ])
  }
  this._emitter.once(C.ACTIONS.QUERY, callback)
}

PresenceHandler.prototype.subscribe = function (callback) {
  if (callback !== undefined && typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (!this._emitter.hasListeners(C.TOPIC.PRESENCE)) {
    this._connection.sendMsg(C.TOPIC.PRESENCE, C.ACTIONS.SUBSCRIBE, [C.ACTIONS.SUBSCRIBE])
  }

  this._emitter.on(C.TOPIC.PRESENCE, callback)
}

PresenceHandler.prototype.unsubscribe = function (callback) {
  if (callback !== undefined && typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  this._emitter.off(C.TOPIC.PRESENCE, callback)

  if (!this._emitter.hasListeners(C.TOPIC.PRESENCE)) {
    this._connection.sendMsg(C.TOPIC.PRESENCE, C.ACTIONS.UNSUBSCRIBE, [ C.ACTIONS.UNSUBSCRIBE ])
  }
}

PresenceHandler.prototype._$handle = function (message) {
  if (message.action === C.ACTIONS.ERROR && message.data[0] === C.EVENT.MESSAGE_DENIED) {
    message.processedError = true
    this._client._$onError(C.TOPIC.PRESENCE, C.EVENT.MESSAGE_DENIED, message.data[1])
  } else if (message.action === C.ACTIONS.PRESENCE_JOIN) {
    this._emitter.emit(C.TOPIC.PRESENCE, message.data[0], true)
  } else if (message.action === C.ACTIONS.PRESENCE_LEAVE) {
    this._emitter.emit(C.TOPIC.PRESENCE, message.data[0], false)
  } else if (message.action === C.ACTIONS.QUERY) {
    this._emitter.emit(C.ACTIONS.QUERY, message.data)
  } else {
    this._client._$onError(C.TOPIC.PRESENCE, C.EVENT.UNSOLICITED_MESSAGE, message.action)
  }
}

PresenceHandler.prototype._sendSubscribe = function () {
  if (this._isSubscribed || this._connection.getState() !== C.CONNECTION_STATE.OPEN) {
    return
  }
  if (this._emitter.hasListeners(C.TOPIC.PRESENCE)) {
    this._connection.sendMsg(C.TOPIC.PRESENCE, C.ACTIONS.SUBSCRIBE, [ C.ACTIONS.SUBSCRIBE ])
  }
}

PresenceHandler.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    this._sendSubscribe()
  } else if (state === C.CONNECTION_STATE.RECONNECTING) {
    this._isSubscribed = false
  }
}

module.exports = PresenceHandler
