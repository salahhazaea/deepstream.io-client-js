'use strict'

const C = require('../constants/constants')
const xuid = require('xuid')
const lz = require('@nxtedition/lz-string')

const Listener = function (topic, pattern, callback, options, client, connection, recordHandler) {
  this._topic = topic
  this._callback = callback
  this._pattern = pattern
  this._options = options
  this._client = client
  this._connection = connection
  this._recordHandler = recordHandler
  this._isListening = false

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._sendListen()
}

Listener.prototype._$destroy = function () {
  this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [ this._pattern ])
}

Listener.prototype.accept = function (name) {
  this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_ACCEPT, [ this._pattern, name ])
}

Listener.prototype.reject = function (name) {
  this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
}

Listener.prototype.set = function (name, context, value) {
  const raw = JSON.stringify(value)

  if (context.raw === raw) {
    return
  }

  context.raw = raw

  const version = `INF-${xuid()}`

  this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
    name,
    version,
    lz.compressToUTF16(raw)
  ])

  this._recordHandler._$handle({
    action: C.ACTIONS.UPDATE,
    data: [ name, version, value ]
  })
}

Listener.prototype._$onMessage = function (message) {
  if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
    this._callback(message.data[1], true, {
      accept: this.accept.bind(this, message.data[1]),
      reject: this.reject.bind(this, message.data[1]),
      set: message.topic === C.TOPIC.RECORD
        ? this.set.bind(this, message.data[1], Object.create(null))
        : undefined
    })
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
    this._callback(message.data[1], false)
  }
}

Listener.prototype._sendListen = function () {
  if (this._isListening || this._connection.getState() !== C.CONNECTION_STATE.OPEN) {
    return
  }
  this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [ this._pattern ])
  this._isListening = true
}

Listener.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    this._sendListen()
  } else if (state === C.CONNECTION_STATE.RECONNECTING) {
    this._isListening = false
  }
}

module.exports = Listener
