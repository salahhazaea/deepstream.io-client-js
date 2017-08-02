'use strict'

const C = require('../constants/constants')
const RpcResponse = require('./rpc-response')
const messageParser = require('../message/message-parser')
const messageBuilder = require('../message/message-builder')

const RpcHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._rpcs = new Map()
  this._providers = new Map()
  this._isProviding = true

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)
}

RpcHandler.prototype.provide = function (name, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (this._providers.has(name)) {
    throw new Error(`RPC ${name} already registered`)
  }

  this._providers.set(name, callback)
  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.SUBSCRIBE, [ name ])
}

RpcHandler.prototype.unprovide = function (name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }

  if (!this._providers.has(name)) {
    throw new Error(`RPC ${name} is not registered`)
  }

  this._providers.delete(name)
  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.UNSUBSCRIBE, [ name ])
}

RpcHandler.prototype.make = function (name, data, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  const id = this._client.getUid()
  this._rpcs.set(id, {
    id,
    name,
    callback
  })
  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.REQUEST, [ name, id, messageBuilder.typed(data) ])
}

RpcHandler.prototype._respond = function (message) {
  const [ name, id, data ] = message.data

  const callback = this._providers.get(name)
  const response = new RpcResponse(this._connection, name, id)

  if (callback) {
    callback(messageParser.convertTyped(data, this._client), response)
  } else {
    response.reject()
  }
}

RpcHandler.prototype._$handle = function (message) {
  const [ , id, data ] = message.data

  if (message.action === C.ACTIONS.REQUEST) {
    this._respond(message)
    return
  }

  if (message.action === C.ACTIONS.ERROR) {
    if (message.data[0] === C.EVENT.MESSAGE_PERMISSION_ERROR) {
      return
    }
    if (message.data[0] === C.EVENT.MESSAGE_DENIED && message.data[2] === C.ACTIONS.SUBSCRIBE) {
      return
    }
  }

  const rpc = this._rpcs.get(id)
  if (!rpc) {
    return
  }
  this._rpcs.delete(id)

  if (message.action === C.ACTIONS.RESPONSE) {
    rpc.callback(null, messageParser.convertTyped(data, this._client))
  } else if (message.action === C.ACTIONS.ERROR) {
    rpc.callback(data)
  }
}

RpcHandler.prototype._sendProviding = function () {
  if (this._isProviding || this._connection.getState() !== C.CONNECTION_STATE.OPEN) {
    return
  }
  for (const name of this._providers.keys()) {
    this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.SUBSCRIBE, [ name ])
  }
}

RpcHandler.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    this._sendProviding()
  } else if (state === C.CONNECTION_STATE.RECONNECTING) {
    this._isProviding = false
  }
}

module.exports = RpcHandler
