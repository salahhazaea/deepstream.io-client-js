const C = require('../constants/constants')
const messageBuilder = require('../message/message-builder')

const RpcResponse = function (connection, name, id) {
  this._connection = connection
  this._name = name
  this._id = id
  this.completed = false
}

RpcResponse.prototype.reject = function () {
  if (this.completed) {
    throw new Error(`Rpc ${this._name} already completed`)
  }
  this.completed = true

  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.REJECTION, [this._name, this._id])
}

RpcResponse.prototype.error = function (error) {
  if (this.completed) {
    throw new Error(`Rpc ${this._name} already completed`)
  }
  this.completed = true

  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.RESPONSE, [
    this._name,
    this._id,
    error.message || error,
    true,
  ])
}

RpcResponse.prototype.send = function (data) {
  if (this.completed) {
    throw new Error(`Rpc ${this._name} already completed`)
  }
  this.completed = true

  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.RESPONSE, [
    this._name,
    this._id,
    messageBuilder.typed(data),
  ])
}

module.exports = RpcResponse
