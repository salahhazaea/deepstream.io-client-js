import * as C from '../constants/constants.js'
import RpcResponse from './rpc-response.js'
import messageParser from '../message/message-parser.js'
import * as messageBuilder from '../message/message-builder.js'
import xuid from 'xuid'

const RpcHandler = function (options, connection, client) {
  this._options = options
  this._connection = connection
  this._client = client
  this._rpcs = new Map()
  this._providers = new Map()
  this._stats = {}

  this.provide = this.provide.bind(this)
  this.unprovide = this.unprovide.bind(this)
  this.make = this.make.bind(this)

  this._client.on(C.EVENT.CONNECTED, this._onConnectionStateChange.bind(this))
}

Object.defineProperty(RpcHandler.prototype, 'connected', {
  get: function connected() {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  },
})

Object.defineProperty(RpcHandler.prototype, 'stats', {
  get: function stats() {
    return {
      ...this._stats,
      listeners: this._providers.size,
      rpcs: this._rpcs.size,
    }
  },
})

RpcHandler.prototype.provide = function (name, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }
  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  if (this._providers.has(name)) {
    this._client._$onError(C.TOPIC.RPC, C.EVENT.PROVIDER_EXISTS, name)
    return
  }

  this._providers.set(name, callback)

  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.SUBSCRIBE, [name])
  }

  return () => this.unprovide(name)
}

RpcHandler.prototype.unprovide = function (name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }

  if (!this._providers.has(name)) {
    this._client._$onError(C.TOPIC.RPC, C.EVENT.NOT_PROVIDING, name)
    return
  }

  this._providers.delete(name)

  if (this.connected) {
    this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.UNSUBSCRIBE, [name])
  }
}

RpcHandler.prototype.make = function (name, data, callback) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('invalid argument name')
  }

  let promise
  if (callback === undefined) {
    promise = new Promise((resolve, reject) => {
      callback = (err, val) => (err ? reject(err) : resolve(val))
    })
  }

  if (typeof callback !== 'function') {
    throw new Error('invalid argument callback')
  }

  const id = xuid()
  this._rpcs.set(id, {
    id,
    name,
    data,
    callback,
  })
  this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.REQUEST, [name, id, messageBuilder.typed(data)])

  return promise
}

RpcHandler.prototype._respond = function (message) {
  const [name, id, data] = message.data

  const callback = this._providers.get(name)
  const response = new RpcResponse(this._connection, name, id)

  if (callback) {
    let promise
    try {
      promise = Promise.resolve(callback(messageParser.convertTyped(data, this._client), response))
    } catch (err) {
      promise = Promise.reject(err)
    }

    if (!response.completed) {
      promise
        .then((val) => {
          if (!response.completed) {
            response.send(val)
          }
        })
        .catch((err) => {
          if (!response.completed) {
            response.error(err)
          }
        })
    }
  } else {
    response.reject()
  }
}

RpcHandler.prototype._$handle = function (message) {
  if (message.action === C.ACTIONS.REQUEST) {
    this._respond(message)
  } else if (message.action === C.ACTIONS.RESPONSE) {
    const [, id, data, error] = message.data

    const rpc = this._rpcs.get(id)
    if (!rpc) {
      return
    }

    this._rpcs.delete(id)

    if (error) {
      rpc.callback(
        Object.assign(new Error(data), {
          rpcId: rpc.id,
          rpcName: rpc.name,
          rpcData: rpc.data,
        }),
      )
    } else {
      rpc.callback(null, messageParser.convertTyped(data, this._client))
    }
  }
}

RpcHandler.prototype._onConnectionStateChange = function (connected) {
  if (connected) {
    for (const name of this._providers.keys()) {
      this._connection.sendMsg(C.TOPIC.RPC, C.ACTIONS.SUBSCRIBE, [name])
    }
  } else {
    const err = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' })
    for (const rpc of this._rpcs.values()) {
      rpc.callback(err)
    }
    this._rpcs.clear()
  }
}

export default RpcHandler
