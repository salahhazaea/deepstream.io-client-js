import * as utils from '../utils/utils.js'
import { convertTyped } from './message-parser.js'
import * as messageBuilder from './message-builder.js'
import * as C from '../constants/constants.js'
import xxhash from 'xxhash-wasm'
import FixedQueue from '../utils/fixed-queue.js'
import Emitter from 'component-emitter2'
import varint from 'varint'

const BrowserWebSocket = globalThis.WebSocket || globalThis.MozWebSocket
const NodeWebSocket = utils.isNode ? await import('ws').then((x) => x.default) : null

const HASHER = await xxhash()

const Connection = function (client, url, options) {
  this._client = client
  this._options = options
  this._logger = options.logger
  this._schedule = options.schedule ?? utils.schedule
  this._batchSize = options.batchSize ?? 1024
  this._authParams = null
  this._authCallback = null
  this._deliberateClose = false
  this._tooManyAuthAttempts = false
  this._connectionAuthenticationTimeout = false
  this._challengeDenied = false
  this._decoder = new TextDecoder()
  this._encoder = new TextEncoder()
  this._recvQueue = new FixedQueue()
  this._reconnectTimeout = null
  this._reconnectionAttempt = 0

  this._processingRecv = false
  this._recvMessages = this._recvMessages.bind(this)

  this._url = new URL(url)

  this._state = C.CONNECTION_STATE.CLOSED

  this.hasher = HASHER

  this._createEndpoint()
}

Emitter(Connection.prototype)

// TODO (fix): Remove
Object.defineProperty(Connection.prototype, 'connected', {
  get: function connected() {
    return this._state === C.CONNECTION_STATE.OPEN
  },
})

Connection.prototype.getState = function () {
  return this._state
}

Connection.prototype.authenticate = function (authParams, callback) {
  this._authParams = authParams
  this._authCallback = callback

  if (this._tooManyAuthAttempts || this._challengeDenied || this._connectionAuthenticationTimeout) {
    const err = new Error("this client's connection was closed")
    this._client._$onError(C.TOPIC.ERROR, C.EVENT.IS_CLOSED, err)
    return
  } else if (this._deliberateClose === true && this._state === C.CONNECTION_STATE.CLOSED) {
    this._createEndpoint()
    this._deliberateClose = false
    return
  }

  if (this._state === C.CONNECTION_STATE.AWAITING_AUTHENTICATION) {
    this._sendAuthParams()
  }
}

Connection.prototype.sendMsg = function (topic, action, data) {
  return this.send(messageBuilder.getMsg(topic, action, data))
}

Connection.prototype.close = function () {
  this._deliberateClose = true
  this._endpoint?.close()

  if (this._reconnectTimeout) {
    clearTimeout(this._reconnectTimeout)
    this._reconnectTimeout = null
  }
}

Connection.prototype._createEndpoint = function () {
  if (utils.isNode) {
    this._endpoint = new NodeWebSocket(this._url, {
      generateMask() {},
    })
  } else {
    this._endpoint = new BrowserWebSocket(this._url)
    this._endpoint.binaryType = 'arraybuffer'
  }
  this._corked = false

  this._endpoint.onopen = this._onOpen.bind(this)
  this._endpoint.onerror = this._onError.bind(this)
  this._endpoint.onclose = this._onClose.bind(this)

  this._endpoint.onmessage = ({ data }) => {
    this._onMessage(data)
  }
}

Connection.prototype.send = function (message) {
  const { maxPacketSize } = this._options

  if (message.length > maxPacketSize) {
    const err = new Error(`Packet to big: ${message.length} > ${maxPacketSize}`)
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err, message)
    return false
  }

  if (
    this._state !== C.CONNECTION_STATE.OPEN ||
    this._endpoint.readyState !== this._endpoint.OPEN
  ) {
    return false
  }

  if (this._endpoint._socket && !this._corked) {
    this._endpoint._socket.cork()
    this._corked = true
    setTimeout(() => {
      this._endpoint._socket.uncork()
      this._corked = false
    }, 1)
  }

  this.emit('send', message)
  this._endpoint.send(message)

  return true
}

Connection.prototype._submit = function (message) {
  const { maxPacketSize } = this._options

  if (message.byteLength > maxPacketSize) {
    const err = new Error(`Packet to big: ${message.byteLength} > ${maxPacketSize}`)
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)
    return false
  } else if (this._endpoint.readyState === this._endpoint.OPEN) {
    this.emit('send', message)
    this._endpoint.send(message)
    return true
  } else {
    const err = new Error('Tried to send message on a closed websocket connection')
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)
    return false
  }
}

Connection.prototype._sendAuthParams = function () {
  this._setState(C.CONNECTION_STATE.AUTHENTICATING)
  const authMessage = messageBuilder.getMsg(C.TOPIC.AUTH, C.ACTIONS.REQUEST, [
    this._authParams,
    '25.0.0', // TODO (fix): How to read from package.json?
    utils.isNode
      ? `Node/${process.version}`
      : globalThis.navigator && globalThis.navigator.userAgent,
  ])
  this._submit(authMessage)
}

Connection.prototype._onOpen = function () {
  this._clearReconnect()
  this._setState(C.CONNECTION_STATE.AWAITING_CONNECTION)
}

Connection.prototype._onError = function (err) {
  this._recvMessages({ didTimeout: true, timeRemaining: () => 0 })

  if (err.error) {
    const { message, error } = err
    err = error
    err.message = message
  }

  if (!err.message) {
    err.message = 'socket error'
  }

  if (err.code === 'ECONNRESET' || err.code === 'ECONNREFUSED') {
    err.message = "Can't t! Deepstream server unreachable on " + this._url
  }

  this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)

  this._setState(C.CONNECTION_STATE.ERROR)
}

Connection.prototype._onClose = function () {
  this._recvMessages({ didTimeout: true, timeRemaining: () => 0 })

  if (this._deliberateClose === true) {
    this._setState(C.CONNECTION_STATE.CLOSED)
  } else {
    this._tryReconnect()
  }
}

Connection.prototype._onMessage = function (raw) {
  if (typeof raw === 'string') {
    raw = this._encoder.encode(raw)
  } else if (!utils.isNode) {
    raw = new Uint8Array(raw)
  }

  const len = raw.byteLength

  const start = 0

  let pos = start

  let headerSize = 0
  if (raw[pos] >= 128) {
    headerSize = raw[pos] - 128
    pos += headerSize
  }

  const topic = String.fromCharCode(raw[pos++])
  pos++

  let action = ''
  while (pos < len && raw[pos] !== 31) {
    action += String.fromCharCode(raw[pos++])
  }
  pos++

  // TODO (fix): Validate topic and action

  let data

  // TODO (fix): Don't stringify binary data...

  if (headerSize > 0) {
    data = []
    let headerPos = start + 1
    while (headerPos < headerSize) {
      const len = varint.decode(raw, headerPos)
      headerPos += varint.decode.bytes
      if (len === 0) {
        break
      }
      data.push(this._decoder.decode(raw.subarray(pos, pos + len - 1)))
      pos += len
    }
  } else {
    data =
      pos < len ? this._decoder.decode(raw.subarray(pos, len)).split(C.MESSAGE_PART_SEPERATOR) : []
  }

  this._recvQueue.push({ topic, action, data })
  if (!this._processingRecv) {
    this._processingRecv = true
    this._schedule(this._recvMessages)
  }
}

Connection.prototype._recvMessages = function (deadline) {
  for (
    let n = 0;
    // eslint-disable-next-line no-unmodified-loop-condition
    deadline ? deadline.didTimeout || deadline.timeRemaining() : n < this._batchSize;
    ++n
  ) {
    const message = this._recvQueue.shift()
    if (!message) {
      this._processingRecv = false
      return
    }

    if (message.length <= 2) {
      continue
    }

    if (message === C.TOPIC.ERROR) {
      this._client._$onError(C.TOPIC.ERROR, message.action, new Error('Message error'), message)
      continue
    }

    this.emit('recv', message)

    if (message.topic === C.TOPIC.CONNECTION) {
      this._handleConnectionResponse(message)
    } else if (message.topic === C.TOPIC.AUTH) {
      this._handleAuthResponse(message)
    } else {
      this._client._$onMessage(message)
    }
  }

  this._schedule(this._recvMessages)
}

Connection.prototype._handleConnectionResponse = function (message) {
  if (message.action === C.ACTIONS.PING) {
    this._submit(messageBuilder.getMsg(C.TOPIC.CONNECTION, C.ACTIONS.PONG))
  } else if (message.action === C.ACTIONS.ACK) {
    this._setState(C.CONNECTION_STATE.AWAITING_AUTHENTICATION)
    if (this._authParams) {
      this._sendAuthParams()
    }
  } else if (message.action === C.ACTIONS.CHALLENGE) {
    this._setState(C.CONNECTION_STATE.CHALLENGING)
    this._submit(
      messageBuilder.getMsg(C.TOPIC.CONNECTION, C.ACTIONS.CHALLENGE_RESPONSE, [this._url])
    )
  } else if (message.action === C.ACTIONS.REJECTION) {
    this._challengeDenied = true
    this.close()
  } else if (message.action === C.ACTIONS.ERROR) {
    if (message.data[0] === C.EVENT.CONNECTION_AUTHENTICATION_TIMEOUT) {
      this._deliberateClose = true
      this._connectionAuthenticationTimeout = true
      this._client._$onError(C.TOPIC.CONNECTION, message.data[0], message.data[1])
    }
  }
}

Connection.prototype._handleAuthResponse = function (message) {
  if (message.action === C.ACTIONS.ERROR) {
    if (message.data[0] === C.EVENT.TOO_MANY_AUTH_ATTEMPTS) {
      this._deliberateClose = true
      this._tooManyAuthAttempts = true
    } else {
      this._setState(C.CONNECTION_STATE.AWAITING_AUTHENTICATION)
    }

    if (this._authCallback) {
      this._authCallback(false, this._getAuthData(message.data[1]))
    }
  } else if (message.action === C.ACTIONS.ACK) {
    this._setState(C.CONNECTION_STATE.OPEN)

    if (this._authCallback) {
      this._authCallback(true, this._getAuthData(message.data[0]))
    }
  }
}

Connection.prototype._getAuthData = function (data) {
  if (data === undefined) {
    return null
  } else {
    return convertTyped(data, this._client)
  }
}

Connection.prototype._setState = function (state) {
  if (this._state === state) {
    return
  }
  this._state = state
  this.emit(C.EVENT.CONNECTION_STATE_CHANGED, state)
  this._client.emit(C.EVENT.CONNECTION_STATE_CHANGED, state)

  if (state === C.CONNECTION_STATE.OPEN) {
    this.emit(C.EVENT.CONNECTED, true)
    this._client.emit(C.EVENT.CONNECTED, true)
  } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
    this._recvQueue = new FixedQueue()
    this.emit(C.EVENT.CONNECTED, false)
    this._client.emit(C.EVENT.CONNECTED, false)
  }
}

Connection.prototype._tryReconnect = function () {
  if (this._reconnectTimeout) {
    return
  }

  if (this._reconnectionAttempt < this._options.maxReconnectAttempts) {
    this._setState(C.CONNECTION_STATE.RECONNECTING)
    this._reconnectTimeout = setTimeout(() => {
      this._reconnectTimeout = null
      this._createEndpoint()
    }, Math.min(this._options.maxReconnectInterval, this._options.reconnectIntervalIncrement * this._reconnectionAttempt))
    this._reconnectionAttempt++
  } else {
    this._clearReconnect()
    this.close()
    this._client.emit(C.EVENT.MAX_RECONNECTION_ATTEMPTS_REACHED, this._reconnectionAttempt)
  }
}

Connection.prototype._clearReconnect = function () {
  if (this._reconnectTimeout) {
    clearTimeout(this._reconnectTimeout)
    this._reconnectTimeout = null
  }
  this._reconnectionAttempt = 0
}

export default Connection
