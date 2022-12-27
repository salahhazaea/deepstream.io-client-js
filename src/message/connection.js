const BrowserWebSocket = globalThis.WebSocket || globalThis.MozWebSocket
const NodeWebSocket = BrowserWebSocket ? null : require('ws')
const messageParser = require('./message-parser')
const messageBuilder = require('./message-builder')
const utils = require('../utils/utils')
const C = require('../constants/constants')
const pkg = require('../../package.json')
const xxhash = require('xxhash-wasm')
const FixedQueue = require('../utils/fixed-queue')
const Emitter = require('component-emitter2')

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
  this._sendQueue = new FixedQueue()
  this._recvQueue = new FixedQueue()
  this._message = {
    raw: null,
    topic: null,
    action: null,
    data: null,
  }
  this._reconnectTimeout = null
  this._reconnectionAttempt = 0
  this._endpoint = null
  this._lastHeartBeat = null
  this._heartbeatInterval = null

  this._processingRecv = false
  this._recvMessages = this._recvMessages.bind(this)
  this._processingSend = false
  this._sendMessages = this._sendMessages.bind(this)
  this._state = C.CONNECTION_STATE.CLOSED
  this._url = new URL(url)
  this._connected = 0

  this.hasher = null
  xxhash().then((hasher) => {
    this.hasher = hasher
    this._createEndpoint()
  })
}

Emitter(Connection.prototype)

Object.defineProperty(Connection.prototype, 'connected', {
  get: function connected() {
    return this._connected
  },
})

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
  this.send(messageBuilder.getMsg(topic, action, data))
}

Connection.prototype.sendMsg1 = function (topic, action, p0) {
  this.send(messageBuilder.getMsg1(topic, action, p0))
}

Connection.prototype.sendMsg2 = function (topic, action, p0, p1) {
  this.send(messageBuilder.getMsg2(topic, action, p0, p1))
}

Connection.prototype.close = function () {
  while (!this._sendQueue.isEmpty) {
    this._submit(this._sendQueue.shift())
  }
  this._reset()
  this._deliberateClose = true
  this._endpoint?.close()

  if (this._reconnectTimeout) {
    clearTimeout(this._reconnectTimeout)
    this._reconnectTimeout = null
  }
}

Connection.prototype._createEndpoint = function () {
  this._endpoint = BrowserWebSocket
    ? new BrowserWebSocket(this._url)
    : new NodeWebSocket(this._url, {
        generateMask() {},
      })

  this._endpoint.onopen = this._onOpen.bind(this)
  this._endpoint.onerror = this._onError.bind(this)
  this._endpoint.onclose = this._onClose.bind(this)
  this._endpoint.onmessage = BrowserWebSocket
    ? ({ data }) => this._onMessage(typeof data === 'string' ? data : Buffer.from(data).toString())
    : ({ data }) => this._onMessage(typeof data === 'string' ? data : data.toString())
}

Connection.prototype.send = function (message) {
  const { maxPacketSize } = this._options

  if (message.length > maxPacketSize) {
    const err = new Error(`Packet to big: ${message.length} > ${maxPacketSize}`)
    this._client._$onError(
      C.TOPIC.CONNECTION,
      C.EVENT.CONNECTION_ERROR,
      err,
      message.split(C.MESSAGE_PART_SEPERATOR).map((x) => x.slice(0, 256))
    )
    return
  }

  if (this._state !== C.CONNECTION_STATE.OPEN) {
    return
  }

  this._sendQueue.push(message)
  if (!this._processingSend) {
    this._processingSend = true
    this._schedule(this._sendMessages)
  }
}

Connection.prototype._sendMessages = function (deadline) {
  if (
    this._state !== C.CONNECTION_STATE.OPEN ||
    this._endpoint.readyState !== this._endpoint.OPEN
  ) {
    this._processingSend = false
    return
  }

  for (
    let n = 0;
    // eslint-disable-next-line no-unmodified-loop-condition
    n < this._batchSize && (!deadline || deadline.timeRemaining() || deadline.didTimeout);
    ++n
  ) {
    const message = this._sendQueue.shift()
    if (!message) {
      this._processingSend = false
      return
    }

    this._submit(message)
  }

  this._schedule(this._sendMessages)
}

Connection.prototype._submit = function (message) {
  const { maxPacketSize } = this._options

  if (message.length > maxPacketSize) {
    const err = new Error(`Packet to big: ${message.length} > ${maxPacketSize}`)
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)
  } else if (this._endpoint.readyState === this._endpoint.OPEN) {
    this.emit('send', message)
    this._endpoint.send(message)
  } else {
    const err = new Error('Tried to send message on a closed websocket connection')
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)
  }
}

Connection.prototype._sendAuthParams = function () {
  this._setState(C.CONNECTION_STATE.AUTHENTICATING)
  const authMessage = messageBuilder.getMsg(C.TOPIC.AUTH, C.ACTIONS.REQUEST, [
    this._authParams,
    pkg.version,
    utils.isNode
      ? `Node/${process.version}`
      : globalThis.navigator && globalThis.navigator.userAgent,
  ])
  this._submit(authMessage)
}

Connection.prototype._checkHeartBeat = function () {
  const heartBeatTolerance = this._options.heartbeatInterval * 3

  if (Date.now() - this._lastHeartBeat > heartBeatTolerance) {
    clearInterval(this._heartbeatInterval)
    this._endpoint.close()
    const err = new Error(`heartbeat not received in the last ${heartBeatTolerance} milliseconds`)
    this._client._$onError(C.TOPIC.CONNECTION, C.EVENT.CONNECTION_ERROR, err)
  } else {
    this._submit(messageBuilder.getMsg(C.TOPIC.CONNECTION, C.ACTIONS.PING))
  }
}

Connection.prototype._onOpen = function () {
  this._clearReconnect()
  this._lastHeartBeat = Date.now()
  this._heartbeatInterval = setInterval(
    this._checkHeartBeat.bind(this),
    this._options.heartbeatInterval
  )
  this._heartbeatInterval.unref?.()
  this._setState(C.CONNECTION_STATE.AWAITING_CONNECTION)
}

Connection.prototype._onError = function (err) {
  this._reset()

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
  this._reset()

  if (this._deliberateClose === true) {
    this._setState(C.CONNECTION_STATE.CLOSED)
  } else {
    this._tryReconnect()
  }
}

Connection.prototype._onMessage = function (data) {
  // Remove MESSAGE_SEPERATOR if exists.
  if (data.charCodeAt(data.length - 1) === 30) {
    data = data.slice(0, -1)
  }

  this._recvQueue.push(data)
  if (!this._processingRecv) {
    this._processingRecv = true
    this._schedule(this._recvMessages)
  }
}

Connection.prototype._recvMessages = function (deadline) {
  for (
    let n = 0;
    // eslint-disable-next-line no-unmodified-loop-condition
    n < this._batchSize && (!deadline || deadline.timeRemaining() || deadline.didTimeout);
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

    if (this._logger) {
      this._logger.trace(message, 'receive')
    }

    messageParser.parseMessage(message, this._client, this._message)

    this.emit('recv', this._message)

    if (this._message.topic === C.TOPIC.CONNECTION) {
      this._handleConnectionResponse(this._message)
    } else if (this._message.topic === C.TOPIC.AUTH) {
      this._handleAuthResponse(this._message)
    } else {
      this._client._$onMessage(this._message)
    }
  }

  this._schedule(this._recvMessages)
}

Connection.prototype._reset = function () {
  clearInterval(this._heartbeatInterval)
  this._heartbeatInterval = null
  this._lastHeartBeat = null

  this._recvQueue = new FixedQueue()
  this._sendQueue = new FixedQueue()
}

Connection.prototype._handleConnectionResponse = function (message) {
  if (message.action === C.ACTIONS.PING) {
    this._lastHeartBeat = Date.now()
    this._submit(messageBuilder.getMsg(C.TOPIC.CONNECTION, C.ACTIONS.PONG))
  } else if (message.action === C.ACTIONS.PONG) {
    this._lastHeartBeat = Date.now()
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

    this._sendMessages()
  }
}

Connection.prototype._getAuthData = function (data) {
  if (data === undefined) {
    return null
  } else {
    return messageParser.convertTyped(data, this._client)
  }
}

Connection.prototype._setState = function (state) {
  if (this._state === state) {
    return
  }
  this._state = state
  this._client.emit(C.EVENT.CONNECTION_STATE_CHANGED, state)

  if (state === C.CONNECTION_STATE.OPEN) {
    this._connected = Date.now()
    this._client.emit(C.EVENT.CONNECTED, this._connected)
  } else if (state === C.CONNECTION_STATE.RECONNECTING || state === C.CONNECTION_STATE.CLOSED) {
    this._connected = 0
    this._client.emit(C.EVENT.CONNECTED, this._connected)
  }
}

Connection.prototype._tryReconnect = function () {
  if (this._reconnectTimeout) {
    return
  }

  if (this._reconnectionAttempt < this._options.maxReconnectAttempts) {
    this._setState(C.CONNECTION_STATE.RECONNECTING)
    this._reconnectTimeout = setTimeout(
      this._tryOpen.bind(this),
      Math.min(
        this._options.maxReconnectInterval,
        this._options.reconnectIntervalIncrement * this._reconnectionAttempt
      )
    )
    this._reconnectionAttempt++
  } else {
    this._clearReconnect()
    this.close()
    this._client.emit(C.EVENT.MAX_RECONNECTION_ATTEMPTS_REACHED, this._reconnectionAttempt)
  }
}

Connection.prototype._tryOpen = function () {
  this._createEndpoint()
  this._reconnectTimeout = null
}

Connection.prototype._clearReconnect = function () {
  clearTimeout(this._reconnectTimeout)
  this._reconnectTimeout = null
  this._reconnectionAttempt = 0
}

module.exports = Connection
