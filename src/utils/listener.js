'use strict'

const C = require('../constants/constants')
const xuid = require('xuid')
const lz = require('@nxtedition/lz-string')

const Listener = function (topic, pattern, callback, options, client, connection, handler) {
  this._topic = topic
  this._callback = callback
  this._pattern = pattern
  this._options = options
  this._client = client
  this._connection = connection
  this._handler = handler
  this._isListening = false
  this._providers = new Map()

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._sendListen()
}

Listener.prototype._$destroy = function () {
  this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [ this._pattern ])
  this._reset()
}

Listener.prototype._$onMessage = function (message) {
  const [ , name ] = message.data
  const provider = this._providers.get(name)

  if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
    if (provider && provider.subscription) {
      provider.subscription.unsubscribe()
    }

    this._providers.set(name, {})

    Promise
      .resolve(this._callback(name))
      .then(value$ => {
        if (!value$) {
          return
        }
        const provider = this._providers.get(name)
        if (provider) {
          provider.value$ = value$
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_ACCEPT, [ this._pattern, name ])
        }
      })
      .catch(err => {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, [ this._pattern, err.message || err ])
      })
  } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
    if (!provider || !provider.value$) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
      return
    }
    provider.subscription = provider.value$.subscribe({
      next: value => {
        if (this._topic === C.TOPIC.EVENT) {
          this._handler.emit(name, value)
        } else if (this._topic === C.TOPIC.RECORD) {
          const raw = JSON.stringify(value)

          if (provider.raw === raw) {
            return
          }

          provider.raw = raw

          const version = `INF-${xuid()}`

          this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
            name,
            version,
            lz.compressToUTF16(raw)
          ])

          this._handler._$handle({
            action: C.ACTIONS.UPDATE,
            data: [ name, version, value ]
          })
        }
      },
      error: err => {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, [ this._pattern, err.message || err ])
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
      }
    })
  } else if (
    message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED ||
    message.action === C.ACTIONS.LISTEN_REJECT
  ) {
    if (provider && provider.subscription) {
      provider.subscription.unsubscribe()
    }
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
    this._reset()
  }
}

Listener.prototype._reset = function () {
  for (const provider of this._providers) {
    if (provider.subscription) {
      provider.subscription.unsubscribe()
    }
  }
  this._providers.clear()
}

module.exports = Listener
