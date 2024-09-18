import * as C from '../constants/constants.js'
import { h64ToString } from '../utils/utils.js'

class Observer {
  #name
  #key
  #listener
  #version = ''

  constructor(name, listener) {
    this.#name = name
    this.#key = h64ToString(name)
    this.#listener = listener
  }

  next(value) {
    let data
    if (value && typeof value === 'string') {
      if (value.charAt(0) !== '{' && value.charAt(0) !== '[') {
        throw new Error(`invalid value: ${value}`)
      }
      data = value
    } else if (value && typeof value === 'object') {
      data = JSON.stringify(value)
    } else if (data != null) {
      throw new Error(`invalid value: ${value}`)
    }

    const version = data ? `INF-${h64ToString(data)}` : ''
    if (this.#version === version) {
      return
    }

    if (version) {
      this.#listener._connection.sendMsg(this.#listener._topic, C.ACTIONS.UPDATE, [
        this.#key,
        version,
        data,
      ])
    } else {
      this.#listener._connection.sendMsg(this.#listener._topic, C.ACTIONS.LISTEN_REJECT, [
        this.#listener._pattern,
        this.#key,
      ])
    }

    this.#version = version
  }
  error(err) {
    this.#listener._error(this.#name, err)
    this.#listener._connection.sendMsg(this.#listener._topic, C.ACTIONS.LISTEN_REJECT, [
      this.#listener._pattern,
      this.#key,
    ])
  }
}

export default class Listener {
  constructor(topic, pattern, callback, handler, opts) {
    if (opts.recursive) {
      throw new Error('invalid argument: recursive')
    }
    if (opts.stringify) {
      throw new Error('invalid argument: stringify')
    }

    this._topic = topic
    this._pattern = pattern
    this._callback = callback
    this._handler = handler
    this._client = this._handler._client
    this._connection = this._handler._connection
    this._listening = false
    this._subscriptions = new Map()

    this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
  }

  get stats() {
    return {
      subscriptions: this._subscriptions.size,
    }
  }

  _$destroy() {
    this._reset()
  }

  _$onMessage(message) {
    const name = message.data[1]

    if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      if (this._subscriptions.has(name)) {
        this._error(name, 'invalid accept: listener exists')
        return
      }

      let value$
      try {
        value$ = this._callback(name)
      } catch (err) {
        this._error(name, err)
      }

      if (value$) {
        this._subscriptions.set(name, value$.subscribe(new Observer(name, this)))
      } else {
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
      }
    } else if (message.action === C.ACTIONS.LISTEN_REJECT) {
      const subscription = this._subscriptions.get(name)

      if (subscription) {
        this._subscriptions.delete(name)
        subscription.unsubscribe()
      } else {
        this._error(name, 'invalid remove: listener missing')
      }
    } else {
      return false
    }
    return true
  }

  _$onConnectionStateChange(connected) {
    if (connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
    } else {
      this._reset()
    }
  }

  _error(name, err) {
    this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err, [this._pattern, name])
  }

  _reset() {
    for (const subscription of this._subscriptions.values()) {
      subscription.unsubscribe()
    }
    this._subscriptions.clear()

    this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [this._pattern])
  }
}
