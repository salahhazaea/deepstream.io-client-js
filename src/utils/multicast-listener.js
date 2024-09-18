import * as rxjs from 'rxjs'
import * as C from '../constants/constants.js'
import { h64ToString } from '../utils/utils.js'
import * as timers from '../utils/timers.js'

class Provider {
  #sending = false
  #accepted = false
  #value$ = null
  #name
  #version
  #timeout
  #valueSubscription
  #patternSubscription
  #observer
  #listener

  constructor(name, listener) {
    this.#name = name
    this.#listener = listener
    this.#observer = {
      next: (value) => {
        if (value == null) {
          this.next(null) // TODO (fix): This is weird...
          return
        }

        if (this.#listener._topic === C.TOPIC.EVENT) {
          this.#listener._handler.emit(this.#name, value)
        } else if (this.#listener._topic === C.TOPIC.RECORD) {
          if (typeof value !== 'object' && typeof value !== 'string') {
            this.#listener._error(this.#name, 'invalid value')
            return
          }

          const body = typeof value !== 'string' ? this.#listener._stringify(value) : value
          const hash = h64ToString(body)
          const version = `INF-${hash}`

          if (this.#version !== version) {
            this.#version = version
            this.#listener._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
              this.#name,
              version,
              body,
            ])
          }
        }
      },
      error: (err) => {
        this.error(err)
      },
    }

    this.#start()
  }

  dispose() {
    this.#stop()
  }

  accept() {
    if (!this.#value$) {
      return
    }

    if (this.#valueSubscription) {
      this.#listener._error(this.#name, 'invalid accept: listener started')
    } else {
      // TODO (fix): provider.version = message.data[2]
      this.#valueSubscription = this.#value$.subscribe(this.#observer)
    }
  }

  reject() {
    this.#listener._error(this.#name, 'invalid reject: not implemented')
  }

  next(value$) {
    if (!value$) {
      value$ = null
    } else if (typeof value$.subscribe !== 'function') {
      value$ = rxjs.of(value$) // Compat for recursive with value
    }

    if (Boolean(this.#value$) !== Boolean(value$) && !this.#sending) {
      this.#sending = true
      // TODO (fix): Why async?
      queueMicrotask(() => {
        this.#sending = false

        if (!this.#patternSubscription) {
          return
        }

        const accepted = Boolean(this.#value$)
        if (this.#accepted === accepted) {
          return
        }

        this.#listener._connection.sendMsg(
          this.#listener._topic,
          accepted ? C.ACTIONS.LISTEN_ACCEPT : C.ACTIONS.LISTEN_REJECT,
          [this.#listener._pattern, this.#name],
        )

        this.#version = null
        this.#accepted = accepted
      })
    }

    this.#value$ = value$

    if (this.#valueSubscription) {
      this.#valueSubscription.unsubscribe()
      this.#valueSubscription = this.#value$?.subscribe(this.#observer)
    }
  }

  error(err) {
    this.#stop()
    // TODO (feat): backoff retryCount * delay?
    // TODO (feat): backoff option?
    this.#timeout = timers.setTimeout(
      (provider) => {
        provider.start()
      },
      10e3,
      this,
    )
    this.#listener._error(this.#name, err)
  }

  #start() {
    try {
      const ret$ = this.#listener._callback(this.#name)
      if (this.#listener._recursive && typeof ret$?.subscribe === 'function') {
        this.patternSubscription = ret$.subscribe(this)
      } else {
        this.patternSubscription = rxjs.of(ret$).subscribe(this)
      }
    } catch (err) {
      this.#listener._error(this.#name, err)
    }
  }

  #stop() {
    if (this.#listener.connected && this.#accepted) {
      this.#listener._connection.sendMsg(this.#listener._topic, C.ACTIONS.LISTEN_REJECT, [
        this.#listener._pattern,
        this.#name,
      ])
    }

    this.#value$ = null
    this.#version = null
    this.#accepted = false
    this.#sending = false

    if (this.#timeout) {
      timers.clearTimeout(this.#timeout)
      this.#timeout = null
    }

    this.#patternSubscription?.unsubscribe()
    this.#patternSubscription = null

    this.#valueSubscription?.unsubscribe()
    this.#valueSubscription = null
  }
}

export default class Listener {
  constructor(topic, pattern, callback, handler, { recursive = false, stringify = null } = {}) {
    this._topic = topic
    this._pattern = pattern
    this._callback = callback
    this._handler = handler
    this._client = this._handler._client
    this._connection = this._handler._connection
    this._subscriptions = new Map()
    this._recursive = recursive
    this._stringify = stringify || JSON.stringify

    this._$onConnectionStateChange()
  }

  get connected() {
    return this._connection.connected
  }

  get stats() {
    return {
      subscriptions: this._subscriptions.size,
    }
  }

  _$destroy() {
    this._reset()

    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [this._pattern])
    }
  }

  _$onMessage(message) {
    if (!this.connected) {
      this._client._$onError(
        C.TOPIC.RECORD,
        C.EVENT.NOT_CONNECTED,
        new Error('received message while not connected'),
        message,
      )
      return
    }

    const name = message.data[1]

    if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
      if (this._subscriptions.has(name)) {
        this._error(name, 'invalid add: listener exists')
      } else {
        this._subscriptions.set(name, new Provider(name, this))
      }
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      this._subscriptions.get(name)?.accept()
    } else if (message.action === C.ACTIONS.LISTEN_REJECT) {
      this._subscriptions.get(name)?.reject()
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._subscriptions.get(name)
      if (provider) {
        provider.dispose()
        this._subscriptions.delete(name)
      } else {
        this._error(name, 'invalid remove: listener missing')
      }
    } else {
      return false
    }
    return true
  }

  _$onConnectionStateChange() {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern])
    } else {
      this._reset()
    }
  }

  _error(name, err) {
    this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err, [this._pattern, name])
  }

  _reset() {
    for (const provider of this._subscriptions.values()) {
      provider.stop()
    }
    this._subscriptions.clear()
  }
}
