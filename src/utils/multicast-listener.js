const C = require('../constants/constants')
const rxjs = require('rxjs')

class Listener {
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
        message
      )
      return
    }

    const name = message.data[1]

    if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
      if (this._subscriptions.has(name)) {
        this._error(name, 'invalid add: listener exists')
        return
      }

      // TODO (refactor): Move to class
      const provider = {
        name,
        value$: null,
        sending: false,
        accepted: false,
        version: null,
        timeout: null,
        patternSubscription: null,
        valueSubscription: null,
      }
      provider.stop = () => {
        if (this.connected && provider.accepted) {
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [
            this._pattern,
            provider.name,
          ])
        }

        provider.value$ = null
        provider.version = null
        provider.accepted = false
        provider.sending = false

        clearTimeout(provider.timeout)
        provider.timeout = null

        provider.patternSubscription?.unsubscribe()
        provider.patternSubscription = null

        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = null
      }
      provider.send = () => {
        provider.sending = false

        if (!provider.patternSubscription) {
          return
        }

        const accepted = Boolean(provider.value$)
        if (provider.accepted === accepted) {
          return
        }

        this._connection.sendMsg(
          this._topic,
          accepted ? C.ACTIONS.LISTEN_ACCEPT : C.ACTIONS.LISTEN_REJECT,
          [this._pattern, provider.name]
        )

        provider.version = null
        provider.accepted = accepted
      }
      provider.next = (value$) => {
        if (!value$) {
          value$ = null
        } else if (typeof value$.subscribe !== 'function') {
          value$ = rxjs.of(value$) // Compat for recursive with value
        }

        if (Boolean(provider.value$) !== Boolean(value$) && !provider.sending) {
          provider.sending = true
          queueMicrotask(provider.send)
        }

        provider.value$ = value$

        if (provider.valueSubscription) {
          provider.valueSubscription.unsubscribe()
          provider.valueSubscription = provider.value$?.subscribe(provider.observer)
        }
      }
      provider.error = (err) => {
        provider.stop()
        // TODO (feat): backoff retryCount * delay?
        // TODO (feat): backoff option?
        provider.timeout = setTimeout(() => {
          provider.start()
        }, 10e3)
        this._error(provider.name, err)
      }
      provider.observer = {
        next: (value) => {
          if (value == null) {
            provider.next(null) // TODO (fix): This is weird...
            return
          }

          if (this._topic === C.TOPIC.EVENT) {
            this._handler.emit(provider.name, value)
          } else if (this._topic === C.TOPIC.RECORD) {
            if (typeof value !== 'object' && typeof value !== 'string') {
              this._error(provider.name, 'invalid value')
              return
            }

            const body = typeof value !== 'string' ? this._stringify(value) : value
            const hash = this._connection.hasher.h64ToString(body)
            const version = `INF-${hash}`

            if (provider.version !== version) {
              provider.version = version
              this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [
                provider.name,
                version,
                body,
              ])
            }
          }
        },
        error: provider.error,
      }
      provider.start = () => {
        try {
          const ret$ = this._callback(name)
          if (this._recursive && typeof ret$?.subscribe === 'function') {
            provider.patternSubscription = ret$.subscribe(provider)
          } else {
            provider.patternSubscription = rxjs.of(ret$).subscribe(provider)
          }
        } catch (err) {
          this._error(provider.name, err)
        }
      }

      provider.start()

      this._subscriptions.set(provider.name, provider)
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      const provider = this._subscriptions.get(name)
      if (!provider?.value$) {
        return
      }

      if (provider.valueSubscription) {
        this._error(name, 'invalid accept: listener started')
      } else {
        // TODO (fix): provider.version = message.data[2]
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._subscriptions.get(name)

      if (!provider) {
        this._error(name, 'invalid remove: listener missing')
      } else {
        provider.stop()
        this._subscriptions.delete(provider.name)
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

module.exports = Listener
