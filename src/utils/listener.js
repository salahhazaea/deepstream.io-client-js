const C = require('../constants/constants')
const rxjs = require('rxjs')

class Listener {
  constructor(topic, pattern, callback, handler, recursive, stringify) {
    this._topic = topic
    this._pattern = pattern
    this._callback = callback
    this._handler = handler
    this._options = this._handler._options
    this._client = this._handler._client
    this._connection = this._handler._connection
    this._providers = new Map()
    this._recursive = recursive
    this._stringify = stringify || JSON.stringify

    this._$handleConnectionStateChange()
  }

  get connected() {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
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
      if (this._providers.has(name)) {
        this._error(name, 'invalid add: listener exists')
        return
      }

      // TODO (refactor): Move to class
      const provider = {
        name,
        value$: null,
        version: null,
        accepted: null,
        patternSubscription: null,
        valueSubscription: null,
      }
      provider.dispose = () => {
        if (provider.patternSubscription) {
          provider.patternSubscription.unsubscribe()
          provider.patternSubscription = null
        }
        if (provider.valueSubscription) {
          provider.valueSubscription.unsubscribe()
          provider.valueSubscription = null
        }
      }
      provider.next = (value$) => {
        if (value$ && typeof value$.subscribe !== 'function') {
          // Compat for recursive with value
          value$ = rxjs.of(value$)
        } else if (!value$) {
          value$ = null
        }

        const accepted = Boolean(value$)
        if (provider.accepted !== accepted) {
          provider.accepted = accepted
          this._connection.sendMsg(
            this._topic,
            accepted ? C.ACTIONS.LISTEN_ACCEPT : C.ACTIONS.LISTEN_REJECT,
            [this._pattern, provider.name]
          )
        }

        provider.value$ = value$
        if (provider.valueSubscription) {
          provider.valueSubscription.unsubscribe()
          provider.valueSubscription = value$ ? value$.subscribe(provider.observer) : null
        }
      }
      provider.error = (err) => {
        this._error(provider.name, err)
        provider.next(null)
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

      try {
        const provider$ = this._callback(name)
        if (this._recursive) {
          provider.patternSubscription = provider$.subscribe(provider)
        } else {
          provider.next(provider$)
        }
      } catch (err) {
        provider.error(err)
      }

      this._providers.set(provider.name, provider)
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      const provider = this._providers.get(name)

      if (!provider || !provider.value$) {
        this._error(name, 'invalid accept: listener missing')
      } else if (provider.valueSubscription) {
        this._error(name, 'invalid accept: listener started')
      } else if (!provider.accepted) {
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
      } else {
        // TODO (fix): provider.version = message.data[2]
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._providers.get(name)

      if (!provider) {
        this._error(name, 'invalid remove: listener missing')
      } else {
        provider.dispose()
        this._providers.delete(provider.name)
      }
    } else {
      return false
    }
    return true
  }

  _$handleConnectionStateChange() {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern])
    } else {
      this._reset()
    }
  }

  _error(name, msg) {
    this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, msg, [this._pattern, name])
  }

  _reset() {
    for (const provider of this._providers.values()) {
      provider.dispose()
    }
    this._providers.clear()
  }
}

module.exports = Listener
