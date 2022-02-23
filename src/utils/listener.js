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
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener exists', [
          this._pattern,
          name,
        ])
        return
      }

      const provider = {
        name,
        value$: undefined,
        version: null,
        body: null,
        accepted: false,
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
        this._providers.delete(provider.name)
      }
      provider.next = (value$) => {
        if (value$ && !value$.subscribe) {
          // Compat for recursive with value
          value$ = rxjs.of(value$)
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

        provider.value$ = value$ || null
        if (provider.valueSubscription) {
          provider.valueSubscription.unsubscribe()
          provider.valueSubscription = value$ ? value$.subscribe(provider.observer) : null
        }
      }
      provider.error = (err) => {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err, [
          this._pattern,
          provider.name,
        ])

        if (provider.accepted) {
          provider.accepted = false
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [
            this._pattern,
            provider.name,
          ])
          provider.value$ = null
        }

        provider.dispose()
      }
      provider.observer = {
        next: (value) => {
          if (value == null) {
            provider.next(null)
            return
          }

          if (this._topic === C.TOPIC.EVENT) {
            this._handler.emit(provider.name, value)
          } else if (this._topic === C.TOPIC.RECORD) {
            if (typeof value !== 'object' && typeof value !== 'string') {
              const err = new Error('invalid value')
              this._client._$onError(this._topic, C.EVENT.USER_ERROR, err, [
                this._pattern,
                provider.name,
                value,
              ])
              return
            }

            const body = typeof value !== 'string' ? this._stringify(value) : value
            const hash = this._connection.hasher.h64ToString(body)
            const version = `INF-${hash}`

            if (provider.version !== version) {
              provider.version = version
              const data = [provider.name, version, body]
              this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, data)
              this._handler._$handle({ action: C.ACTIONS.UPDATE, data })
            }
          }
        },
        error: provider.error,
      }

      try {
        let provider$
        try {
          provider$ = this._callback(name)
          if (!this._recursive) {
            provider$ = rxjs.of(provider$)
          }
        } catch (err) {
          provider$ = rxjs.throwError(err)
        }

        provider.patternSubscription = provider$.subscribe(provider)
        this._providers.set(provider.name, provider)
      } catch (err) {
        this._client._$onError(this._topic, C.EVENT.USER_ERROR, err, [this._pattern, name])
      }
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      const provider = this._providers.get(name)

      if (provider && provider.valueSubscription) {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener started', [
          this._pattern,
          name,
        ])
      } else if (!provider || !provider.value$) {
        if (provider.accepted) {
          provider.accepted = false
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
        }
      } else {
        // TODO (fix): provider.version = message.data[2]
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._providers.get(name)

      if (!provider) {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener not found', [
          this._pattern,
          name,
        ])
        return
      }

      provider.dispose()
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

  _reset() {
    for (const provider of this._providers.values()) {
      provider.dispose()
    }
    this._providers.clear()
  }
}

module.exports = Listener
