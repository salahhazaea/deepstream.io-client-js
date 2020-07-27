const C = require('../constants/constants')
const xuid = require('xuid')
const { Observable } = require('rxjs')
const lz = require('@nxtedition/lz-string')

class Listener {
  constructor (topic, pattern, callback, handler, recursive) {
    this._topic = topic
    this._pattern = pattern
    this._callback = callback
    this._handler = handler
    this._options = this._handler._options
    this._client = this._handler._client
    this._connection = this._handler._connection
    this._providers = new Map()
    this._recursive = recursive

    this._$handleConnectionStateChange()
  }

  get connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }

  _$destroy () {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [this._pattern])
    }

    this._reset()
  }

  _$onMessage (message) {
    const name = message.data[1]

    if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
      if (this._providers.has(name)) {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener exists', [this._pattern, name])
        return
      }

      const provider = {
        name,
        value$: null,
        patternSubscription: null,
        valueSubscription: null
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
      provider.next = value$ => {
        if (!value$) {
          value$ = null
        } else if (!value$.subscribe) {
          // Compat for recursive with value
          value$ = Observable.of(value$)
        }

        if (Boolean(value$) !== Boolean(provider.value$)) {
          this._connection.sendMsg(this._topic, value$ ? C.ACTIONS.LISTEN_ACCEPT : C.ACTIONS.LISTEN_REJECT, [this._pattern, provider.name])
        }

        provider.value$ = value$
        if (provider.valueSubscription) {
          provider.valueSubscription.unsubscribe()
          provider.valueSubscription = value$ ? value$.subscribe(provider.observer) : null
        }
      }
      provider.error = err => {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err, [this._pattern, provider.name])

        if (provider.value$) {
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, provider.name])
          provider.value$ = null
        }

        provider.dispose()
      }
      provider.observer = {
        next: value => {
          if (!value) {
            provider.next(null)
            return
          }

          if (typeof value !== 'object') {
            const err = new Error('invalid value')
            this._client._$onError(this._topic, C.EVENT.USER_ERROR, err, [this._pattern, provider.name, value])
            return
          }

          if (this._topic === C.TOPIC.EVENT) {
            this._handler.emit(provider.name, value)
          } else if (this._topic === C.TOPIC.RECORD) {
            // TODO (perf): Check for equality before compression.
            let body
            try {
              body = lz.compressToUTF16(JSON.stringify(value))
            } catch (err) {
              this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, [this._pattern, provider.name, value])
              return
            }

            const version = `INF-${xuid()}-${this._client.user || ''}`

            this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [provider.name, version, body])
            this._handler._$handle({
              action: C.ACTIONS.UPDATE,
              data: [provider.name, version, value]
            })
          }
        },
        error: provider.error
      }

      let provider$
      try {
        provider$ = this._callback(name)
        if (!this._recursive) {
          provider$ = Observable.of(provider$)
        }
      } catch (err) {
        provider$ = Observable.throw(err)
      }

      this._providers.set(provider.name, provider)
      provider.patternSubscription = provider$.subscribe(provider)
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      const provider = this._providers.get(name)

      if (provider && provider.valueSubscription) {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener started', [this._pattern, name])
      } else if (!provider || !provider.value$) {
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
      } else {
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._providers.get(name)

      if (!provider) {
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, 'listener not found', [this._pattern, name])
        return
      }

      provider.dispose()
      this._providers.delete(provider.name)
    } else {
      return false
    }
    return true
  }

  _$handleConnectionStateChange () {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern])
    } else {
      this._reset()
    }
  }

  _reset () {
    for (const provider of this._providers.values()) {
      provider.dispose()
    }
    this._providers.clear()
  }
}

module.exports = Listener
