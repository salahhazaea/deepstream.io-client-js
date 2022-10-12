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
    this._connected = false

    this._$handleConnectionStateChange()
  }

  _$destroy() {
    this._reset()

    if (this._connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [this._pattern])
    }
  }

  _$onMessage(message) {
    if (!this._connected) {
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
        timeout: null,
        patternSubscription: null,
        valueSubscription: null,
        accepted: false,
      }
      provider.clientReject = () => {
        if (!this._connected) {
          return
        }

        if (provider.value$) {
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [
            this._pattern,
            provider.name,
          ])
        }

        provider.version = null
        provider.value$ = null
        provider.accepted = false
        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = null
      }
      provider.clientAccept = (value$) => {
        if (!this._connected) {
          return
        }

        if (!provider.value$) {
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_ACCEPT, [
            this._pattern,
            provider.name,
          ])
        }

        provider.version = null
        provider.value$ = value$
        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
      provider.serverAccept = () => {
        provider.accepted = true
        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = provider.value$.subscribe(provider.observer)
      }
      provider.serverReject = () => {
        provider.accepted = false
        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = null
      }
      provider.stop = () => {
        provider.clientReject()

        provider.value$ = null
        provider.version = null
        provider.accepted = false

        clearTimeout(provider.timeout)
        provider.timeout = null

        provider.patternSubscription?.unsubscribe()
        provider.patternSubscription = null

        provider.valueSubscription?.unsubscribe()
        provider.valueSubscription = null
      }
      provider.next = (value$) => {
        if (value$ && typeof value$.subscribe !== 'function') {
          // Compat for recursive with value
          value$ = rxjs.of(value$)
        }

        if (value$) {
          provider.clientAccept(value$)
        } else {
          provider.clientReject()
        }
      }
      provider.observer = {
        next: (value) => {
          if (value == null) {
            // Compat for recursive with value
            provider.clientReject()
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
      provider.error = (err) => {
        provider.stop()
        // TODO (feat): backoff retryCount * delay?
        // TODO (feat): backoff option?
        provider.timeout = setTimeout(() => {
          provider.start()
        }, 10e3)
        this._error(provider.name, err)
      }
      provider.start = () => {
        try {
          const pattern$ = this._callback(name)
          if (this._recursive && pattern$ && typeof pattern$.subscribe === 'function') {
            provider.patternSubscription = pattern$.subscribe(provider)
          } else {
            provider.next(pattern$)
          }
        } catch (err) {
          this._error(provider.name, err)
        }
      }

      provider.start()

      this._providers.set(provider.name, provider)
    } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      const provider = this._providers.get(name)
      if (!provider || !provider.value$) {
        return
      }

      if (provider.accepted) {
        this._error(name, 'invalid accept: listener started')
      } else {
        provider.serverAccept()
      }
    } else if (message.action === C.ACTIONS.LISTEN_REJECT) {
      const provider = this._providers.get(name)
      if (!provider) {
        return
      }

      if (!provider.accepted) {
        this._error(name, 'invalid reject: listener stopped')
      } else {
        provider.serverReject()
      }
    } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
      const provider = this._providers.get(name)

      if (!provider) {
        this._error(name, 'invalid remove: listener missing')
      } else {
        provider.stop()
        this._providers.delete(provider.name)
      }
    } else {
      return false
    }
    return true
  }

  _$handleConnectionStateChange(connected) {
    this._connected = connected

    if (connected) {
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
      provider.stop()
    }
    this._providers.clear()
  }
}

module.exports = Listener
