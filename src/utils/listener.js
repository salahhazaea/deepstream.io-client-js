const C = require('../constants/constants')
const xuid = require('xuid')
const lz = require('@nxtedition/lz-string')
const { Observable } = require('rxjs')

const Listener = function (topic, pattern, callback, options, client, connection, handler, recursive) {
  this._topic = topic
  this._callback = callback
  this._pattern = pattern
  this._options = options
  this._client = client
  this._connection = connection
  this._handler = handler
  this._providers = new Map()
  this.recursive = recursive

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)

  this._handleConnectionStateChange()
}

Object.defineProperty(Listener.prototype, '_isConnected', {
  get: function _isConnected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Listener.prototype._$destroy = function () {
  if (this._isConnected) {
    this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [ this._pattern ])
  }
  this._reset()
}

Listener.prototype._$onMessage = function (message) {
  const [ , name ] = message.data

  let provider = this._providers.get(name)

  if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND) {
    if (provider) {
      return
    }

    provider = {
      value$: null,
      raw: null
    }
    provider.dispose = () => {
      provider.value$ = null
      if (provider.patternSubscription) {
        provider.patternSubscription.unsubscribe()
        provider.patternSubscription = null
      }
      if (provider.valueSubscription) {
        provider.valueSubscription.unsubscribe()
        provider.valueSubscription = null
      }
    }
    provider.observer = {
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
            data: [ name, version, JSON.parse(raw) ]
          })
        }
      },
      error: err => {
        provider.value$ = null
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, [ this._pattern, err.message || err ])
      }
    }
    provider.patternSubscription = Observable
      .defer(() => Promise.resolve(this._callback(name)))
      // recursive=false: Observable<T>|value|null
      // recursive=true: Observable< Observable<T>|value|null >
      .map(value => value == null || value.subscribe ? value : Observable.of(value))
      // recursive=false: Observable<T|value>|null
      // recursive=true: Observable< Observable<T>|value|null >
      .switchMap(value$ => this.recursive ? value$ : Observable.of(value$))
      // recursive=false: Observable<T|value|null>
      // recursive=true: Observable<T>|value|null
      .map(value$ => value$ == null || value$.subscribe ? value$ : Observable.of(value$))
      // Observable<T|value>|null
      .subscribe({
        next: value$ => {
          if (value$) {
            if (!provider.value$) {
              this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_ACCEPT, [ this._pattern, name ])
            }

            if (provider.valueSubscription) {
              provider.valueSubscription.unsubscribe()
              provider.valueSubscription = value$.subscribe(provider.observer)
            }
          } else {
            if (provider.value$) {
              this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
            }

            if (provider.valueSubscription) {
              provider.valueSubscription.unsubscribe()
              provider.valueSubscription = null
            }
          }
          provider.value$ = value$
        },
        error: err => {
          provider.value$ = null
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
          this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, [ this._pattern, err.message || err ])
        }
      })

    this._providers.set(name, provider)
  } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
    if (!provider || !provider.value$) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
      return
    }

    if (!provider.valueSubscription) {
      provider.valueSubscription = provider.value$.subscribe(provider.observer)
    }
  } else if (message.action === C.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED) {
    if (!provider) {
      return
    }

    provider.dispose()
    this._providers.delete(name)
  }
}

Listener.prototype._handleConnectionStateChange = function () {
  const state = this._client.getConnectionState()

  if (state === C.CONNECTION_STATE.OPEN) {
    this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [ this._pattern ])
  } else if (state === C.CONNECTION_STATE.RECONNECTING) {
    this._reset()
  }
}

Listener.prototype._reset = function () {
  for (const provider of this._providers.values()) {
    provider.dispose()
  }
  this._providers.clear()
}

module.exports = Listener
