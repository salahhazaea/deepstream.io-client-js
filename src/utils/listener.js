const C = require('../constants/constants')
const xuid = require('xuid')
const { Observable } = require('rxjs')

const Listener = function (topic, pattern, callback, options, client, connection, handler, recursive) {
  this._topic = topic
  this._callback = callback
  this._pattern = pattern
  this._options = options
  this._client = client
  this._connection = connection
  this._handler = handler
  this._lz = handler._lz
  this._providers = new Map()
  this.recursive = recursive

  this._handleConnectionStateChange = this._handleConnectionStateChange.bind(this)

  this._client.on('connectionStateChanged', this._handleConnectionStateChange)
  this._handleConnectionStateChange()
}

Object.defineProperty(Listener.prototype, 'connected', {
  get: function connected () {
    return this._client.getConnectionState() === C.CONNECTION_STATE.OPEN
  }
})

Listener.prototype._$destroy = function () {
  if (this.connected) {
    this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [ this._pattern ])
  }

  this._reset()

  this._client.off('connectionStateChanged', this._handleConnectionStateChange)
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
      version: null,
      body: null,
      ready: false,
      patternSubscription: null,
      valueSubscription: null
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
        if (value == null) {
          provider.value$ = null
          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
          return
        }

        if (this._topic === C.TOPIC.EVENT) {
          this._handler.emit(name, value)
        } else if (this._topic === C.TOPIC.RECORD) {
          // TODO (perf): Check for equality before compression.
          // TODO (perf): Avoid closure allocation.
          this._lz.compress(value, (err, body) => {
            if (err || !body) {
              this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, [ this._pattern, name, value ])
              return
            }

            if (provider.ready && provider.body === body) {
              return
            }

            const version = provider.version && provider.version.startsWith('INF') && provider.body === body
              ? provider.version
              : `INF-${xuid()}`

            this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [ name, version, body ])

            this._handler._$handle({
              action: C.ACTIONS.UPDATE,
              data: [ name, version, value ]
            })

            provider.ready = true
            provider.version = version
            provider.body = body
          })
        }
      },
      error: err => {
        provider.value$ = null
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
        this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err)
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
          this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err)
        }
      })

    this._providers.set(name, provider)
  } else if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
    if (!provider || !provider.value$) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
      return
    }

    if (!provider.valueSubscription) {
      const [ version, body ] = message.data.slice(2)
      provider.ready = false
      provider.version = version
      provider.body = body
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
  } else {
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
