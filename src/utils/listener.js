const C = require('../constants/constants')
const xuid = require('xuid')
const { Observable } = require('rxjs')

const Listener = function (topic, pattern, callback, handler, recursive) {
  this._topic = topic
  this._pattern = pattern
  this._callback = callback
  this._handler = handler
  this._options = this._handler._options
  this._client = this._handler._client
  this._connection = this._handler._connection
  this._lz = this._handler._lz
  this._providers = new Map()

  this.recursive = recursive

  this._$handleConnectionStateChange()
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
      this._providers.delete(name)
    }
    provider.observer = {
      next: value => {
        if (value == null) {
          if (provider.valueSubscription) {
            provider.valueSubscription.unsubscribe()
            provider.valueSubscription = null
          }

          provider.value$ = null

          this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [ this._pattern, name ])
          return
        }

        if (typeof value !== 'object') {
          const err = new Error('invalid value')
          this._client._$onError(this._topic, C.EVENT.USER_ERROR, err, [ this._pattern, name, value ])
          return
        }

        if (this._topic === C.TOPIC.EVENT) {
          this._handler.emit(name, value)
        } else if (this._topic === C.TOPIC.RECORD) {
          // TODO (perf): Check for equality before compression.
          // TODO (perf): Avoid closure allocation.
          this._lz.compress(value, (body, err) => {
            if (err || !body) {
              this._client._$onError(this._topic, C.EVENT.LZ_ERROR, err, [ this._pattern, name, value ])
              return
            }

            if (provider.body !== body || !/^INF-/.test(provider.version)) {
              provider.version = `INF-${xuid()}-${this._client.user || ''}`
              provider.body = body
              provider.ready = true
              this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [ name, provider.version, provider.body ])

              this._handler._$handle({
                action: C.ACTIONS.UPDATE,
                data: [ name, provider.version, body ]
              })
            } else if (!provider.ready) {
              provider.ready = true
              // TODO (perf): Sending body here should be unnecessary.
              this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [ name, provider.version, provider.body ])

              this._handler._$handle({
                action: C.ACTIONS.UPDATE,
                data: [ name, provider.version, body ]
              })
            }
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
      .defer(() => {
        const result = this._callback(name)
        if (result && result.then) {
          // TODO (fix): Deprecation warning
        }
        return Promise.resolve(result)
      })
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
          provider.dispose()
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
    if (provider) {
      provider.dispose()
    }
  }
}

Listener.prototype._$handleConnectionStateChange = function () {
  if (this.connected) {
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
