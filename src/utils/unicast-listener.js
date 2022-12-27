const C = require('../constants/constants')
const rx = require('rxjs/operators')
const rxjs = require('rxjs')

class Listener {
  constructor(topic, pattern, callback, handler, { stringify = null } = {}) {
    this._topic = topic
    this._pattern = pattern
    this._callback = callback
    this._handler = handler
    this._client = this._handler._client
    this._connection = this._handler._connection
    this._subscriptions = new Map()
    this._stringify = stringify || JSON.stringify
    this._pipe = rxjs.pipe(
      rx.map((value) => {
        if (value == null) {
          throw new Error('invalid value: null')
        }

        if (typeof value !== 'object' && typeof value !== 'string') {
          throw new Error(`invalid value: ${typeof value}`)
        }

        const data = typeof value !== 'string' ? this._stringify(value) : value
        const hash = this._connection.hasher.h64ToString(data)

        return { data, hash }
      }),
      rx.distinctUntilKeyChanged('hash')
    )

    if (this._connection.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
    }
  }

  _$destroy() {
    this._reset()
    if (this._connection.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.UNLISTEN, [this._pattern])
    }
  }

  _$onMessage(message) {
    const name = message.data[1]

    if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      if (this._subscriptions.has(name)) {
        this._error(name, 'invalid accept: listener exists')
        return
      }

      const value$ = this._callback(name)
      if (value$) {
        const subscription = value$.pipe(this._pipe).subscribe({
          next: ({ data, hash }) => {
            this._connection.sendMsg(this._topic, C.ACTIONS.UPDATE, [name, `INF-${hash}`, data])
          },
          error: (err) => {
            this._error(name, err)
            this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
          },
        })
        this._subscriptions.set(name, subscription)
      } else {
        this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
        this._subscriptions.set(name, null)
      }
    } else if (message.action === C.ACTIONS.LISTEN_REJECT) {
      if (!this._subscriptions.has(name)) {
        this._error(name, 'invalid remove: listener missing')
        return
      }

      const subscription = this._subscriptions.get(name)

      subscription?.unsubscribe()

      this._subscriptions.delete(name)
    } else {
      return false
    }
    return true
  }

  _error(name, err) {
    this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, err, [this._pattern, name])
  }

  _reset() {
    for (const subscription of this._subscriptions.values()) {
      subscription?.unsubscribe()
    }
    this._subscriptions.clear()
  }

  _$handleConnectionStateChange() {
    this._reset()
    if (this._connection.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
    }
  }
}

module.exports = Listener
