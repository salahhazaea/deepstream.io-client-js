const C = require('../constants/constants')
const rx = require('rxjs/operators')
const rxjs = require('rxjs')

class Listener {
  constructor(topic, pattern, callback, handler, { stringify = null, recursive = false } = {}) {
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

    this._$handleConnectionStateChange()

    if (recursive) {
      throw new Error('invalid argument: recursive')
    }
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

    if (message.action === C.ACTIONS.LISTEN_ACCEPT) {
      if (this._subscriptions.has(name)) {
        this._error(name, 'invalid accept: listener exists')
        return
      }

      const subscription = this._callback(name)
        .pipe(this._pipe)
        .subscribe({
          next: ({ data, hash }) => {
            this._connection.sendMsg(C.TOPIC.RECORD, C.ACTIONS.UPDATE, [name, `INF-${hash}`, data])
          },
          error: (err) => {
            this._error(name, err)
          },
        })

      this._subscriptions.set(name, subscription)
    } else if (message.action === C.ACTIONS.REJECT) {
      const subscription = this._subscriptions.get(name)

      if (!subscription) {
        this._error(name, 'invalid remove: listener missing')
        return
      }

      subscription.unsubscribe()

      this._subscriptions.delete(name)
    } else {
      return false
    }
    return true
  }

  _$handleConnectionStateChange() {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
    } else {
      this._reset()
    }
  }

  _error(name, msg) {
    this._client._$onError(this._topic, C.EVENT.LISTENER_ERROR, msg, [this._pattern, name])
  }

  _reset() {
    for (const provider of this._subscriptions.values()) {
      provider.stop()
    }
    this._subscriptions.clear()
  }
}

module.exports = Listener
