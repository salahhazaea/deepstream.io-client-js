const C = require('../constants/constants')
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
    this._data = ''
    this._version = ''

    this._$onConnectionStateChange()

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

      let value$
      try {
        value$ = this._callback(name)
      } catch (err) {
        value$ = rxjs.throwError(() => err)
      }

      if (value$) {
        const subscription = value$
          .pipe(
            rxjs.map((value) => {
              let data
              if (value && typeof value === 'string') {
                if (value.charAt(0) !== '{' && value.charAt(0) !== '[') {
                  throw new Error(`invalid value: ${value}`)
                }
                data = value
              } else if (value && typeof value === 'object') {
                data = this._stringify(value)
              }

              return data
            }),
            rxjs.takeWhile(Boolean)
          )
          .subscribe({
            next: (data) => {
              if (data === this._data) {
                return
              }

              this._data = data
              this._version = `INF-${this._connection.hasher.h64ToString(data)}`
              this._connection.sendMsg(this._topic, C.ACTIONS.UPDATE, [
                name,
                this._version,
                this._data,
              ])
            },
            error: (err) => {
              this._error(name, err)
              this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN_REJECT, [this._pattern, name])
            },
            complete: () => {
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

  _$onConnectionStateChange() {
    if (this.connected) {
      this._connection.sendMsg(this._topic, C.ACTIONS.LISTEN, [this._pattern, 'U'])
    } else {
      this._reset()
    }
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
}

module.exports = Listener
