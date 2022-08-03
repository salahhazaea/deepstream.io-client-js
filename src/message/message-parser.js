const C = require('../constants/constants')

const MessageParser = function () {
  this._actions = this._getActions()
}

MessageParser.prototype.convertTyped = function (value, client) {
  const type = value.charAt(0)

  if (type === C.TYPES.STRING) {
    return value.substr(1)
  }

  if (type === C.TYPES.OBJECT) {
    try {
      return JSON.parse(value.substr(1))
    } catch (err) {
      client._$onError(C.TOPIC.ERROR, C.EVENT.MESSAGE_PARSE_ERROR, err)
      return undefined
    }
  }

  if (type === C.TYPES.NUMBER) {
    return parseFloat(value.substr(1))
  }

  if (type === C.TYPES.NULL) {
    return null
  }

  if (type === C.TYPES.TRUE) {
    return true
  }

  if (type === C.TYPES.FALSE) {
    return false
  }

  if (type === C.TYPES.UNDEFINED) {
    return undefined
  }

  client._$onError(C.TOPIC.ERROR, C.EVENT.MESSAGE_PARSE_ERROR, new Error(`UNKNOWN_TYPE (${value})`))

  return undefined
}

MessageParser.prototype._getActions = function () {
  const actions = {}

  for (const key in C.ACTIONS) {
    actions[C.ACTIONS[key]] = key
  }

  return actions
}

MessageParser.prototype.parseMessage = function (message, client, result) {
  const parts = message.split(C.MESSAGE_PART_SEPERATOR)

  if (parts.length < 2) {
    client._$onError(
      C.TOPIC.ERROR,
      C.EVENT.MESSAGE_PARSE_ERROR,
      new Error('Insufficiant message parts')
    )
    return null
  }

  if (parts[0] === C.TOPIC.ERROR) {
    client._$onError(C.TOPIC.ERROR, parts[1], new Error('Message error'), message)
    return null
  }

  if (this._actions[parts[1]] === undefined) {
    client._$onError(
      C.TOPIC.ERROR,
      C.EVENT.MESSAGE_PARSE_ERROR,
      new Error('Unknown action'),
      message
    )
    return null
  }

  result.raw = message
  result.topic = parts[0]
  result.action = parts[1]
  result.data = parts.splice(2)
}

module.exports = new MessageParser()
