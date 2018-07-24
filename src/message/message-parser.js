const C = require('../constants/constants')

/**
 * Parses ASCII control character seperated
 * message strings into digestable maps
 *
 * @constructor
 */
const MessageParser = function () {
  this._actions = this._getActions()
}

/**
 * Deserializes values created by MessageBuilder.typed to
 * their original format
 *
 * @param {String} value
 *
 * @public
 * @returns {Mixed} original value
 */
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

/**
 * Turns the ACTION:SHORTCODE constants map
 * around to facilitate shortcode lookup
 *
 * @private
 *
 * @returns {Object} actions
 */
MessageParser.prototype._getActions = function () {
  const actions = {}

  for (const key in C.ACTIONS) {
    actions[C.ACTIONS[key]] = key
  }

  return actions
}

/**
 * Parses an individual message (as oppnosed to a
 * block of multiple messages as is processed by .parse())
 *
 * @param   {String} message
 *
 * @private
 *
 * @returns {Object} parsedMessage
 */
MessageParser.prototype.parseMessage = function (message, client, result) {
  const parts = message.split(C.MESSAGE_PART_SEPERATOR)

  if (parts.length < 2) {
    client._$onError(C.TOPIC.ERROR, C.EVENT.MESSAGE_PARSE_ERROR, new Error('Insufficiant message parts'))
    return null
  }

  if (this._actions[parts[1]] === undefined) {
    client._$onError(C.TOPIC.ERROR, C.EVENT.MESSAGE_PARSE_ERROR, new Error(`Unknown action ${parts[1]}`))
    return null
  }

  result.raw = message
  result.topic = parts[0]
  result.action = parts[1]
  result.data = parts.splice(2)
}

module.exports = new MessageParser()
