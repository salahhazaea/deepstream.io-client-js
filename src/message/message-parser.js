import * as C from '../constants/constants.js'

export function convertTyped(value, client) {
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
