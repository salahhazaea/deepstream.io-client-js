import * as C from '../constants/constants.js'

const SEP = C.MESSAGE_PART_SEPERATOR

export function getMsg(topic, action, data) {
  if (data && !(data instanceof Array)) {
    throw new Error('data must be an array')
  }

  const sendData = [topic, action]

  if (data) {
    for (let i = 0; i < data.length; i++) {
      if (typeof data[i] === 'object') {
        sendData.push(JSON.stringify(data[i]))
      } else {
        sendData.push(data[i])
      }
    }
  }

  return sendData.join(SEP)
}

export function typed(value) {
  const type = typeof value

  if (type === 'string') {
    return C.TYPES.STRING + value
  }

  if (value === null) {
    return C.TYPES.NULL
  }

  if (type === 'object') {
    return C.TYPES.OBJECT + JSON.stringify(value)
  }

  if (type === 'number') {
    return C.TYPES.NUMBER + value.toString()
  }

  if (value === true) {
    return C.TYPES.TRUE
  }

  if (value === false) {
    return C.TYPES.FALSE
  }

  if (value === undefined) {
    return C.TYPES.UNDEFINED
  }

  throw new Error(`Can't serialize type ${value}`)
}
