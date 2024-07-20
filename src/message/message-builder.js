const C = require('../constants/constants')

const poolEncoder = new TextEncoder()

let poolSize
let poolBuffer
let poolView
let poolOffset

function reallocPool(size) {
  poolSize = size ?? poolSize ?? 1024 * 1024
  poolBuffer = new Uint8Array(new ArrayBuffer(poolSize))
  poolView = new DataView(poolBuffer.buffer)
  poolOffset = 0
}

function alignPool() {
  // Ensure aligned slices
  if (poolOffset & 0x7) {
    poolOffset |= 0x7
    poolOffset++
  }
}

reallocPool()

module.exports.getMsg = function (topic, action, data) {
  if (data && !(data instanceof Array)) {
    throw new Error('data must be an array')
  }

  if (poolOffset + poolSize / 16 >= poolSize) {
    reallocPool()
  } else {
    alignPool()
  }

  const start = poolOffset

  poolBuffer[poolOffset++] = topic.charCodeAt(0)
  poolBuffer[poolOffset++] = 31
  for (let n = 0; n < action.length; n++) {
    poolBuffer[poolOffset++] = action.charCodeAt(n)
  }

  if (data) {
    for (let i = 0; i < data.length; i++) {
      const type = typeof data[i]
      if (data[i] == null) {
        poolBuffer[poolOffset++] = 31
      } else if (type === 'object') {
        poolBuffer[poolOffset++] = 31
        const res = poolEncoder.encodeInto(
          JSON.stringify(data[i]),
          new Uint8Array(poolBuffer.buffer, poolOffset)
        )
        poolOffset += res.written
      } else if (type === 'bigint') {
        poolBuffer[poolOffset++] = 31
        poolView.setBigUint64(poolOffset, data[i], false)
        poolOffset += 8
      } else if (type === 'string') {
        poolBuffer[poolOffset++] = 31
        const res = poolEncoder.encodeInto(data[i], new Uint8Array(poolBuffer.buffer, poolOffset))
        poolOffset += res.written
      } else {
        throw new Error('invalid data')
      }

      if (poolOffset >= poolBuffer.length) {
        reallocPool(start === 0 ? poolSize * 2 : poolSize)
        return this.getMsg(topic, action, data)
      }
    }
  }
  return new Uint8Array(poolBuffer.buffer, start, poolOffset - start)
}

module.exports.typed = function (value) {
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
