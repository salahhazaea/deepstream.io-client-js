const NODE_ENV = typeof process !== 'undefined' && process.env && process.env.NODE_ENV
const isNode = typeof process !== 'undefined' && process.toString() === '[object process]'
const isProduction = NODE_ENV === 'production'

module.exports.isNode = isNode
module.exports.isProduction = isProduction

module.exports.deepFreeze = function (o) {
  if (isProduction) {
    return o
  }

  if (!o || typeof o !== 'object' || Object.isFrozen(o)) {
    return o
  }

  Object.freeze(o)

  Object.getOwnPropertyNames(o).forEach((prop) => module.exports.deepFreeze(o[prop]))

  return o
}

module.exports.splitRev = function (s) {
  if (!s) {
    return [-1, '00000000000000']
  }

  const i = s.indexOf('-')
  const ver = s.slice(0, i)

  return [ver.charAt(0) === 'I' ? Infinity : parseInt(ver, 10), s.slice(i + 1)]
}

module.exports.isPlainObject = function (value) {
  if (
    typeof value !== 'object' ||
    value == null ||
    Object.prototype.toString(value) !== '[object Object]'
  ) {
    return false
  }
  if (Object.getPrototypeOf(value) === null) {
    return true
  }
  let proto = value
  while (Object.getPrototypeOf(proto) !== null) {
    proto = Object.getPrototypeOf(proto)
  }
  return Object.getPrototypeOf(value) === proto
}

module.exports.isSameOrNewer = function (a, b) {
  const [av, ar] = module.exports.splitRev(a)
  const [bv, br] = module.exports.splitRev(b)
  return av > bv || (av === bv && ar >= br)
}

module.exports.shallowCopy = function (obj) {
  if (Array.isArray(obj)) {
    return obj.slice(0)
  }

  const copy = {}
  const props = Object.keys(obj)
  for (let i = 0; i < props.length; i++) {
    copy[props[i]] = obj[props[i]]
  }
  return copy
}

module.exports.setTimeout = function (callback, timeoutDuration) {
  if (timeoutDuration !== null) {
    return setTimeout(callback, timeoutDuration)
  } else {
    return -1
  }
}

module.exports.setInterval = function (callback, intervalDuration) {
  if (intervalDuration !== null) {
    return setInterval(callback, intervalDuration)
  } else {
    return -1
  }
}

// From nxt-lib.
module.exports.compareRev = function compareRev(a, b) {
  if (!a || !a.length) {
    return !b || !b.length ? 0 : -1
  } else if (!b || !b.length) {
    return 1
  }

  // Handle INF-XXXXXXXX
  {
    const isInfA = a[0] === 'I'
    const isInfB = b[0] === 'I'
    if (isInfA !== isInfB) {
      return isInfB ? -1 : 1
    }
  }

  let indexA = 0
  const endA = a.length
  let lenA = endA

  let indexB = 0
  const endB = b.length
  let lenB = endB

  // Skip leading zeroes
  while (a[indexA] === '0') {
    ++indexA
    --lenA
  }
  while (b[indexB] === '0') {
    ++indexB
    --lenB
  }

  // Compare the revision number
  let result = 0
  while (indexA < endA && indexB < endB) {
    const ac = a[indexA++]
    const bc = b[indexB++]

    if (ac === '-') {
      if (bc === '-') {
        break
      }
      return -1
    } else if (bc === '-') {
      return 1
    }

    result ||= ac === bc ? 0 : ac < bc ? -1 : 1
  }

  if (result) {
    return result
  }

  // Compare the rest
  while (indexA < endA && indexB < endB) {
    const ac = a[indexA++]
    const bc = b[indexB++]
    if (ac !== bc) {
      return ac < bc ? -1 : 1
    }
  }

  return lenA - lenB
}

module.exports.AbortError = class AbortError extends Error {
  constructor() {
    super('The operation was aborted')
    this.code = 'ABORT_ERR'
    this.name = 'AbortError'
  }
}

module.exports.schedule = isNode ? setImmediate : window.requestIdleCallback
