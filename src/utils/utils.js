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

module.exports.isPlainObject = function (value, isPlainJSON) {
  if (isPlainJSON) {
    return value && typeof value === 'object' && !Array.isArray(value)
  }
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

module.exports.compareRev = function compareRev(a, b) {
  if (!a) {
    return b ? -1 : 0
  }

  if (!b) {
    return a ? 1 : 0
  }

  if (a === b) {
    return 0
  }

  const av = a[0] === 'I' ? Number.MAX_SAFE_INTEGER : parseInt(a)
  const bv = b[0] === 'I' ? Number.MAX_SAFE_INTEGER : parseInt(b)

  if (av !== bv) {
    return av > bv ? 1 : -1
  }

  const ar = a.slice(a.indexOf('-') + 1)
  const br = b.slice(b.indexOf('-') + 1)

  if (ar !== br) {
    return ar > br ? 1 : -1
  }

  return 0
}

module.exports.AbortError = class AbortError extends Error {
  constructor() {
    super('The operation was aborted')
    this.code = 'ABORT_ERR'
    this.name = 'AbortError'
  }
}

function defaultSchedule(fn) {
  setTimeout(fn, 0)
}

module.exports.schedule = isNode ? defaultSchedule : window.requestIdleCallback

const abortSignals = new WeakMap()
const onAbort = function () {
  const handlers = abortSignals.get(this)
  if (handlers) {
    for (const handler of handlers) {
      handler()
    }
  }
}

module.exports.addAbortListener = function addAbortListener(signal, handler) {
  if (!signal) {
    return
  }

  if (signal.aborted) {
    queueMicrotask(handler)
  } else {
    let handlers = abortSignals.get(signal)
    if (!handlers) {
      handlers = []
      abortSignals.set(signal, handlers)
      signal.addEventListener('abort', onAbort)
    }
    handlers.push(handler)
  }
}

module.exports.removeAbortListener = function removeAbortListener(signal, handler) {
  if (!signal) {
    return
  }

  const handlers = abortSignals.get(signal)
  if (handlers) {
    const index = handlers.indexOf(handler)
    if (index !== -1) {
      handlers.splice(index, 1)
      if (handlers.length === 0) {
        abortSignals.delete(signal)
        signal.removeEventListener('abort', onAbort)
      }
    }
  }
}
