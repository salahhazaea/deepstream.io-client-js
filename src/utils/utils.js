const NODE_ENV = typeof process !== 'undefined' && process.env && process.env.NODE_ENV
export const isNode = typeof process !== 'undefined' && process.toString() === '[object process]'
export const isProduction = NODE_ENV === 'production'

export function deepFreeze(o) {
  if (isProduction) {
    return o
  }

  if (!o || typeof o !== 'object' || Object.isFrozen(o)) {
    return o
  }

  Object.freeze(o)

  Object.getOwnPropertyNames(o).forEach((prop) => deepFreeze(o[prop]))

  return o
}

export function splitRev(s) {
  if (!s) {
    return [-1, '00000000000000']
  }

  const i = s.indexOf('-')
  const ver = s.slice(0, i)

  return [ver.charAt(0) === 'I' ? Infinity : parseInt(ver, 10), s.slice(i + 1)]
}

export function isPlainObject(value, isPlainJSON) {
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

export function isSameOrNewer(a, b) {
  const [av, ar] = splitRev(a)
  const [bv, br] = splitRev(b)
  return av > bv || (av === bv && ar >= br)
}

export function shallowCopy(obj) {
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

export function setTimeout(callback, timeoutDuration) {
  if (timeoutDuration !== null) {
    return globalThis.setTimeout(callback, timeoutDuration)
  } else {
    return -1
  }
}

export function setInterval(callback, intervalDuration) {
  if (intervalDuration !== null) {
    return globalThis.setInterval(callback, intervalDuration)
  } else {
    return -1
  }
}

export function compareRev(a, b) {
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

export class AbortError extends Error {
  constructor() {
    super('The operation was aborted')
    this.code = 'ABORT_ERR'
    this.name = 'AbortError'
  }
}

function defaultSchedule(fn) {
  setTimeout(fn, 0)
}

export const schedule = isNode ? defaultSchedule : window.requestIdleCallback

const abortSignals = new WeakMap()
const onAbort = function () {
  const handlers = abortSignals.get(this)
  if (handlers) {
    for (const handler of handlers) {
      handler()
    }
  }
}

export function addAbortListener(signal, handler) {
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

export function removeAbortListener(signal, handler) {
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
