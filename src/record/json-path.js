const utils = require('../utils/utils')
const PARTS_REG_EXP = /([^.[\]\s]+)/g

const cache = new Map()
const EMPTY = utils.deepFreeze({})

function get (data, path) {
  const tokens = tokenize(path)

  data = data || EMPTY

  for (let i = 0; i < tokens.length; i++) {
    if (data == null || typeof data !== 'object') {
      return undefined
    }
    data = data[tokens[i]]
  }

  return data
}

function set (data, path, value) {
  const tokens = tokenize(path)

  if (tokens.length === 0) {
    return patch(data, value)
  }

  const oldValue = get(data, path)
  const newValue = patch(oldValue, value)

  if (newValue === oldValue) {
    return data
  }

  const result = data ? utils.shallowCopy(data) : {}

  let node = result
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    if (i === tokens.length - 1) {
      node[token] = newValue
    } else if (node[token] != null && typeof node[token] === 'object') {
      node = node[token] = utils.shallowCopy(node[token])
    } else if (tokens[i + 1] && !isNaN(tokens[i + 1])) {
      node = node[token] = []
    } else {
      node = node[token] = {}
    }
  }
  return result
}

function patch (oldValue, newValue) {
  if (
    newValue != null &&
    typeof newValue !== 'string' &&
    typeof newValue !== 'number' &&
    typeof newValue !== 'boolean' &&
    !utils.isPlainObject(newValue)
  ) {
    throw new Error('invalid operation: can\'t patch with non-plain object')
  }

  if (oldValue === newValue) {
    return oldValue
  } else if (oldValue === null || newValue === null) {
    return newValue
  } else if (Array.isArray(oldValue) && Array.isArray(newValue)) {
    // TODO (perf): Return newValue when possible...
    let arr = newValue.length === oldValue.length ? null : []
    for (let i = 0; i < newValue.length; i++) {
      const value = patch(oldValue[i], newValue[i])

      if (!arr) {
        if (value === oldValue[i]) {
          continue
        }
        arr = []
        for (let j = 0; j < i; ++j) {
          arr[j] = oldValue[j]
        }
      }
      arr[i] = value
    }

    return arr || oldValue
  } else if (!Array.isArray(newValue) && typeof oldValue === 'object' && typeof newValue === 'object') {
    // TODO (perf): Return newValue when possible...
    const newKeys = Object.keys(newValue)
    const oldKeys = Object.keys(oldValue)

    let obj = newKeys.length === oldKeys.length ? null : {}
    for (let i = 0; i < newKeys.length; ++i) {
      const key = newKeys[i]
      const val = patch(oldValue[key], newValue[key])

      if (!obj) {
        if (val === oldValue[key] && key === oldKeys[i]) {
          continue
        }
        obj = {}
        for (let j = 0; j < i; j++) {
          obj[newKeys[j]] = oldValue[newKeys[j]]
        }
      }
      obj[key] = val
    }

    return obj || oldValue
  } else {
    return newValue
  }
}

function tokenize (path) {
  if (!path) {
    return []
  }

  let parts = cache.get(path)

  if (parts) {
    return parts
  }

  parts = path && String(path) !== 'undefined' ? String(path).match(PARTS_REG_EXP) : []

  if (!parts) {
    throw new Error('invalid path ' + path)
  }

  cache.set(path, parts)

  return parts
}

module.exports = {
  EMPTY,
  get,
  set
}
