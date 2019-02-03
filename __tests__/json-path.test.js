
const jsonPath = require('../src/record/json-path')

describe('undefined props', () => {
  it ('ignore undefined updates', () => {
    const val1 = {
      type: 'event',
      parent: 'MYZRXbx919v_Sv',
      time: 1548968391.703,
      duration: 796.8669998645782
    }
    expect(jsonPath.set(val1, undefined, {
      ...val1,
      gallery: undefined,
      rundown: undefined
    })).toBe(val1)
  })

  it ('ignore undefined updates on empty object', () => {
    const val1 = {}
    expect(jsonPath.set(val1, undefined, {
      ...val1,
      gallery: undefined,
      rundown: undefined
    })).toBe(val1)
  })

  it ('ignore undefined updates on new object', () => {
    const val1 = {
      time: 1
    }
    const res = jsonPath.set(val1, undefined, {
      ...val1,
      time: undefined
    })
    expect(res).not.toBe(val1)
    expect(Object.keys(res)).toEqual([])
  })
})

describe('equality', () => {
  it('return old value', () => {
    function test(val1, val2) {
      return jsonPath.set(val1, undefined, val2) == val1
    }

    expect(test({
      asd: true
    }, {
      asd: true
    })).toBe(true)

    expect(test({
      asd: true
    }, {
      asd: true,
      foo: undefined
    })).toBe(true)

    expect(test({}, {})).toBe(true)
    expect(test({}, { foo: undefined })).toBe(true)
  })
})

describe('order', () => {
  it('updates order', () => {
    const val1 = {
      foo: 1,
      bar: 1
    }
    const res = jsonPath.set(val1, undefined, {
      bar: 1,
      foo: 1
    })
    expect(res).not.toBe(val1)
  })

  it('keeps order', () => {
    const val1 = {
      foo: 1,
      bar: 1
    }
    const res = jsonPath.set(val1, undefined, {
      foo: 1,
      bar: 1
    })
    expect(res).toBe(val1)
  })
})
