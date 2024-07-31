import { randomBytes } from 'node:crypto'
import { Bench } from 'tinybench'
import { getMsg } from '../src/message/message-builder.js'

const bench = new Bench()

const name = randomBytes(16).toString('hex')
const version = randomBytes(16).toString('hex')
const body = randomBytes(256).toString('hex')

function getMsgString(topic, action, data) {
  let msg = `R${topic}U${action}`
  if (data) {
    for (let i = 0; i < data.length; i++) {
      const type = typeof data[i]
      if (data[i] == null) {
        msg += '\u0001'
        msg += ''
      } else if (type === 'object') {
        msg += '\u0001'
        msg += JSON.stringify(data[i])
      } else if (type === 'bigint') {
        msg += '\u0001'
        msg += data[i].toString()
      } else if (type === 'string') {
        msg += '\u0001'
        msg += data[i]
      } else {
        throw new Error('invalid data')
      }
    }
  }
  return msg
}

bench
  .add('build message string', () => {
    getMsgString('R', 'U', [ name, version, body ])
  })
  .add('build message buffer', () => {
    getMsg('R', 'U', [ name, version, body ])
  })

await bench.warmup()
await bench.run()

console.table(bench.table())
