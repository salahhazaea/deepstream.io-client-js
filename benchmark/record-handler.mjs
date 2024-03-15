import { bench, run } from 'mitata'
import createDeepstream from '../src/client.js'

const ds = createDeepstream('ws://localhost:6020/deepstream')

bench('record.get', () => {
  ds.record.get(Math.random().toString())
})

bench('record.observe', () => {
  ds.record.observe(Math.random().toString()).subscribe()
})

await run()
