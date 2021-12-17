'use strict'

const { parentPort, workerData } = require('worker_threads')
const NodeWebSocket = require('ws')

const { url } = workerData

const endpoint = new NodeWebSocket(new URL(url))

endpoint.onopen = () => parentPort.postMessage({ event: 'open' })
endpoint.onerror = (err) => parentPort.postMessage({ event: 'error', data: err })
endpoint.onclose = () => parentPort.postMessage({ event: 'close' })
endpoint.onmessage = ({ data }) => {
  // TODO (perf): Transfer Buffer?
  parentPort.postMessage({ event: 'data', data: typeof data === 'string' ? data : data.toString() })
}

parentPort.on('message', (message) => {
  endpoint.send(message)
})
