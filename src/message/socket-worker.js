'use strict'

const { workerData } = require('worker_threads')
const NodeWebSocket = require('ws')

const { port, url } = workerData

const endpoint = new NodeWebSocket(url)

endpoint.onopen = () => port.postMessage({ event: 'open' })
endpoint.onerror = (err) => port.postMessage({ event: 'error', data: err })
endpoint.onclose = () => port.postMessage({ event: 'close' })
endpoint.onmessage = ({ data }) => {
  if (typeof data === 'string') {
    data = Buffer.from(data)
  }
  port.postMessage({ event: 'data', data }, [data.buffer])
}

port.on('message', (message) => {
  endpoint.send(message)
})
