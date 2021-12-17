'use strict'

const { Worker } = require('worker_threads')
const path = require('path')

class Socket {
  constructor(url) {
    this._worker = new Worker(path.join(__dirname, 'socket-worker.js'), {
      workerData: {
        url: url.href,
      },
    })
      .on('error', (err) => {
        this.onerror?.(err)
      })
      .on('message', ({ event, data }) => {
        if (event === 'open') {
          this.readyState = this.OPEN
          this.onopen?.()
        } else if (event === 'error') {
          this.readyState = null
          this.onerror?.(data)
        } else if (event === 'close') {
          this.readyState = null
          this.onclose?.()
        } else if (event === 'data') {
          this.onmessage?.({ data: Buffer.from(data) })
        }
      })

    this.OPEN = 'OPEN'

    this.onopen = null
    this.onerror = null
    this.onclose = null
    this.onmessage = null
    this.readyState = null
  }

  send(data) {
    // TODO (perf): Transfer Buffer?
    this._worker.postMessage(data)
  }

  close() {
    this._worker.terminate()
  }
}

module.exports = Socket
