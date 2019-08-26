function LZ () {
}

// Node 12+
if (process && process.version && /v(\d\d\d+|\d[2-9]|[2-9]\d)/.test(process.version)) {
  const {
    Worker, isMainThread, parentPort
  } = require('worker_threads');

  if (isMainThread) {
      const callbacks = []
      const worker = new Worker(__filename);
      worker.on('message', msg => {
        const cb = callbacks.shift()
        const op = msg.charAt(0)
        const str = msg.slice(1)
        try {
          if (op === 'C') {
            cb(str)
          } else if (op === 'D') {
            cb(JSON.parse(str))
          } else if (op === 'E') {
            throw new Error(str)
          } else {
            throw new Error('invalid worker reply')
          }
        } catch (err) {
          cb(null, err)
        }
      })

      LZ.prototype.compress = function (compress, cb) {
        callbacks.push(cb)
        worker.postMessage('C' + JSON.stringify(compress))
      }

      LZ.prototype.decompress = function (decompress, cb) {
        if (typeof decompress !== 'string') {
          cb(decompress)
        } else {
          callbacks.push(cb)
          worker.postMessage('D' + decompress)
        }
      }
  } else {
    const lz = require('@nxtedition/lz-string')
    parentPort.on('message', msg => {
      var op = msg.charAt(0)
      var str = msg.slice(1)
      try {
        if (op === 'C') {
          parentPort.postMessage('C' + lz.compressToUTF16(str))
        } else if (op === 'D') {
          parentPort.postMessage('D' + lz.decompressFromUTF16(str))
        }
      } catch (err) {
        parentPort.postMessage('E' + err.message)
      }
    })
  }
} else {
  const lz = require('@nxtedition/lz-string')

  LZ.prototype.compress = function (obj, cb) {
    try {
      cb(lz.compressToUTF16(JSON.stringify(obj)))
    } catch (err) {
      cb(null, err)
    }
  }

  LZ.prototype.decompress = function (raw, cb) {
    try {
      cb(typeof raw === 'string' ? JSON.parse(lz.decompressFromUTF16(raw)) : raw)
    } catch (err) {
      cb(null, err)
    }
  }
}

module.exports = LZ
