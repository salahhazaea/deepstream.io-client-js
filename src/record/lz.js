const lz = require('@nxtedition/lz-string')

module.exports = function () {
  this.compress = function compress (obj, cb) {
    try {
      cb(lz.compressToUTF16(JSON.stringify(obj)))
    } catch (err) {
      cb(null)
    }
  }

  this.decompress = function decompress (raw, cb) {
    try {
      cb(typeof raw === 'string' ? JSON.parse(lz.decompressFromUTF16(raw)) : raw)
    } catch (err) {
      cb(null)
    }
  }
}
