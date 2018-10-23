const lz = require('@nxtedition/lz-string')

function LZ () {
}

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

module.exports = LZ
