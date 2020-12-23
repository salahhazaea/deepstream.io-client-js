module.exports = {
  heartbeatInterval: 60e3,
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxPacketSize: 256 * 1024,
  cacheFilter: (name, version, data) => {
    return /^[^{]/.test(name) && /^[^0]/.test(version)
  },
  cacheDb: null,
  lz: null,
  schedule: null,
  cacheSize: 1024,
  logger: null,
  path: '/deepstream'
}
