module.exports = {
  heartbeatInterval: 30000,
  reconnectIntervalIncrement: 4000,
  maxReconnectInterval: 120000,
  maxReconnectAttempts: Infinity,
  maxMessagesPerPacket: 128,
  sendDelay: 5,
  syncDelay: 5,
  maxIdleTime: 500,
  cacheFilter: (name, version, data) => {
    return /^[^{]/.test(name) && /^[^0]/.test(version)
  },
  cacheDb: null,
  cacheSize: 1024,
  lz: null,
  path: '/deepstream',
  compat: false
}
