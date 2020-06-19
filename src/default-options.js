module.exports = {
  heartbeatInterval: 30000,
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxMessagesPerPacket: 128,
  sendDelay: 5,
  syncDelay: 5,
  maxIdleTime: 500,
  cacheFilter: (name, version, data) => {
    return /^[^{]/.test(name) && /^[^0]/.test(version)
  },
  cacheDb: null,
  lz: null,
  schedule: null,
  cacheSize: 1024,
  logger: null,
  path: '/deepstream',
}
