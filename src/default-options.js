module.exports = {
  heartbeatInterval: 30000,
  reconnectIntervalIncrement: 4000,
  maxReconnectInterval: 120000,
  maxReconnectAttempts: Infinity,
  maxMessagesPerPacket: 128,
  sendDelay: 5,
  cacheSize: 512,
  maxIdleTime: 500,
  path: '/deepstream',
  cacheDb: null
}
