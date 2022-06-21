module.exports = {
  heartbeatInterval: 60e3,
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxPacketSize: 512 * 1024,
  batchSize: 1024,
  schedule: null,
  logger: null,
  cache: null,
}
