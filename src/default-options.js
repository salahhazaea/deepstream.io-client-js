module.exports = {
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxPacketSize: 512 * 1024,
  batchSize: 4096,
  schedule: null,
  logger: null,
}
