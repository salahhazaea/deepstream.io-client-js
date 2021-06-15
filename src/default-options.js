module.exports = {
  heartbeatInterval: 60e3,
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxPacketSize: 256 * 1024,
  schedule: null,
  logger: null,
}
