module.exports = {
  heartbeatInterval: 60e3,
  reconnectIntervalIncrement: 1e3,
  maxReconnectInterval: 6e3,
  maxReconnectAttempts: Infinity,
  maxPacketSize: 512 * 1024,
  defaultReadTimeout: 2 * 60e3,
  schedule: null,
  logger: null,
}
