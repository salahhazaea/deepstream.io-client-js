'use strict'

module.exports = {
  heartbeatInterval: 30000,
  reconnectIntervalIncrement: 4000,
  maxReconnectInterval: 180000,
  maxReconnectAttempts: 5,
  maxMessagesPerPacket: 128,
  sendDelay: 10,
  maxIdleTime: 500,
  path: '/deepstream'
}
