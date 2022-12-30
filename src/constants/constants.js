module.exports.CONNECTION_STATE = {}

module.exports.CONNECTION_STATE.CLOSED = 'CLOSED'
module.exports.CONNECTION_STATE.AWAITING_CONNECTION = 'AWAITING_CONNECTION'
module.exports.CONNECTION_STATE.CHALLENGING = 'CHALLENGING'
module.exports.CONNECTION_STATE.AWAITING_AUTHENTICATION = 'AWAITING_AUTHENTICATION'
module.exports.CONNECTION_STATE.AUTHENTICATING = 'AUTHENTICATING'
module.exports.CONNECTION_STATE.OPEN = 'OPEN'
module.exports.CONNECTION_STATE.ERROR = 'ERROR'
module.exports.CONNECTION_STATE.RECONNECTING = 'RECONNECTING'

module.exports.RECORD_STATE = {}
module.exports.RECORD_STATE.VOID = 0
module.exports.RECORD_STATE.CLIENT = 1
module.exports.RECORD_STATE.SERVER = 2
module.exports.RECORD_STATE.STALE = 3
module.exports.RECORD_STATE.PROVIDER = 4

module.exports.RECORD_STATE_NAME = []
for (const [key, val] of Object.entries(module.exports.RECORD_STATE)) {
  module.exports.RECORD_STATE_NAME[val] = key
}

module.exports.MESSAGE_SEPERATOR = String.fromCharCode(30) // ASCII Record Seperator 1E
module.exports.MESSAGE_PART_SEPERATOR = String.fromCharCode(31) // ASCII Unit Separator 1F

module.exports.TYPES = {}
module.exports.TYPES.STRING = 'S'
module.exports.TYPES.OBJECT = 'O'
module.exports.TYPES.NUMBER = 'N'
module.exports.TYPES.NULL = 'L'
module.exports.TYPES.TRUE = 'T'
module.exports.TYPES.FALSE = 'F'
module.exports.TYPES.UNDEFINED = 'U'

module.exports.TOPIC = {}
module.exports.TOPIC.CONNECTION = 'C'
module.exports.TOPIC.AUTH = 'A'
module.exports.TOPIC.ERROR = 'X'
module.exports.TOPIC.EVENT = 'E'
module.exports.TOPIC.RECORD = 'R'
module.exports.TOPIC.RPC = 'P'
module.exports.TOPIC.PRIVATE = 'PRIVATE/'

module.exports.EVENT = {}
module.exports.EVENT.CONNECTION_ERROR = 'connectionError'
module.exports.EVENT.CONNECTION_STATE_CHANGED = 'connectionStateChanged'
module.exports.EVENT.CONNECTED = 'connected'
module.exports.EVENT.MAX_RECONNECTION_ATTEMPTS_REACHED = 'MAX_RECONNECTION_ATTEMPTS_REACHED'
module.exports.EVENT.CONNECTION_AUTHENTICATION_TIMEOUT = 'CONNECTION_AUTHENTICATION_TIMEOUT'
module.exports.EVENT.NO_RPC_PROVIDER = 'NO_RPC_PROVIDER'
module.exports.EVENT.RPC_ERROR = 'RPC_ERROR'
module.exports.EVENT.TIMEOUT = 'TIMEOUT'
module.exports.EVENT.UNSOLICITED_MESSAGE = 'UNSOLICITED_MESSAGE'
module.exports.EVENT.MESSAGE_DENIED = 'MESSAGE_DENIED'
module.exports.EVENT.NOT_CONNECTED = 'NOT_CONNECTED'
module.exports.EVENT.MESSAGE_PARSE_ERROR = 'MESSAGE_PARSE_ERROR'
module.exports.EVENT.NOT_AUTHENTICATED = 'NOT_AUTHENTICATED'
module.exports.EVENT.MESSAGE_PERMISSION_ERROR = 'MESSAGE_PERMISSION_ERROR'
module.exports.EVENT.LISTENER_EXISTS = 'LISTENER_EXISTS'
module.exports.EVENT.PROVIDER_ERROR = 'PROVIDER_ERROR'
module.exports.EVENT.CACHE_ERROR = 'CACHE_ERROR'
module.exports.EVENT.UPDATE_ERROR = 'UPDATE_ERROR'
module.exports.EVENT.USER_ERROR = 'USER_ERROR'
module.exports.EVENT.REF_ERROR = 'REF_ERROR'
module.exports.EVENT.PROVIDER_EXISTS = 'PROVIDER_EXISTS'
module.exports.EVENT.NOT_LISTENING = 'NOT_LISTENING'
module.exports.EVENT.NOT_PROVIDING = 'NOT_PROVIDING'
module.exports.EVENT.LISTENER_ERROR = 'LISTENER_ERROR'
module.exports.EVENT.TOO_MANY_AUTH_ATTEMPTS = 'TOO_MANY_AUTH_ATTEMPTS'
module.exports.EVENT.IS_CLOSED = 'IS_CLOSED'
module.exports.EVENT.RECORD_NOT_FOUND = 'RECORD_NOT_FOUND'
module.exports.EVENT.NOT_SUBSCRIBED = 'NOT_SUBSCRIBED'

module.exports.ACTIONS = {}
module.exports.ACTIONS.PING = 'PI'
module.exports.ACTIONS.PONG = 'PO'
module.exports.ACTIONS.ACK = 'A'
module.exports.ACTIONS.REDIRECT = 'RED'
module.exports.ACTIONS.CHALLENGE = 'CH'
module.exports.ACTIONS.CHALLENGE_RESPONSE = 'CHR'
module.exports.ACTIONS.READ = 'R'
module.exports.ACTIONS.UPDATE = 'U'
module.exports.ACTIONS.SUBSCRIBE = 'S'
module.exports.ACTIONS.SYNC = 'SY'
module.exports.ACTIONS.UNSUBSCRIBE = 'US'
module.exports.ACTIONS.SUBSCRIPTION_FOR_PATTERN_FOUND = 'SP'
module.exports.ACTIONS.SUBSCRIPTION_FOR_PATTERN_REMOVED = 'SR'
module.exports.ACTIONS.SUBSCRIPTION_HAS_PROVIDER = 'SH'
module.exports.ACTIONS.LISTEN = 'L'
module.exports.ACTIONS.UNLISTEN = 'UL'
module.exports.ACTIONS.LISTEN_ACCEPT = 'LA'
module.exports.ACTIONS.LISTEN_REJECT = 'LR'
module.exports.ACTIONS.EVENT = 'EVT'
module.exports.ACTIONS.ERROR = 'E'
module.exports.ACTIONS.REQUEST = 'REQ'
module.exports.ACTIONS.RESPONSE = 'RES'
module.exports.ACTIONS.REJECTION = 'REJ'
