var querystring = require('querystring')
var underscore = require('underscore')
var wreck = require('wreck')

var DB = require('./database')
var Queue = require('./queue')
var Wallet = require('./wallet')

var profile = process.env.NODE_ENV || 'development'
var config = require('../config/config.' + profile + '.js')

underscore.keys(config).forEach((key) => {
  var m = config[key]
  if (typeof m === 'undefined') return

  underscore.keys(m).forEach((k) => {
    if (typeof m[k] === 'undefined') throw new Error('config.' + key + '.' + k + ': undefined')

    if ((typeof m[k] !== 'number') && (typeof m[k] !== 'boolean') && (!m[k])) {
      throw new Error('config.' + key + '.' + k + ': empty')
    }
  })
})

var runtime = {
  config: config,
  db: new DB(config),
  login: config.login,
  queue: new Queue(config)
}
runtime.wallet = new Wallet(config, runtime)

runtime.notify = (debug, payload) => {
  var opts
  var params = runtime.config.slack

  debug('notify', payload)
  if (!(params && params.webhook && params.channel)) return debug('notify', 'slack webhook not configured')

  underscore.defaults(payload, { channel: params.channel,
                                 username: params.username || 'webhookbot',
                                 icon_emoji: params.icon_emoji || ':ghost:',
                                 text: 'ping.' })
  opts = { headers: { 'content-type': 'application/x-www-form-url-encoded' },
           payload: querystring.stringify(payload)
         }
  wreck.post(params.webhook, opts, (err, response, body) => {
    if (err) return debug('notify', { payload: payload, reason: err.toString() })

    debug('notify', payload)
  })
}

module.exports = runtime
