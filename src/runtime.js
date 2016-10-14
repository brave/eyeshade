var Slack = require('node-slack')
var underscore = require('underscore')

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
if (runtime.config.slack && runtime.config.slack.webhook) runtime.slack = new Slack(runtime.config.slack.webhook)
runtime.wallet = new Wallet(config, runtime)

runtime.notify = (debug, payload) => {
  var params = runtime.config.slack

try {
  if (!runtime.slack) return debug('notify0', 'slack webhook not configured')
  underscore.defaults(payload, { channel: params.channel,
                                 username: params.username || 'webhookbot',
                                 icon_emoji: params.icon_emoji || ':ghost:',
                                 text: 'ping.' })
  debug('notify1', payload)
  runtime.slack(payload, (res, err, body) => {
    if (err) debug('notify2', err)
  })
} catch (ex) { debug('notify3', ex) }
}

module.exports = runtime
