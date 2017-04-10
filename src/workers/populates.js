var braveHapi = require('../brave-hapi')
var bson = require('bson')
var create = require('./reports').create
var underscore = require('underscore')
var url = require('url')
var uuid = require('uuid')

var exports = {}

var oneHundredTwentyFourDays = 124 * 24 * 60 * 60 * 1000
exports.oneHundredTwentyFourDays = oneHundredTwentyFourDays

exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug, [
    {
      category: runtime.db.get('populates', debug),
      name: 'populates',
      property: 'transactionId',
      empty: {
        transactionId: '',
        hash: '',
        paymentId: '',
        address: '',
        actor: '',
        amount: '',
        fee: '',
        currency: '',
        satoshis: 0,
        holdUntil: bson.Timestamp.ZERO,
        timestamp: bson.Timestamp.ZERO
      },
      unique: [ { transactionId: 1 } ],
      others: [ { hash: 1 }, { paymentId: 1 }, { address: 1 }, { actor: 1 }, { amount: 1 }, { fee: 1 }, { currency: 1 },
                { satoshis: 1 }, { holdUntil: 1 }, { timestamp: 1 } ]
    }
  ])
}

exports.workers = {
/* sent by ledger PUT /v1/address/{personaId}/validate

    { queue            : 'population-report'
    , message          :
      { paymentId      : '...'
      , address        : '...'
      , satoshis       : 1403982
      , actor          : 'authorize.stripe'
      , transactionId  : '...'
      , amount         : 10.25
      , fee            : 0.25
      , currency       : 'USD'
      }
    }
 */
  'population-report':
    async function (debug, runtime, payload) {
      var file, reportURL, result, state, wallet
      var address = payload.address
      var satoshis = payload.satoshis
      var now = underscore.now()
      var reportId = uuid.v4().toLowerCase()
      var populates = runtime.db.get('populates', debug)
      var wallets = runtime.db.get('wallets', debug)

      wallet = wallets.findOne({ address: address })
      if (!wallet) throw new Error('no such wallet address: ' + address)

      if (runtime.wallet.transferP(wallet)) {
        try {
          result = runtime.wallet.transfer(wallet, satoshis)
          state = {
            $currentDate: { timestamp: { $type: 'timestamp' } },
            $set: underscore.defaults(result, { holdUntil: new Date(now + oneHundredTwentyFourDays) }, payload)
          }
          await populates.update(state, { upsert: true })
          runtime.notify(debug, { channel: '#funding-bot', text: JSON.stringify(payload) })
        } catch (ex) {
          runtime.notify(debug, { channel: '#funding-bot', text: ex.toString() })
        }
      } else {
        runtime.notify(debug, { channel: '#funding-bot', text: 'not configured for automatic funding' })
      }

      file = await create(runtime, 'population-', { format: 'json', reportId: reportId })
      underscore.extend(payload, { BTC: (payload.satoshis / 1e8).toFixed(8) })
      await file.write(JSON.stringify([ payload ], null, 2), true)

      reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
      runtime.notify(debug, { channel: '#funding-bot', text: reportURL })
    }
}

var notify = async function (debug, runtime, address, type, payload) {
  var result

  try {
    result = await braveHapi.wreck.post(runtime.config.funding.url + '/v1/notifications/' + encodeURIComponent(address) +
                                        '?type=' + type,
      { headers: { authorization: 'Bearer ' + runtime.config.funding.access_token,
        'content-type': 'application/json'
      },
        payload: JSON.stringify(payload),
        useProxyP: true
      })
    if (Buffer.isBuffer(result)) try { result = JSON.parse(result) } catch (ex) { result = result.toString() }
    debug('publishers', { address: address, reason: result })
  } catch (ex) {
    debug('publishers', { address: address, reason: ex.toString() })
  }

  if (!result) return

  result = underscore.extend({ address: address }, payload)
  debug('notify', result)
  runtime.notify(debug, { channel: '#funding-bot', text: 'consumer notified: ' + JSON.stringify(result) })
}
exports.notify = notify

module.exports = exports
