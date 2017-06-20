/*
1. when contribution document is entered
  - find newest populates document with same paymentId and a holdSatoshis >= satoshis
  - populates.holdSatoshis -= satoshis
  - contribution.populatesId = populates.transactionId

2. do not collate any contribution with a non-empty populatesId

3. regularly search for populates documents where holdUntil is in the past:
     remove populatesId from any contribution where contribution.populatesId = populates.transactionId
     populates.holdSatoshis = 0

4. populates.
 */

var braveHapi = require('../brave-hapi')
var bson = require('bson')
var create = require('./reports').create
var underscore = require('underscore')
var url = require('url')
var uuid = require('uuid')

var exports = {}

var oneHundredTwentyFourDays = 124 * 24 * 60 * 60 * 1000
exports.oneHundredTwentyFourDays = oneHundredTwentyFourDays

exports.initialize = async (debug, runtime) => {
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
        status: '',
        eventId: '',
        amount: '',
        fiatFee: '',
        currency: '',
        satoshis: 0,
        fee: 0,
        holdUntil: bson.Timestamp.ZERO,
        holdSatoshis: 0,
        timestamp: bson.Timestamp.ZERO
      },
      unique: [ { transactionId: 1 } ],
      others: [ { hash: 1 }, { paymentId: 1 }, { address: 1 }, { actor: 1 }, { status: 1 }, { eventId: 1 }, { amount: 1 },
                { fiatFee: 1 }, { currency: 1 }, { satoshis: 1 }, { fee: 1 }, { holdUntil: 1 }, { holdSatoshis: 1 },
                { timestamp: 1 } ]
    }
  ])
}

exports.workers = {
/* sent by ledger PUT /v1/address/{address}/validate

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
    async (debug, runtime, payload) => {
      var entry, file, reportURL, result, state, wallet
      var address = payload.address
      var satoshis = payload.satoshis
      var transactionId = payload.transactionId
      var now = underscore.now()
      var reportId = uuid.v4().toLowerCase()
      var populates = runtime.db.get('populates', debug)
      var wallets = runtime.db.get('wallets', debug)

      wallet = await wallets.findOne({ address: address })
      if (!wallet) throw new Error('no such wallet address: ' + address)

      if (runtime.wallet.transferP.bind(runtime.wallet)(wallet)) {
        try {
          payload.fiatFee = payload.fee
          result = await runtime.wallet.transfer.bind(runtime.wallet)(wallet, satoshis)
          state = {
            $currentDate: { timestamp: { $type: 'timestamp' } },
            $set: underscore.defaults(underscore.pick(result, [ 'hash', 'fee' ]), {
              holdUntil: new Date(now + oneHundredTwentyFourDays),
              holdSatoshis: payload.satoshis
            }, underscore.omit(payload, [ 'transactionId' ]))
          }
          await populates.update({ address: address, transactionId: transactionId }, state, { upsert: true })

          entry = underscore.extend(underscore.omit(payload, [ 'address' ]), {
            subject: 'Brave Payments Transaction Confirmation',
            trackingURL: 'https://blockchain.info/tx/' + result.hash
          })
          notify(debug, runtime, address, 'purchase_completed', entry)
          return runtime.notify(debug, {
            channel: '#funding-bot',
            text: 'purchase completed: ' + JSON.stringify(underscore.extend(entry, underscore.pick(result, [ 'remaining' ])))
          })
        } catch (ex) {
          runtime.notify(debug, { text: 'populates error: ' + ex.toString() })
          debug('populates', ex)
        }
      } else {
        runtime.notify(debug, { channel: '#funding-bot', text: 'not configured for automatic funding' })
      }

      file = await create(runtime, 'population-', { format: 'json', reportId: reportId })
      underscore.extend(payload, { BTC: (payload.satoshis / 1e8).toFixed(8) })
      await file.write(JSON.stringify([ payload ], null, 2), true)

      reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
      runtime.notify(debug, { channel: '#funding-bot', text: reportURL })
    },

/* sent by ledger PATCH /v1/address/{address}/{transactionId}

    { queue            : 'population-update'
    , message          :
      { paymentId      : '...'
      , address        : '...'
      , transactionId  : '...'
      , status         : 'failed' | 'refunded' | 'disputed' | 'closed'
      , actor          : 'webhook.stripe'
      , eventId        : '...'
      }
    }
 */
  'population-update':
    async (debug, runtime, payload) => {
      var entry, state
      var address = payload.address
      var eventId = payload.eventId
      var status = payload.status
      var transactionId = payload.transactionId
      var populates = runtime.db.get('populates', debug)

      entry = await populates.findOne({ address: address, transactionId: transactionId })
      if (!entry) {
        runtime.notify(debug, { text: 'no such transaction: ' + JSON.stringify(payload) })
        return debug('population', payload)
      }

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
        $set: { status: status, eventId: eventId, holdSatoshis: 0 }
      }
      await populates.update({ address: address, transactionId: transactionId }, state, { upsert: true })

      runtime.notify(debug, {
        channel: '#funding-bot',
        text: 'purchase ' + status + ':' + JSON.stringify(underscore.defaults(payload, entry))
      })
    }
}

var notify = async (debug, runtime, address, type, payload) => {
  var result

  debug('notify', { address: address, type: type, payload: payload })
  try {
    result = await braveHapi.wreck.post(runtime.config.funding.url + '/v1/notifications/' + encodeURIComponent(address) +
                                        '?type=' + type,
      {
        headers: { authorization: 'Bearer ' + runtime.config.funding.access_token, 'content-type': 'application/json' },
        payload: JSON.stringify(payload),
        useProxyP: true
      })
    if (Buffer.isBuffer(result)) try { result = JSON.parse(result) } catch (ex) { result = result.toString() }
    debug('debug', { address: address, reason: result })
  } catch (ex) {
    debug('debug', 'notify error: ' + JSON.stringify({ address: address, reason: ex.toString() }))
  }

  if (!result) return

  result = underscore.extend({ address: address }, payload)
  debug('notify', result)
}
exports.notify = notify

var holdover = async (debug, runtime, paymentId, satoshis) => {
  var entry
  var populates = runtime.db.get('populates', debug)

  entry = await populates.findOneAndUpdate({
    $and: [
      { paymentId: paymentId },
      { holdSatoshis: { $exists: true, $ge: satoshis } }
    ]
  }, {
    $currentDate: { timestamp: { $type: 'timestamp' } },
    $inc: { holdSatoshis: -satoshis }
  }, { sort: { $_id: -1 }, upsert: false })

  if (entry) return entry.transactionId
}
exports.holdover = holdover

module.exports = exports
