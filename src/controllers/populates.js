var braveHapi = require('../brave-hapi')
var braveJoi = require('../brave-joi')
var bson = require('bson')
var Joi = require('joi')
var populatew = require('../workers/populates')
var underscore = require('underscore')

var v1 = {}

/*
   POST /v1/populates/{hash}
 */

v1.populates = {
  handler: (runtime) => {
    return async function (request, reply) {
      var entry, i, state
      var now = underscore.now()
      var hash = request.params.hash
      var payload = request.payload
      var debug = braveHapi.debug(module, request)
      var populates = runtime.db.get('populates', debug)

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
        $set: { hash: hash, holdUntil: new Date(now + populatew.oneHundredTwentyFourDays) }
      }
      for (i = 0; i < payload.length; i++) {
        entry = payload[i]

        underscore.extend(state.$set, underscore.pick(entry, [ 'address', 'satoshis' ]))
        await populates.update({ transactionId: entry.transactionId }, state, { upsert: true })

        entry.subject = 'Brave Payments Transaction Confirmation'
        entry.trackingURL = 'https://blockchain.info/tx/' + hash

        populatew.notify(debug, runtime, entry.address, 'purchase_completed', underscore.omit(entry, [ 'address' ]))
      }

      reply({})
    }
  },

  auth: {
    strategy: 'session',
    scope: [ 'ledger' ],
    mode: 'required'
  },

  description: 'Posts a "populates" for one or more wallets',
  tags: [ 'api' ],

  validate: {
    params: { hash: Joi.string().hex().required().description('transaction hash') },
    payload: Joi.array().min(1).items(Joi.object().keys({
      address: braveJoi.string().base58().required().description('BTC address'),
      satoshis: Joi.number().integer().min(1).required().description('the settlement in satoshis'),
      transactionId: Joi.string().required().description('the transactionId')
    }).unknown(true)).required().description('wallet population report')
  },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

module.exports.routes = [
  braveHapi.routes.async().post().path('/v1/populates/{hash}').config(v1.populates)
]

module.exports.initialize = async function (debug, runtime) {
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
        currency: '',
        satoshis: 0,
        holdUntil: bson.Timestamp.ZERO,
        timestamp: bson.Timestamp.ZERO
      },
      unique: [ { transactionId: 1 } ],
      others: [ { hash: 1 }, { paymentId: 1 }, { address: 1 }, { actor: 1 }, { amount: 1 }, { currency: 1 }, { satoshis: 1 },
                { holdUntil: 1 }, { timestamp: 1 } ]
    }
  ])
}
