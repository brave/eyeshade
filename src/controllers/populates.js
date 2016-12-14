var braveHapi = require('../brave-hapi')
var braveJoi = require('../brave-joi')
var bson = require('bson')
var Joi = require('joi')
var underscore = require('underscore')

var v1 = {}

var oneHundredTwentyFourDays = 124 * 24 * 60 * 60 * 1000

/*
   POST /v1/populates/{hash}
 */

v1.populates =
{ handler: function (runtime) {
  return async function (request, reply) {
    var entry, i, state
    var now = underscore.now()
    var hash = request.params.hash
    var payload = request.payload
    var debug = braveHapi.debug(module, request)
    var populates = runtime.db.get('populates', debug)

    state = { $currentDate: { timestamp: { $type: 'timestamp' } },
              $set: { hash: hash, holdUntil: new Date(now + oneHundredTwentyFourDays) }
            }
    for (i = 0; i < payload.length; i++) {
      entry = payload[i]

      underscore.extend(state.$set, underscore.pick(entry, [ 'address', 'satoshis' ]))
      await populates.update({ transactionId: entry.transactionId }, state, { upsert: true })

      notify(debug, runtime, entry.address, 'purchase_completed', underscore.omit(entry, [ 'address' ]))
    }

    reply({})
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Posts a "populates" for one or more wallets',
  tags: [ 'api' ],

  validate:
    { params: { hash: Joi.string().hex().required().description('transaction hash') },
      payload: Joi.array().min(1).items(Joi.object()
               .keys({
                 address: braveJoi.string().base58().required().description('BTC address'),
                 satoshis: Joi.number().integer().min(1).required().description('the settlement in satoshis'),
                 transactionId: Joi.string().required().description('the transactionId')
               }).unknown(true)).required().description('publisher settlement report')
    },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

var notify = async function (debug, runtime, address, type, payload) {
  var result

  try {
    result = await braveHapi.wreck.post(runtime.config.payments.url + '/v1/notifications/' + encodeURIComponent(address) +
                                        '?type=' + type,
                                        { headers: { authorization: 'Bearer ' + runtime.config.publishers.access_token,
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
  runtime.notify(debug, { channel: '#payments-bot', text: 'consumer notified: ' + JSON.stringify(result) })
}

module.exports.routes = [
  braveHapi.routes.async().post().path('/v1/populates/{hash}').config(v1.populates)
]

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('populates', debug),
      name: 'populates',
      property: 'transactionId',
      empty: { transactionId: '',
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
      unique: [ { transactionId: 0 } ],
      others: [ { hash: 0 }, { paymentId: 0 }, { address: 0 }, { actor: 1 }, { amount: 1 }, { currency: 1 }, { satoshis: 1 },
                { holdUntil: 1 }, { timestamp: 1 } ]
    }
  ])
}
