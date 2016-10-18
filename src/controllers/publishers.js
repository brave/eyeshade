var boom = require('boom')
var braveHapi = require('../brave-hapi')
var braveJoi = require('../brave-joi')
var bson = require('bson')
var crypto = require('crypto')
var dns = require('dns')
var Joi = require('joi')
var ledgerPublisher = require('ledger-publisher')
var underscore = require('underscore')

var v1 = {}
var prefix = 'brave-ledger-verification='

/*
   POST /v1/publishers/prune
 */

var pruner = async function (debug, runtime) {
  var results, state, votes
  var tokens = runtime.db.get('tokens', debug)
  var voting = runtime.db.get('voting', debug)

  votes = await voting.aggregate([
      { $match: { counts: { $gt: 0 },
                  exclude: false
                }
      },
      { $group: { _id: '$publisher' } },
      { $project: { _id: 1 } }
  ])

  state = { $currentDate: { timestamp: { $type: 'timestamp' } },
            $set: { exclude: true }
          }

  results = []
  votes.forEach(async function (entry) {
    var publisher = entry._id
    var result

    try {
      result = ledgerPublisher.getPublisher('https://' + publisher)
      if (result) return
    } catch (err) {
      return debug('prune', underscore.defaults({ publisher: publisher }, err))
    }

    results.push(publisher)
    await voting.update({ publisher: publisher }, state, { upsert: false, multi: true })
  })

  runtime.notify(debug, { text: 'pruned ' + JSON.stringify(results, null, 2) })

  debug('begin', {})
  tokens.find({ verified: true }).forEach(async function (entry) {
    try {
      await braveHapi.wreck.patch(runtime.config.ledger.url + '/v1/publisher/identity',
                                  { headers: { authorization: 'bearer ' + runtime.config.ledger.access_token },
                                    payload: JSON.stringify({ publisher: entry.publisher, verified: true })
                                  })
    } catch (ex) {
      debug('prune', underscore.extend(entry, { reason: ex.toString() }))
    }
  })
  debug('done.', {})
}

v1.prune =
{ handler: function (runtime) {
  return async function (request, reply) {
    var debug = braveHapi.debug(module, request)

    pruner(debug, runtime)
    reply({})
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Prunes votes corresponding to pruned publishers',
  tags: [ 'api' ],

  validate:
    { query: {} },

  response:
    { schema: Joi.object().length(0) }
}

/*
   GET /v1/publishers/{publisher}/balance
 */

v1.getBalance =
{ handler: function (runtime) {
  return async function (request, reply) {
    var amount, rate, satoshis, summary
    var publisher = request.params.publisher
    var currency = request.query.currency
    var debug = braveHapi.debug(module, request)
    var voting = runtime.db.get('voting', debug)

    summary = await voting.aggregate([
      { $match:
        { satoshis: { $gt: 0 },
          publisher: { $eq: publisher },
          exclude: false
        }
      },
      { $group:
        { _id: '$publisher',
          satoshis: { $sum: '$satoshis' }
        }
      }
    ])
    satoshis = summary.length > 0 ? summary[0].satoshis : 0

    rate = runtime.wallet.rates[currency.toUpperCase()]
    // TBD: assumes 2 decimals
    if (rate) amount = ((satoshis * rate) / 1e8).toFixed(2)
    reply({ amount: amount, currency: currency, satoshis: satoshis })
  }
},

  auth:
    { strategy: 'simple',
      mode: 'required'
    },

  description: 'Gets a verification token for a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') },
      query: { currency: braveJoi.string().currencyCode().optional().default('USD').description('the payment currency'),
               access_token: Joi.string().guid().optional() }
    },

  response:
    { schema: Joi.object().keys(
      { amount: Joi.number().min(0).optional().description('the balance in the payment currency'),
        currency: braveJoi.string().currencyCode().optional().default('USD').description('the payment currency'),
        satoshis: Joi.number().integer().min(0).optional().description('the balance in satoshis')
      })
    }
}

/*
   GET /v1/publishers/{publisher}/verifications/{verificationId}
 */

v1.getToken =
{ handler: function (runtime) {
  return async function (request, reply) {
    var entry, state, token
    var publisher = request.params.publisher
    var verificationId = request.params.verificationId
    var debug = braveHapi.debug(module, request)
    var tokens = runtime.db.get('tokens', debug)

    entry = await tokens.findOne({ verificationId: verificationId, publisher: publisher })
    if (entry) return reply({ token: entry.token })

    token = crypto.randomBytes(32).toString('hex')
    state = { $currentDate: { timestamp: { $type: 'timestamp' } },
              $set: { token: token }
            }
    await tokens.update({ verificationId: verificationId, publisher: publisher }, state, { upsert: true })

    reply({ token: token })
  }
},

  auth:
    { strategy: 'simple',
      mode: 'required'
    },

  description: 'Gets a verification token for a publisher',
  tags: [ 'api' ],

  validate:
    { params:
      { publisher: braveJoi.string().publisher().required().description('the publisher identity'),
        verificationId: Joi.string().guid().required().description('identity of the requestor')
      },
      query: { access_token: Joi.string().guid().optional() }
    },

  response:
    { schema: Joi.object().keys({ token: Joi.string().hex().length(64).required().description('verification token') }) }
}

/*
   PUT /v1/publishers/{publisher}/wallet
 */

v1.setWallet =
{ handler: function (runtime) {
  return async function (request, reply) {
    var entry, state
    var publisher = request.params.publisher
    var bitcoinAddress = request.payload.bitcoinAddress
    var verificationId = request.payload.verificationId
    var debug = braveHapi.debug(module, request)
    var publishers = runtime.db.get('publishers', debug)
    var tokens = runtime.db.get('tokens', debug)

    entry = await tokens.findOne({ verificationId: verificationId, publisher: publisher })
    if (!entry) return reply(boom.notFound('no such entry: ' + publisher))

    if (!entry.verified) return reply(boom.badData('not verified: ' + publisher + ' using ' + verificationId))

    state = { $currentDate: { timestamp: { $type: 'timestamp' } },
              $set: { address: bitcoinAddress }
            }
    await publishers.update({ publisher: publisher }, state, { upsert: true })

    reply({})
  }
},

  auth:
    { strategy: 'simple',
      mode: 'required'
    },

  description: 'Sets the bitcoin address for a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') },
      query: { access_token: Joi.string().guid().optional() },
      payload: { bitcoinAddress: braveJoi.string().base58().required().description('BTC address'),
                 verificationId: Joi.string().guid().required().description('identity of the requestor')
               }
    },

  response:
    { schema: Joi.object().length(0) }
}

/*
   GET /v1/publishers/{publisher}/verify
 */

var dnsTxtResolver = async function (domain) {
  return new Promise((resolve, reject) => {
    dns.resolveTxt(domain, (err, rrset) => {
      if (err) return reject(err)
      resolve(rrset)
    })
  })
}

var verified = async function (request, reply, runtime, entry, verified, reason) {
  var payload, state
  var indices = underscore.pick(entry, [ 'verificationId', 'publisher' ])
  var debug = braveHapi.debug(module, request)
  var tokens = runtime.db.get('tokens', debug)

  debug('verified', underscore.extend(underscore.clone(indices), { verified: verified, reason: reason }))

  entry.verified = verified
  state = { $currentDate: { timestamp: { $type: 'timestamp' } },
            $set: { verified: entry.verified, reason: reason }
          }
  await tokens.update(indices, state, { upsert: true })

  reason = reason || (verified ? 'ok' : 'unknown')
  payload = underscore.extend(underscore.pick(entry, [ 'verificationId', 'token', 'verified' ]), { status: reason })
  try {
    await braveHapi.wreck.patch(runtime.config.publishers.url + '/v1/publishers/' + encodeURIComponent(entry.publisher) +
                                  '/verifications',
                                { payload: JSON.stringify(payload) })
  } catch (ex) {
    debug('publishers patch', underscore.extend(indices, { payload: payload, reason: ex.toString() }))
  }
  if (!verified) return

  try {
    await braveHapi.wreck.patch(runtime.config.ledger.url + '/v1/publisher/identity',
                                { headers: { authorization: 'bearer ' + runtime.config.ledger.access_token },
                                  payload: JSON.stringify({ publisher: entry.publisher, verified: true })
                                })
  } catch (ex) {
    debug('ledger patch', underscore.extend(indices, { payload: payload, reason: ex.toString() }))
  }

  reply({ status: 'success', verificationId: entry.verificationId })
}

v1.verifyToken =
{ handler: function (runtime) {
  return async function (request, reply) {
    var data, entry, entries, i, info, j, matchP, reason, rr, rrset
    var publisher = request.params.publisher
    var debug = braveHapi.debug(module, request)
    var tokens = runtime.db.get('tokens', debug)

    entries = await tokens.find({ publisher: publisher })
    if (entries.length === 0) return reply(boom.notFound('no such publisher: ' + publisher))

    for (i = 0; i < entries.length; i++) {
      entry = entries[i]
      if (entry.verified) return reply({ status: 'success', verificationId: entry.verificationId })
    }

    try { rrset = await dnsTxtResolver(publisher) } catch (ex) {
      reason = ex.toString()
      if (reason.indexOf('ENODATA') === -1) {
        debug('dnsTxtResolver', underscore.extend({ publisher: publisher, reason: reason }))
      }
      rrset = []
    }
    for (i = 0; i < rrset.length; i++) { rrset[i] = rrset[i].join('') }

    var loser = async function (reason) {
      debug('verify', underscore.extend(info, { reason: reason }))
      await verified(request, reply, runtime, entry, false, reason)
    }

    info = { publisher: publisher }
    for (i = 0; i < entries.length; i++) {
      entry = entries[i]
      info.verificationId = entry.verificationId

      for (j = 0; j < rrset.length; j++) {
        rr = rrset[j]
        if (rr.indexOf(prefix) !== 0) continue

        matchP = true
        if (rr.substring(prefix.length) !== entry.token) {
          await loser('TXT RR suffix mismatch ' + prefix + entry.token)
          continue
        }

        return await verified(request, reply, runtime, entry, true, 'TXT RR matches')
      }
      if (!matchP) await loser('no TXT RRs starting with ' + prefix)

      try {
        data = await braveHapi.wreck.get('http://' + publisher + '/.well-known/brave-payments-verification.txt')
        if (data.toString().indexOf(entry.token) !== -1) {
          return await verified(request, reply, runtime, entry, true, 'web file matches')
        }

        try {
          data = await braveHapi.wreck.get('https://' + publisher + '/.well-known/brave-payments-verification.txt')
          if (data.toString().indexOf(entry.token) !== -1) {
            return await verified(request, reply, runtime, entry, true, 'web file matches')
          }
        } catch (ex) {}
        await loser('data mismatch')
      } catch (ex) {
        await loser(ex.toString())
      }
    }

    return reply({ status: 'failure' })
  }
},

  description: 'Verifies a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') } },

  response:
    { schema: Joi.object().keys(
      { status: Joi.string().valid('success', 'failure').required().description('victory is mine!'),
        verificationId: Joi.string().guid().optional().description('identity of the verified requestor')
      })
    }
}

module.exports.notify =
  async function (debug, runtime, publisher, payload) {
// TBD: add some logging here for testing...

    await braveHapi.wreck.post(runtime.config.publishers.url + '/v1/publishers/' + encodeURIComponent(publisher) +
                                 '/notifications',
                               { payload: JSON.stringify(payload) })
  }

module.exports.routes = [
  braveHapi.routes.async().post().path('/v1/publishers/prune').config(v1.prune),
  braveHapi.routes.async().post().path('/v1/publishers/{publisher}/balance').whitelist().config(v1.getBalance),
  braveHapi.routes.async().path('/v1/publishers/{publisher}/verifications/{verificationId}').whitelist().config(v1.getToken),
  braveHapi.routes.async().put().path('/v1/publishers/{publisher}/wallet').whitelist().config(v1.setWallet),
  braveHapi.routes.async().path('/v1/publishers/{publisher}/verify').config(v1.verifyToken)
]

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('publishers', debug),
      name: 'publishers',
      property: 'publisher',
      empty: { publisher: '', verified: false, address: '', token: '', timestamp: bson.Timestamp.ZERO },
      unique: [ { publisher: 1 } ],
      others: [ { verified: 1 }, { address: 0 }, { token: 0 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('tokens', debug),
      name: 'tokens',
      property: 'verificationId_0_publisher',
      empty: { verificationId: '', publisher: '', token: '', verified: false, reason: '', timestamp: bson.Timestamp.ZERO },
      unique: [ { verificationId: 0, publisher: 1 } ],
      others: [ { token: 0 }, { verified: 1 }, { reason: 1 }, { timestamp: 1 } ]
    }
  ])
}
