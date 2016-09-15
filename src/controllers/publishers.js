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
   GET /v1/publishers/txt
 */

v1.txt =
{ handler: function (runtime) {
  return async function (request, reply) {
    var value
    var btcAddress = request.query.btcAddress
    var hmacSecret = request.query.hmacSecret

    value = crypto.createHmac('sha384', new Buffer(hmacSecret, 'hex').toString('binary')).update(btcAddress).digest('hex')

    reply('. IN TXT "' + prefix + value + '"')
  }
},

  description: 'Returns a TXT record',
  tags: [ 'api' ],

  validate:
    { query:
      { btcAddress: braveJoi.string().base58().required().description('BTC address'),
        hmacSecret: Joi.string().hex().required().description('secret used to initialize SHA-348 HMAC')
      }
    },

  response:
    { schema: Joi.string() }
}

/*
   GET /v1/publishers/hmac
 */

v1.read_hmac =
{ handler: function (runtime) {
  return async function (request, reply) {
    var entry
    var publisher = request.query.publisher
    var debug = braveHapi.debug(module, request)
    var publishers = runtime.db.get('publishers', debug)

    entry = await publishers.findOne({ publisher: publisher })
    if (!entry) return reply(boom.notFound('no such publisher: ' + publisher))

    reply(entry.hmacSecret)
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns the hmacSecret for a publisher',
  tags: [ 'api' ],

  validate:
    { query: { publisher: braveJoi.string().publisher().required() } },

  response:
    { schema: Joi.string().hex() }
}

/*
   POST /v1/publishers/hmac
 */

v1.write_hmac =
{ handler: function (runtime) {
  return async function (request, reply) {
    var result, state
    var publisher = request.query.publisher
    var debug = braveHapi.debug(module, request)
    var publishers = runtime.db.get('publishers', debug)

    result = crypto.randomBytes(16).toString('hex')
    state = { $currentDate: { timestamp: { $type: 'timestamp' } },
              $set: { hmacSecret: result }
            }
    await publishers.update({ publisher: publisher }, state, { upsert: true })

    reply(result)
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Creates the hmacSecret for a publisher',
  tags: [ 'api' ],

  validate:
    { query: { publisher: braveJoi.string().publisher().required() } },

  response:
    { schema: Joi.string().hex() }
}

/*
   POST /v1/publishers/prune
 */

v1.prune =
{ handler: function (runtime) {
  return async function (request, reply) {
    var results, state, votes
    var debug = braveHapi.debug(module, request)
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

    reply(results)
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
    { schema: Joi.array().min(0) }
}

/*
   GET /v1/publishers/verify
 */

var dnsTxtResolver = async function (domain) {
  return new Promise((resolve, reject) => {
    dns.resolveTxt(domain, (err, rrset) => {
      if (err) return reject(err)
      resolve(rrset)
    })
  })
}

v1.verify =
{ handler: function (runtime) {
  return async function (request, reply) {
    var entry, i, result, rr, rrset, value
    var btcAddress = request.query.btcAddress
    var hmacSecret = request.query.hmacSecret
    var publisher = request.query.publisher
    var debug = braveHapi.debug(module, request)
    var publishers = runtime.db.get('publishers', debug)

    entry = await publishers.findOne({ publisher: publisher })
    if (!entry) return reply(boom.notFound('no such publisher: ' + publisher))

    if (!hmacSecret) hmacSecret = entry.hmacSecret
    value = crypto.createHmac('sha384', new Buffer(hmacSecret, 'hex').toString('binary')).update(btcAddress).digest('hex')

    try { rrset = await dnsTxtResolver(publisher) } catch (ex) { return reply({ status: 'failure', reason: ex.toString() }) }

    result = { status: 'failure' }
    for (i = 0; i < rrset.length; i++) {
      rr = rrset[i].join('')

      if (rr.indexOf(prefix) !== 0) {
        if (!result.reason) result.reason = 'no TXT RRs starting with ' + prefix
        continue
      }

      if (rr.substring(prefix.length) !== value) {
        result.reason = 'TXT RR suffix mismatch'
        continue
      }

      result.status = 'success'
      break
    }
    if ((result.status === 'failure') && (!result.reason)) result.reason = 'no TXT RRs'

    return reply(result)
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Verifies the TXT record for a publisher',
  tags: [ 'api' ],

  validate:
    { query:
      { publisher: braveJoi.string().publisher().required(),
        btcAddress: braveJoi.string().base58().required().description('BTC address'),
        hmacSecret: Joi.string().hex().optional().description('secret used to initialize SHA-348 HMAC')
      }
    },

  response:
    { schema: Joi.object().keys(
      { status: Joi.string().valid('success', 'failure').required().description('victory is mine!'),
        reason: Joi.string().optional().description('reason for failure')
      })
    }
}

module.exports.routes = [
  braveHapi.routes.async().path('/v1/publishers/hmac').config(v1.read_hmac),
  braveHapi.routes.async().post().path('/v1/publishers/hmac').config(v1.write_hmac),
  braveHapi.routes.async().post().path('/v1/publishers/prune').config(v1.prune),
  braveHapi.routes.async().path('/v1/publishers/txt').config(v1.txt),
  braveHapi.routes.async().path('/v1/publishers/verify').config(v1.verify)
]

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('publishers', debug),
      name: 'publishers',
      property: 'publisher',
      empty: { publisher: '', address: '', hmacSecret: '', timestamp: bson.Timestamp.ZERO },
      unique: [ { publisher: 0 } ],
      others: [ { address: 0 }, { hmacSecret: 1 }, { paymentStamp: 1 }, { timestamp: 1 } ]
    }
  ])
}
