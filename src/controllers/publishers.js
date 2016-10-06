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

    return reply({ token: token })
  }
},

  description: 'Gets a verification token for a publisher',
  tags: [ 'api' ],

  validate:
    { params:
      { publisher: braveJoi.string().publisher().required().description('the publisher identity'),
        verificationId: Joi.string().guid().required().description('identity of the requestor')
      }
    },

  response:
    { schema: Joi.object().keys({ token: Joi.string().hex().length(64).required().description('verification token') }) }
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

v1.verifyToken =
{ handler: function (runtime) {
  return async function (request, reply) {
    var data, entry, entries, i, info, matchP, rr, rrset
    var publisher = request.params.publisher
    var debug = braveHapi.debug(module, request)
    var tokens = runtime.db.get('tokens', debug)

    var verified = async function (entry) {
      var state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                    $set: { verified: true }
                  }

      await tokens.update(underscore.pick(entry, [ 'publisher', 'verificationId' ]), state, { upsert: true })

      return reply({ status: 'success', verificationId: entry.verificationId })
    }

    entries = await tokens.find({ publisher: publisher })
    if (entries.length === 0) return reply(boom.notFound('no such publisher: ' + publisher))

    for (i = 0; i < entries.length; i++) {
      entry = entries[i]
      if (entry.verified) return reply({ status: 'success', verificationId: entry.verificationId })
    }

    try { rrset = await dnsTxtResolver(publisher) } catch (ex) { return reply({ status: 'failure', reason: ex.toString() }) }
    for (i = 0; i < rrset.length; i++) { rrset[i] = rrset[i].join('') }
//  console.log(JSON.stringify(rrset, null, 2))

    info = { publisher: publisher }
    for (i = 0; i < entries.length; i++) {
      entry = entries[i]
      info.verificationId = entry.verificationId

      for (i = 0; i < rrset.length; i++) {
        rr = rrset[i]
        if (rr.indexOf(prefix) !== 0) continue

        matchP = true
        if (rr.substring(prefix.length) !== entry.token) {
          debug('verify', underscore.extend(info, { reason: 'TXT RR suffix mismatch ' + prefix + entry.token }))
          continue
        }

        return await verified(entry)
      }
      if (!matchP) debug('verify', underscore.extend(info, { reason: 'no TXT RRs starting with ' + prefix }))

      try {
        data = await braveHapi.wreck.get('http://' + publisher + '/.well-known/brave-payments-verification.txt')
        if (data.toString().indexOf(entry.token) !== -1) return await verified(entry)

        debug('verify', underscore.extend(info, { reason: 'data mismatch' }))
      } catch (ex) {
        debug('verify', underscore.extend(info, { reason: ex.toString() }))
      }
    }

    return reply({ status: 'failure' })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
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

module.exports.routes = [
  braveHapi.routes.async().post().path('/v1/publishers/prune').config(v1.prune),
  braveHapi.routes.async().path('/v1/publishers/{publisher}/verifications/{verificationId}').config(v1.getToken),
  braveHapi.routes.async().path('/v1/publishers/{publisher}/verify').config(v1.verifyToken)
]

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('publishers', debug),
      name: 'publishers',
      property: 'publisher',
      empty: { publisher: '', verified: false, address: '', token: '', timestamp: bson.Timestamp.ZERO },
      unique: [ { publisher: 0 } ],
      others: [ { verified: 1 }, { address: 0 }, { token: 0 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('tokens', debug),
      name: 'tokens',
      property: 'verificationId_0_publisher',
      empty: { verificationId: '', publisher: '', token: '', verified: false, timestamp: bson.Timestamp.ZERO },
      unique: [ { verificationId: 0, publisher: 1 } ],
      others: [ { token: 0 }, { verified: 1 }, { timestamp: 1 } ]
    }
  ])
}
