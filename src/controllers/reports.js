var braveHapi = require('../brave-hapi')
var bson = require('bson')
var dateformat = require('dateformat')
var Joi = require('joi')
var json2csv = require('json2csv')
var underscore = require('underscore')

var v1 = {}
var datefmt = 'yyyy-mm-dd HH:MM:ss'

/*
   GET /v1/reports/publishers
 */

v1.publishers =
{ handler: function (runtime) {
  return async function (request, reply) {
    var data, fees, filename, i, publishers, results, satoshis, usd
    var debug = braveHapi.debug(module, request)
    var format = request.query.format || 'json'
    var summaryP = request.query.summary
    var voting = runtime.db.get('voting', debug)

    var slicer = async function (quantum) {
      var fees, i, satoshis, slice, state
      var slices = await voting.find({ surveyorId: quantum.surveyorId, exclude: false })

      for (i = 0; i < slices.length; i++) {
        slice = slices[i]

        satoshis = Math.floor(quantum.quantum * slice.counts * 0.95)
        fees = Math.floor((quantum.quantum * slice.counts) - satoshis)
        if (!publishers[slice.publisher]) publishers[slice.publisher] = { satoshis: 0, fees: 0, votes: [] }
        publishers[slice.publisher].satoshis += satoshis
        publishers[slice.publisher].fees += fees
        publishers[slice.publisher].votes.push({ surveyorId: quantum.surveyorId,
                                                 counts: slice.counts,
                                                 satoshis: satoshis,
                                                 fees: fees
                                               })
        if (slice.satoshis === satoshis) continue

        state = { $set: { satoshis: satoshis } }
        await voting.update({ surveyorId: quantum.surveyorId, publisher: slice.publisher }, state, { upsert: true })
      }
    }

    publishers = {}
    results = await quanta(debug, runtime)
    for (i = 0; i < results.length; i++) await slicer(results[i])

    results = []
    underscore.keys(publishers).forEach((publisher) => {
      publishers[publisher].votes = underscore.sortBy(publishers[publisher].votes, 'surveyorId')
      results.push(underscore.extend({ publisher: publisher }, publishers[publisher]))
    })
    results = underscore.sortBy(results, 'publisher')
    if (format !== 'csv') return reply(results)

    usd = runtime.wallet.rates.USD
    usd = (Number.isFinite(usd)) ? (usd / 1e8) : null
    satoshis = 0
    fees = 0

    data = []
    results.forEach((result) => {
      satoshis += result.satoshis
      fees += result.fees
      data.push({ publisher: result.publisher,
                  total: result.satoshis,
                  fees: result.fees,
                  'publisher USD': (result.satoshis * usd).toFixed(2),
                  'processor USD': (result.fees * usd).toFixed(2)
                })
      if (!summaryP) result.votes.forEach((vote) => { data.push(underscore.extend({ publisher: result.publisher }, vote)) })
    })
    data.push({ publisher: 'TOTAL',
                total: satoshis,
                fees: fees,
                'publisher USD': (satoshis * usd).toFixed(2),
                'processor USD': (fees * usd).toFixed(2)
              })
    filename = 'publishers-' + dateformat(underscore.now(), datefmt) + '.csv'
    reply(json2csv({ data: data })).type('text/csv').header('content-disposition', 'attachment; filename="' + filename + '"')
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns information about contributions to publishers',
  tags: [ 'api' ],

  validate:
    { query: { format: Joi.string().valid('json', 'csv').optional().default('json').description(
                         'the format of the response'
                       ),
               summary: Joi.boolean().optional().default(false).description('summarize results (CSV only)')
              } },

  response:
    { schema: Joi.alternatives().try(Joi.array().min(0).items(Joi.object().keys().unknown(true)), Joi.string()) }
}

v1.surveyors =
{ handler: function (runtime) {
  return async function (request, reply) {
    var filename
    var debug = braveHapi.debug(module, request)
    var format = request.query.format || 'json'
    var results = await quanta(debug, runtime)

    results = underscore.sortBy(results, 'created')
    if (format !== 'csv') return reply(results)

    results.forEach((result) => {
      underscore.extend(result,
                        { created: dateformat(result.created, datefmt), modified: dateformat(result.modified, datefmt) })
    })

    filename = 'surveyors-' + dateformat(underscore.now(), datefmt) + '.csv'
    reply(json2csv({ data: results })).type('text/csv').header('content-disposition', 'attachment; filename="' + filename + '"')
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns information about contribution activity',
  tags: [ 'api' ],

  validate:
    { query: { format: Joi.string().valid('json', 'csv').optional().default('json').description(
                         'the format of the response'
                       ) } },

  response:
    { schema: Joi.alternatives().try(Joi.array().min(0).items(Joi.object().keys().unknown(true)), Joi.string()) }
}

var quanta = async function (debug, runtime) {
  var i, results, votes
  var contributions = runtime.db.get('contributions', debug)
  var voting = runtime.db.get('voting', debug)

  var dicer = async function (quantum, counts) {
    var params, state, updateP, vote
    var surveyors = runtime.db.get('surveyors', debug)
    var surveyor = await surveyors.findOne({ surveyorId: quantum._id })

    if (!surveyor) return debug('missing surveyor.surveyorId', { surveyorId: quantum._id })

    quantum.created = new Date(parseInt(surveyor._id.toHexString().substring(0, 8), 16) * 1000).getTime()
    quantum.modified = (surveyor.timestamp.high_ * 1000) + (surveyor.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)

    vote = underscore.find(votes, (entry) => { return (quantum._id === entry._id) })
    underscore.extend(quantum, { counts: vote ? vote.counts : 0 })

    params = underscore.pick(quantum, [ 'counts', 'inputs', 'fee', 'quantum' ])
    updateP = false
    underscore.keys(params).forEach((key) => { if (params[key] !== surveyor[key]) updateP = true })
    if (!updateP) return

    state = { $currentDate: { timestamp: { $type: 'timestamp' } }, $set: params }
    await surveyors.update({ surveyorId: quantum._id }, state, { upsert: true })

    surveyor = await surveyors.findOne({ surveyorId: quantum._id })
    if (surveyor) {
      quantum.modified = (surveyor.timestamp.high_ * 1000) + (surveyor.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)
    }
  }

  results = await contributions.aggregate([
    { $match: { satoshis: { $gt: 0 } } },
    { $group:
      { _id: '$surveyorId',
        satoshis: { $sum: '$satoshis' },
        fee: { $sum: '$fee' },
        inputs: { $sum: { $subtract: [ '$satoshis', '$fee' ] } },
        votes: { $sum: '$votes' }
      }
    },
    { $project:
      { _id: 1,
        satoshis: 1,
        fee: 1,
        inputs: 1,
        votes: 1,
        quantum: { $divide: [ '$inputs', '$votes' ] }
      }
    }
  ])
  votes = await voting.aggregate([
      { $match:
        { counts: { $gt: 0 },
          exclude: false
        }
      },
      { $group:
        { _id: '$surveyorId',
          counts: { $sum: '$counts' }
        }
      },
      { $project:
        { _id: 1,
          counts: 1
        }
      }
  ])

  for (i = 0; i < results.length; i++) await dicer(results[i])

  return (underscore.map(results, function (result) {
    return underscore.extend({ surveyorId: result._id }, underscore.omit(result, [ '_id' ]))
  }))
}

module.exports.routes = [
  braveHapi.routes.async().path('/v1/reports/publishers').config(v1.publishers),
  braveHapi.routes.async().path('/v1/reports/surveyors').config(v1.surveyors)
]
