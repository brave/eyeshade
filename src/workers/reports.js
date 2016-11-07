var bson = require('bson')
var braveHapi = require('../brave-hapi')
var currencyCodes = require('currency-codes')
var dateformat = require('dateformat')
var json2csv = require('json2csv')
const moment = require('moment')
var underscore = require('underscore')

var currency = currencyCodes.code('USD')
if (!currency) currency = { digits: 2 }

var datefmt = 'yyyy-mm-dd HH:MM:ss'

var create = async function (runtime, prefix, params) {
  var extension, filename, options

  if (params.format !== 'csv') {
    options = { content_type: 'application/json' }
    extension = '.json'
  } else {
    options = { content_type: 'text/csv' }
    extension = '.csv'
  }
  filename = prefix + dateformat(underscore.now(), datefmt) + extension
  options.metadata = { 'content-disposition': 'attachment; filename="' + filename + '"' }
  return await runtime.db.file(params.reportId, 'w', options)
}

var daily = async function (debug, runtime) {
  var midnight, tomorrow
  var now = underscore.now()

  debug('daily', 'running')

  midnight = new Date(now)
  midnight.setHours(0, 0, 0, 0)
  midnight = Math.floor(midnight.getTime() / 1000)

  try {
    await runtime.db.purgeSince(debug, runtime, midnight * 1000)
  } catch (ex) {
    debug('daily', ex)
  }
  tomorrow = new Date(now)
  tomorrow.setHours(24, 0, 0, 0)
  setTimeout(function () { daily(debug, runtime) }, tomorrow - now)
  debug('daily', 'running again ' + moment(tomorrow).fromNow())
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

var exports = {}

exports.initialize = async function (debug, runtime) {
  if ((typeof process.env.DYNO === 'undefined') || (process.env.DYNO === 'worker.1')) {
    setTimeout(function () { daily(debug, runtime) }, 5 * 1000)
  }
}

exports.workers = {
/* sent by GET /v1/reports/publishers

    { queue            : 'report-publishers-contributions'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      , summary        :  true  | false
      }
    }
 */
  'report-publishers-contributions':
    async function (debug, runtime, payload) {
      var data, fees, file, i, publishers, results, satoshis, usd
      var format = payload.format || 'csv'
      var summaryP = payload.summary

      var slicer = async function (quantum) {
        var fees, i, satoshis, slice, state
        var voting = runtime.db.get('voting', debug)
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

      file = await create(runtime, 'publishers-', payload)
      if (format !== 'csv') {
        await file.write(JSON.stringify(results, null, 2), true)
        return runtime.notify(debug, { text: 'report-publishers-contributions completed' })
      }

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
                    'publisher USD': (result.satoshis * usd).toFixed(currency.digits),
                    'processor USD': (result.fees * usd).toFixed(currency.digits)
                  })
        if (!summaryP) result.votes.forEach((vote) => { data.push(underscore.extend({ publisher: result.publisher }, vote)) })
      })
      data.push({ publisher: 'TOTAL',
                  total: satoshis,
                  fees: fees,
                  'publisher USD': (satoshis * usd).toFixed(currency.digits),
                  'processor USD': (fees * usd).toFixed(currency.digits)
                })

      await file.write(json2csv({ data: data }), true)
      runtime.notify(debug, { text: 'report-publishers-contributions completed' })
    },

/* sent by GET /v1/reports/publishers/status

    { queue            : 'report-publishers-status'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      , elide          :  true  | false
      , summary        :  true  | false
      }
    }
 */
  'report-publishers-status':
    async function (debug, runtime, payload) {
      var data, entries, f, fields, file, i, keys, results, satoshis, summary, usd
      var format = payload.format || 'csv'
      var elideP = payload.elide
      var summaryP = payload.summary
      var publishers = runtime.db.get('publishers', debug)
      var tokens = runtime.db.get('tokens', debug)
      var voting = runtime.db.get('voting', debug)

      results = {}
      entries = await tokens.find()
      entries.forEach((entry) => {
        var publisher

        publisher = entry.publisher
        if (!publisher) return

        if (!results[publisher]) results[publisher] = underscore.pick(entry, [ 'publisher', 'verified' ])
        if (entry.verified) {
          underscore.extend(results[publisher], underscore.pick(entry, [ 'verified', 'verificationId', 'reason' ]))
        }
        if (summaryP) return

        if (!results[publisher].history) results[publisher].history = []
        entry.modified = (entry.timestamp.high_ * 1000) + (entry.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)
        results[publisher].history.push(underscore.pick(entry, [ 'verificationId', 'verified', 'reason', 'modified' ]))
      })

      summary = await voting.aggregate([
        { $match:
          { satoshis: { $gt: 0 },
          exclude: false
          }
        },
        { $group:
          { _id: '$publisher',
            satoshis: { $sum: '$satoshis' }
          }
        }
      ])
      satoshis = {}
      summary.forEach(function (entry) { satoshis[entry._id] = entry.satoshis })
      usd = runtime.wallet.rates.USD

      f = async function (publisher) {
        var datum, result

        results[publisher].satoshis = satoshis[publisher] || 0
        if (usd) results[publisher].USD = ((results[publisher].satoshis * usd) / 1e8).toFixed(currency.digits)

        if (results[publisher].history) {
          results[publisher].history = underscore.sortBy(results[publisher].history, 'modified')
          if (!results[publisher].verified) results[publisher].reason = underscore.last(results[publisher].history).reason
        }

        datum = await publishers.findOne({ publisher: publisher })
        if (datum) {
          datum.created = new Date(parseInt(datum._id.toHexString().substring(0, 8), 16) * 1000).getTime()
          datum.modified = (datum.timestamp.high_ * 1000) + (datum.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)
          underscore.extend(results[publisher], underscore.omit(datum, [ '_id', 'publisher', 'timestamp', 'verified' ]))
        }

        try {
          result = await braveHapi.wreck.get(runtime.config.publishers.url + '/api/publishers/' + encodeURIComponent(publisher),
                                            { headers: { authorization: 'Bearer ' + runtime.config.publishers.access_token },
                                              useProxyP: true
                                            })
          if (Buffer.isBuffer(result)) result = JSON.parse(result)
          datum = underscore.findWhere(result, function (entry) { return results[publisher].verificationId === entry.id })
          if (datum) {
            underscore.extend(results[publisher], underscore.pick(datum, [ 'name', 'email' ]),
                              { phone: datum.phone_normalized })
          }
          if (!summaryP) {
            results[publisher].history.forEach((record) => {
              datum = underscore.findWhere(result, function (entry) { return record.verificationId === entry.id })
              if (datum) {
                underscore.extend(record, underscore.pick(datum, [ 'name', 'email' ]), { phone: datum.phone_normalized })

                if (elideP) {
                  if (record.address) record.address = 'yes'
                  if (record.email) record.email = 'yes'
                  if (record.phone) record.phone = 'yes'
                }
              }
            })
          }
        } catch (ex) { debug('publisher', { publisher: publisher, reason: ex.toString() }) }

        if (elideP) {
          if (results[publisher].address) results[publisher].address = 'yes'
          if (results[publisher].email) results[publisher].email = 'yes'
          if (results[publisher].phone) results[publisher].phone = 'yes'
          if (results[publisher].legalFormURL) results[publisher].legalFormURL = 'yes'
        }

        data.push(results[publisher])
      }
      data = []
      keys = underscore.keys(results)
      for (i = 0; i < keys.length; i++) await f(keys[i])
      results = underscore.sortBy(data, 'publisher')

      file = await create(runtime, 'publishers-', payload)
      if (format !== 'csv') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { text: 'report-publishers-status completed' })
      }

      data = []
      results.forEach((result) => {
        data.push(underscore.extend(underscore.omit(result, [ 'history' ]),
                                    { created: result.created && dateformat(result.created, datefmt),
                                      modified: result.modified && dateformat(result.modified, datefmt)
                                    }))
        if (!summaryP) {
          result.history.forEach((record) => {
            data.push(underscore.extend({ publisher: result.publisher }, record,
                                        { modified: dateformat(record.modified, datefmt) }))
          })
        }
      })

      fields = [ 'publisher', 'USD', 'satoshis',
                 'verified', 'authorized', 'authority',
                 'name', 'email', 'phone', 'address',
                 'verificationId', 'reason',
                 'created', 'modified',
                 'legalFormURL' ]
      await file.write(json2csv({ data: data, fields: fields }), true)
      runtime.notify(debug, { text: 'report-publishers-status completed' })
    },

/* sent by GET /v1/reports/surveyors-contributions

    { queue            : 'report-surveyors-contributions'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      }
    }
 */
  'report-surveyors-contributions':
    async function (debug, runtime, payload) {
      var data, file
      var format = payload.format || 'csv'

      data = underscore.sortBy(await quanta(debug, runtime), 'created')

      file = await create(runtime, 'surveyors-', payload)
      if (format !== 'csv') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { text: 'report-surveyors-contributions completed' })
      }

      data.forEach((result) => {
        underscore.extend(result,
                          { created: dateformat(result.created, datefmt), modified: dateformat(result.modified, datefmt) })
      })

      await file.write(json2csv({ data: data }), true)
      runtime.notify(debug, { text: 'report-surveyors-contributions completed' })
    }
}

module.exports = exports
