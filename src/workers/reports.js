var bson = require('bson')
var dateformat = require('dateformat')
var json2csv = require('json2csv')
const moment = require('moment')
var underscore = require('underscore')

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

    { queue            : 'report-publishers'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      , summary        :  true  | false
      }
    }
 */
  'report-publishers':
    async function (debug, runtime, payload) {
      var data, fees, file, i, publishers, results, satoshis, usd
      var reportURL = payload.reportURL
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

      file = await create(runtime, 'publishers-', payload)

      publishers = {}
      results = await quanta(debug, runtime)
      for (i = 0; i < results.length; i++) await slicer(results[i])

      results = []
      underscore.keys(publishers).forEach((publisher) => {
        publishers[publisher].votes = underscore.sortBy(publishers[publisher].votes, 'surveyorId')
        results.push(underscore.extend({ publisher: publisher }, publishers[publisher]))
      })
      results = underscore.sortBy(results, 'publisher')
      if (format !== 'csv') {
        await file.write(JSON.stringify(results, null, 2), true)
        return runtime.notify(debug, { text: 'created ' + reportURL })
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

      await file.write(json2csv({ data: data }), true)
      runtime.notify(debug, { text: 'report-publishers completed' })
    },

/* sent by GET /v1/reports/surveyors

    { queue            : 'report-surveyors'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      }
    }
 */
  'report-surveyors':
    async function (debug, runtime, payload) {
      var data, file
      var format = payload.format || 'csv'
      var reportURL = payload.reportURL

      file = await create(runtime, 'surveyors-', payload)

      data = underscore.sortBy(await quanta(debug, runtime), 'created')
      if (format !== 'csv') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { text: 'created ' + reportURL })
      }

      data.forEach((result) => {
        underscore.extend(result,
                          { created: dateformat(result.created, datefmt), modified: dateformat(result.modified, datefmt) })
      })

      await file.write(json2csv({ data: data }), true)
      runtime.notify(debug, { text: 'report-surveyors completed' })
    }
}

module.exports = exports
