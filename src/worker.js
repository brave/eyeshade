process.env.NEW_RELIC_NO_CONFIG_FILE = true
if (process.env.NEW_RELIC_APP_NAME && process.env.NEW_RELIC_LICENSE_KEY) { var newrelic = require('newrelic') }
if (!newrelic) {
  newrelic = {
    createBackgroundTransaction: (name, group, cb) => { return (cb || group) },
    noticeError: (ex, params) => {},
    recordCustomEvent: (eventType, attributes) => {},
    endTransaction: () => {}
  }
}

var bson = require('bson')
var dateformat = require('dateformat')
var debug = new (require('sdebug'))('worker')
var json2csv = require('json2csv')
var ledgerPublisher = require('ledger-publisher')
var path = require('path')
var underscore = require('underscore')

var npminfo = require(path.join(__dirname, '..', 'package'))
var runtime = require('./runtime.js')
runtime.newrelic = newrelic

var datefmt = 'yyyy-mm-dd HH:MM:ss'

var creater = async function (params) {
  var extension, filename, options

  if (params.format !== 'csv') {
    options = { content_type: 'application/json' }
    extension = '.json'
  } else {
    options = { content_type: 'text/csv' }
    extension = '.csv'
  }
  filename = 'publishers-' + dateformat(underscore.now(), datefmt) + extension
  options.metadata = { 'content-disposition': 'attachment; filename="' + filename + '"' }
  return await runtime.db.file(params.reportId, 'w', options)
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

var slicer = async function (debug, publishers, quantum) {
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

  return publishers
}

var reports = {
  'prune-publishers':
      async function (debug, payload) {
/* sent by POST /v1/publishers/prune

    { queue            : 'prune-publishers'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      }
    }
 */
        var file, results, state, votes
        var reportId = payload.reportId
        var reportURL = payload.reportURL
        var voting = runtime.db.get('voting', debug)

        file = await runtime.db.file(reportId, 'w', { content_type: 'application/json' })

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

        await file.write(JSON.stringify(results, null, 2), true)
        runtime.notify(debug, { text: 'created ' + reportURL })
      },

  'report-publishers':
      async function (debug, payload) {
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

        var data, fees, file, i, publishers, results, satoshis, usd
        var reportURL = payload.reportURL
        var format = payload.format || 'json'
        var summaryP = payload.summary

        file = await creater(payload)

        publishers = {}
        results = await quanta(debug, runtime)
        for (i = 0; i < results.length; i++) publishers = (await slicer(debug, publishers, results[i]))

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
        runtime.notify(debug, { text: 'created ' + reportURL })
      },

  'report-surveyors':
      async function (debug, payload) {
/* sent by GET /v1/reports/surveyors

    { queue            : 'report-surveyors'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , format         : 'json' | 'csv'
      }
    }
 */

        var data, file
        var format = payload.format || 'json'
        var reportURL = payload.reportURL

        file = await creater(payload)

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
        runtime.notify(debug, { text: 'created ' + reportURL })
      }
}

var main = async function (id) {
  var register = async function (report) {
    await runtime.queue.create(report)
    runtime.queue.listen(report,
      runtime.newrelic.createBackgroundTransaction(report, async function (err, debug, payload) {
        if (err) return debug('prune-publishers listen', err)

        try { await reports[report](debug, payload) } catch (ex) {
          debug(report, { payload: payload, err: ex, stack: ex.stack })
          runtime.newrelic.noticeError(ex, payload)
        }
        runtime.newrelic.endTransaction()
      })
    )
  }

  debug.initialize({ worker: { id: id } })

  runtime.npminfo = underscore.pick(npminfo, 'name', 'version', 'description', 'author', 'license', 'bugs', 'homepage')
  runtime.npminfo.children = {}
  runtime.notify(debug, { text: require('os').hostname() + ' ' + npminfo.name + '@' + npminfo.version +
                                  ' started ' + (process.env.DYNO || 'worker') + '/' + id })

  underscore.keys(reports).forEach(async function (report) { await register(report) })
}

main(1)
