var bson = require('bson')
var braveHapi = require('../brave-hapi')
var currencyCodes = require('currency-codes')
var dateformat = require('dateformat')
var json2csv = require('json2csv')
const moment = require('moment')
var underscore = require('underscore')

var currency = currencyCodes.code('USD')
if (!currency) currency = { digits: 2 }

var datefmt = 'yyyymmdd-HHMMss'
var datefmt2 = 'yyyymmdd-HHMMss-l'

var create = async function (runtime, prefix, params) {
  var extension, filename, options

  if (params.format === 'json') {
    options = { content_type: 'application/json' }
    extension = '.json'
  } else {
    options = { content_type: 'text/csv' }
    extension = '.csv'
  }
  filename = prefix + dateformat(underscore.now(), datefmt2) + extension
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

var hourly = async function (debug, runtime) {
  var next
  var now = underscore.now()

  debug('hourly', 'running')

  try {
    await mixer(debug, runtime, undefined, false)
  } catch (ex) {
    debug('hourly', ex)
  }
  next = now + 60 * 60 * 1000
  setTimeout(function () { hourly(debug, runtime) }, next - now)
  debug('hourly', 'running again ' + moment(next).fromNow())
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

var mixer = async function (debug, runtime, publisher, reportP) {
  var i, results
  var publishers = {}
  var publishersC = runtime.db.get('publishers', debug)

  var slicer = async function (quantum) {
    var entry, fees, i, satoshis, slice, state
    var voting = runtime.db.get('voting', debug)
    var slices = await voting.find({ surveyorId: quantum.surveyorId, exclude: false })

    for (i = 0; i < slices.length; i++) {
      slice = slices[i]

      satoshis = Math.floor(quantum.quantum * slice.counts * 0.95)
      fees = Math.floor((quantum.quantum * slice.counts) - satoshis)
      if ((publisher) && (slice.publisher !== publisher)) continue

      if (!publishers[slice.publisher]) {
        publishers[slice.publisher] = { satoshis: 0, fees: 0, votes: [] }

        if (reportP) {
          entry = await publishersC.findOne({ publisher: slice.publisher })
          if (entry) {
            underscore.extend(publishers[slice.publisher], underscore.pick(entry, [ 'authorized', 'address' ]))
          } else {
            publishers[slice.publisher].authorized = false
          }
        }
      }
      publishers[slice.publisher].satoshis += satoshis
      publishers[slice.publisher].fees += fees
      publishers[slice.publisher].votes.push({ surveyorId: quantum.surveyorId,
                                               lastUpdated: (slice.timestamp.high_ * 1000) +
                                                              (slice.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_),
                                               counts: slice.counts,
                                               satoshis: satoshis,
                                               fees: fees
                                             })
      if (slice.satoshis === satoshis) continue

      state = { $set: { satoshis: satoshis } }
      await voting.update({ surveyorId: quantum.surveyorId, publisher: slice.publisher }, state, { upsert: true })
    }
  }

  results = await quanta(debug, runtime)
  for (i = 0; i < results.length; i++) await slicer(results[i])
  return publishers
}

var publisherCompare = function (a, b) {
  return braveHapi.domainCompare(a.publisher, b.publisher)
}

var publisherContributions = function (runtime, publishers, authority, authorized, format, reportId, summaryP, threshold, usd) {
  var data, fees, results, satoshis

  results = []
  underscore.keys(publishers).forEach((publisher) => {
    if (publishers[publisher].satoshis <= threshold) return

    if ((typeof authorized === 'boolean') && (publishers[publisher].authorized !== authorized)) return

    publishers[publisher].votes = underscore.sortBy(publishers[publisher].votes, 'surveyorId')
    results.push(underscore.extend({ publisher: publisher }, publishers[publisher]))
  })
  results = results.sort(publisherCompare)

  if (format === 'json') {
    if (summaryP) {
      publishers = []
      results.forEach((entry) => {
        var result

        if (!entry.authorized) return

        result = underscore.pick(entry, [ 'publisher', 'address', 'satoshis', 'fees' ])
        result.authority = authority
        result.transactionId = reportId
        result.amount = (entry.satoshis * usd).toFixed(currency.digits)
        result.fee = (entry.fees * usd).toFixed(currency.digits)
        result.currency = 'USD'
        publishers.push(result)
      })

      results = publishers
    }

    return { data: results }
  }

  satoshis = 0
  fees = 0

  data = []
  results.forEach((result) => {
    satoshis += result.satoshis
    fees += result.fees
    data.push({ publisher: result.publisher,
                satoshis: result.satoshis,
                fees: result.fees,
                'publisher USD': (result.satoshis * usd).toFixed(currency.digits),
                'processor USD': (result.fees * usd).toFixed(currency.digits)
              })
    if (!summaryP) {
      underscore.sortBy(result.votes, 'lastUpdated').forEach((vote) => {
        data.push(underscore.extend({ publisher: result.publisher },
                                    underscore.omit(vote, [ 'surveyorId', 'updated' ]),
                                    { transactionId: vote.surveyorId,
                                      lastUpdated: dateformat(vote.lastUpdated, datefmt)
                                    }))
      })
    }
  })

  return { data: data, satoshis: satoshis, fees: fees }
}

var publisherSettlements = function (runtime, entries, format, summaryP, usd) {
  var data, fees, results, satoshis
  var publishers = {}

  entries.forEach((entry) => {
    if (entry.publisher === '') return

    if (!publishers[entry.publisher]) publishers[entry.publisher] = { satoshis: 0, fees: 0, txns: [] }

    publishers[entry.publisher].satoshis += entry.satoshis
    publishers[entry.publisher].fees += entry.fees
    entry.created = new Date(parseInt(entry._id.toHexString().substring(0, 8), 16) * 1000).getTime()
    entry.modified = (entry.timestamp.high_ * 1000) + (entry.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)

    publishers[entry.publisher].txns.push(underscore.pick(entry, [ 'satoshis', 'fees', 'settlementId', 'address',
                                                                   'hash', 'created', 'modified' ]))
  })

  results = []
  underscore.keys(publishers).forEach((publisher) => {
    publishers[publisher].txns = underscore.sortBy(publishers[publisher].txns, 'created')
    results.push(underscore.extend({ publisher: publisher }, publishers[publisher]))
  })
  results = results.sort(publisherCompare)

  if (format === 'json') return { data: results }

  satoshis = 0
  fees = 0

  data = []
  results.forEach((result) => {
    satoshis += result.satoshis
    fees += result.fees
    data.push({ publisher: result.publisher,
                satoshis: result.satoshis,
                fees: result.fees,
                'publisher USD': (result.satoshis * usd).toFixed(currency.digits),
                'processor USD': (result.fees * usd).toFixed(currency.digits)
              })
    if (!summaryP) {
      result.txns.forEach((txn) => {
        data.push(underscore.extend({ publisher: result.publisher },
                                    underscore.omit(txn, [ 'hash', 'settlementId', 'created', 'modified' ]),
                                    { transactionId: txn.hash,
                                      lastUpdated: txn.created && dateformat(txn.created, datefmt)
                                    }))
      })
    }
  })

  return { data: data, satoshis: satoshis, fees: fees }
}

var exports = {}

exports.initialize = async function (debug, runtime) {
  if ((typeof process.env.DYNO === 'undefined') || (process.env.DYNO === 'worker.1')) {
    setTimeout(function () { daily(debug, runtime) }, 5 * 1000)
    setTimeout(function () { hourly(debug, runtime) }, 30 * 1000)
  }
}

exports.create = create

exports.workers = {
/* sent by GET /v1/reports/publisher/{publisher}/contributions
           GET /v1/reports/publishers/contributions

    { queue            : 'report-publishers-contributions'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authorized     :  true  | false
      , authority      : '...:...'
      , format         : 'json' | 'csv'
      , publisher      : '...'
      , summary        :  true  | false
      , threshold      : satoshis
      }
    }
 */
  'report-publishers-contributions':
    async function (debug, runtime, payload) {
      var data, file, info, previous, publishers, usd
      var authority = payload.authority
      var authorized = payload.authorized
      var format = payload.format || 'csv'
      var publisher = payload.publisher
      var reportId = payload.reportId
      var summaryP = payload.summary
      var threshold = payload.threshold || 0
      var settlements = runtime.db.get('settlements', debug)

      publishers = await mixer(debug, runtime, publisher, (format === 'json') || (typeof authorized === 'boolean'))
      previous = await settlements.aggregate([
        { $match:
          { satoshis: { $gt: 0 } }
        },
        { $group:
          { _id: '$publisher',
            satoshis: { $sum: '$satoshis' }
          }
        }
      ])
      previous.forEach(function (entry) {
        if (typeof publishers[entry._id] !== 'undefined') publishers[entry._id].satoshis -= entry.satoshis
      })

      usd = runtime.wallet.rates.USD
      usd = (Number.isFinite(usd)) ? (usd / 1e8) : null

      info = publisherContributions(runtime, publishers, authority, authorized, format, reportId, summaryP, threshold, usd)
      data = info.data

      file = await create(runtime, 'publishers-', payload)
      if (format === 'json') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { channel: '#publishers-bot',
                                       text: authority + ' report-publishers-contributions completed' })
      }

      if (!publisher) {
        data.push({ publisher: 'TOTAL',
                    satoshis: info.satoshis,
                    fees: info.fees,
                    'publisher USD': (info.satoshis * usd).toFixed(currency.digits),
                    'processor USD': (info.fees * usd).toFixed(currency.digits)
                  })
      }

      try { await file.write(json2csv({ data: data }), true) } catch (ex) {
        debug('reports', { report: 'report-publishers-contributions', reason: ex.toString() })
        file.close()
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' report-publishers-contributions completed' })
    },

/* sent by GET /v1/reports/publisher/{publisher}/settlements
           GET /v1/reports/publishers/settlements

    { queue            : 'report-publishers-settlements'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , format         : 'json' | 'csv'
      , publisher      : '...'
      , summary        :  true  | false
      }
    }
 */
  'report-publishers-settlements':
    async function (debug, runtime, payload) {
      var data, entries, file, info, usd
      var authority = payload.authority
      var format = payload.format || 'csv'
      var publisher = payload.publisher
      var summaryP = payload.summary
      var settlements = runtime.db.get('settlements', debug)

      entries = publisher ? (await settlements.find({ publisher: publisher })) : (await settlements.find())

      usd = runtime.wallet.rates.USD
      usd = (Number.isFinite(usd)) ? (usd / 1e8) : null
      info = publisherSettlements(runtime, entries, format, summaryP, usd)
      data = info.data

      file = await create(runtime, 'publishers-settlements-', payload)
      if (format === 'json') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { channel: '#publishers-bot',
                                       text: authority + ' report-publishers-settlements completed' })
      }

      if (!publisher) {
        data.push({ publisher: 'TOTAL',
                    satoshis: info.satoshis,
                    fees: info.fees,
                    'publisher USD': (info.satoshis * usd).toFixed(currency.digits),
                    'processor USD': (info.fees * usd).toFixed(currency.digits)
                })
      }

      try { await file.write(json2csv({ data: data }), true) } catch (ex) {
        debug('reports', { report: 'report-publishers-settlements', reason: ex.toString() })
        file.close()
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' report-publishers-settlements completed' })
    },

/* sent by GET /v1/reports/publisher/{publisher}/statements
           GET /v1/reports/publishers/statements

    { queue            : 'report-publishers-statements'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , hash           : '...'
      , publisher      : '...'
      , summary        :  true  | false
      }
    }
 */
  'report-publishers-statements':
    async function (debug, runtime, payload) {
      var data, data1, data2, file, entries, publishers, usd
      var authority = payload.authority
      var hash = payload.hash
      var summaryP = payload.summary
      var publisher = payload.publisher
      var settlements = runtime.db.get('settlements', debug)

      if (publisher) {
        entries = await settlements.find({ publisher: publisher })
        publishers = await mixer(debug, runtime, publisher, false)
      } else {
        entries = await settlements.find({ hash: hash })
        publishers = await mixer(debug, runtime, undefined, false)
        underscore.keys(publishers).forEach(function (publisher) {
          if (underscore.where(entries, { publisher: publisher }).length === 0) delete publishers[publisher]
        })
      }

      usd = runtime.wallet.rates.USD
      usd = (Number.isFinite(usd)) ? (usd / 1e8) : null

      data = []
      data1 = { satoshis: 0, fees: 0 }
      data2 = { satoshis: 0, fees: 0 }
      underscore.keys(publishers).sort(braveHapi.domainCompare).forEach(function (publisher) {
        var entry = {}
        var info

        entry[publisher] = publishers[publisher]
        info = publisherContributions(runtime, entry, undefined, undefined, 'csv', undefined, summaryP, undefined, usd)
        data = data.concat(info.data)
        data1.satoshis += info.satoshis
        data1.fees += info.fees
        if (!summaryP) data.push([])

        info = publisherSettlements(runtime, underscore.where(entries, { publisher: publisher }), 'csv', summaryP, usd)
        data = data.concat(info.data)
        data2.satoshis += info.satoshis
        data2.fees += info.fees
        data.push([])
        if (!summaryP) data.push([])
      })
      if (!publisher) {
        data.push({ publisher: 'TOTAL',
                    satoshis: data1.satoshis,
                    fees: data1.fees,
                    'publisher USD': (data1.satoshis * usd).toFixed(currency.digits),
                    'processor USD': (data1.fees * usd).toFixed(currency.digits)
                  })
        if (!summaryP) data.push([])
        data.push({ publisher: 'TOTAL',
                    satoshis: data2.satoshis,
                    fees: data2.fees,
                    'publisher USD': (data2.satoshis * usd).toFixed(currency.digits),
                    'processor USD': (data2.fees * usd).toFixed(currency.digits)
                })
      }

      file = await create(runtime, 'publishers-statements-', payload)
      try { await file.write(json2csv({ data: data }), true) } catch (ex) {
        debug('reports', { report: 'report-publishers-contributions', reason: ex.toString() })
        file.close()
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' report-publishers-statements completed' })
    },

/* sent by GET /v1/reports/publishers/status

    { queue            : 'report-publishers-status'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , format         : 'json' | 'csv'
      , elide          :  true  | false
      , summary        :  true  | false
      }
    }
 */
  'report-publishers-status':
    async function (debug, runtime, payload) {
      var data, entries, f, fields, file, i, keys, now, results, satoshis, summary, usd
      var authority = payload.authority
      var format = payload.format || 'csv'
      var elideP = payload.elide
      var summaryP = payload.summary
      var publishers = runtime.db.get('publishers', debug)
      var settlements = runtime.db.get('settlements', debug)
      var tokens = runtime.db.get('tokens', debug)
      var voting = runtime.db.get('voting', debug)

      var daysago = (timestamp) => {
        return Math.round((now - timestamp) / (86400 * 1000))
      }

      now = underscore.now()
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

        if (!results[publisher].history) results[publisher].history = []
        entry.created = new Date(parseInt(entry._id.toHexString().substring(0, 8), 16) * 1000).getTime()
        entry.modified = (entry.timestamp.high_ * 1000) + (entry.timestamp.low_ / bson.Timestamp.TWO_PWR_32_DBL_)
        results[publisher].history.push(underscore.pick(entry,
                                                        [ 'verificationId', 'verified', 'reason', 'created', 'modified' ]))
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
      summary = await settlements.aggregate([
        { $match:
          { satoshis: { $gt: 0 } }
        },
        { $group:
          { _id: '$publisher',
            satoshis: { $sum: '$satoshis' }
          }
        }
      ])
      summary.forEach(function (entry) {
        if (typeof satoshis[entry._id] !== 'undefined') satoshis[entry._id] -= entry.satoshis
      })
      usd = runtime.wallet.rates.USD

      f = async function (publisher) {
        var datum, datum2, result

        results[publisher].satoshis = satoshis[publisher] || 0
        if (usd) results[publisher].USD = ((results[publisher].satoshis * usd) / 1e8).toFixed(currency.digits)

        if (results[publisher].history) {
          results[publisher].history = underscore.sortBy(results[publisher].history, (record) => {
            return (record.verified ? Number.POSITIVE_INFINITY : record.modified)
          })
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
          datum = underscore.findWhere(result, { id: results[publisher].verificationId })
          if (datum) {
            underscore.extend(results[publisher], underscore.pick(datum, [ 'name', 'email' ]),
                              { phone: datum.phone_normalized })
          }

          results[publisher].history.forEach((record) => {
            datum2 = underscore.findWhere(result, { id: record.verificationId })
            if (datum2) {
              underscore.extend(record, underscore.pick(datum2, [ 'name', 'email' ]), { phone: datum2.phone_normalized })
            }
          })
          if ((!datum) && (datum2)) {
            underscore.extend(results[publisher], underscore.pick(datum2, [ 'name', 'email' ]),
                              { phone: datum2.phone_normalized })
          }
        } catch (ex) { debug('publisher', { publisher: publisher, reason: ex.toString() }) }

        if (elideP) {
          if (results[publisher].email) results[publisher].email = 'yes'
          if (results[publisher].phone) results[publisher].phone = 'yes'
          if (results[publisher].address) results[publisher].address = 'yes'
          if (results[publisher].verificationId) results[publisher].verificationId = 'yes'
          if (results[publisher].legalFormURL) results[publisher].legalFormURL = 'yes'
        }

        data.push(results[publisher])
      }
      data = []
      keys = underscore.keys(results)
      for (i = 0; i < keys.length; i++) await f(keys[i])
      results = data.sort(publisherCompare)

      file = await create(runtime, 'publishers-status-', payload)
      if (format === 'json') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { channel: '#publishers-bot',
                                       text: authority + ' report-publishers-status completed' })
      }

      data = []
      results.forEach((result) => {
        if (!result.created) {
          underscore.extend(result, underscore.pick(underscore.last(result.history), [ 'created', 'modified' ]))
        }
        data.push(underscore.extend(underscore.omit(result, [ 'history' ]),
                                    { created: dateformat(result.created, datefmt),
                                      modified: dateformat(result.modified, datefmt),
                                      daysInQueue: daysago(result.created)
                                    }))
        if (!summaryP) {
          result.history.forEach((record) => {
            if (elideP) {
              if (record.email) record.email = 'yes'
              if (record.phone) record.phone = 'yes'
              if (record.address) record.address = 'yes'
              if (record.verificationId) record.verificationId = 'yes'
            }
            data.push(underscore.extend({ publisher: result.publisher }, record,
                                        { created: dateformat(record.created, datefmt),
                                          modified: dateformat(record.modified, datefmt),
                                          daysInQueue: daysago(record.created)
                                        }))
          })
        }
      })

      fields = [ 'publisher', 'USD', 'satoshis',
                 'verified', 'authorized', 'authority',
                 'name', 'email', 'phone', 'address',
                 'verificationId', 'reason',
                 'daysInQueue', 'created', 'modified',
                 'legalFormURL' ]
      try { await file.write(json2csv({ data: data, fields: fields }), true) } catch (ex) {
        debug('reports', { report: 'report-publishers-status', reason: ex.toString() })
        file.close()
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' report-publishers-status completed' })
    },

/* sent by GET /v1/reports/surveyors-contributions

    { queue            : 'report-surveyors-contributions'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , format         : 'json' | 'csv'
      }
    }
 */
  'report-surveyors-contributions':
    async function (debug, runtime, payload) {
      var data, file
      var authority = payload.authority
      var format = payload.format || 'csv'

      data = underscore.sortBy(await quanta(debug, runtime), 'created')

      file = await create(runtime, 'surveyors-contributions-', payload)
      if (format === 'json') {
        await file.write(JSON.stringify(data, null, 2), true)
        return runtime.notify(debug, { channel: '#publishers-bot',
                                       text: authority + ' report-surveyors-contributions completed' })
      }

      data.forEach((result) => {
        underscore.extend(result,
                          { created: dateformat(result.created, datefmt), modified: dateformat(result.modified, datefmt) })
      })

      try { await file.write(json2csv({ data: data }), true) } catch (ex) {
        debug('reports', { report: 'report-surveyors-contributions', reason: ex.toString() })
        file.close()
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' report-surveyors-contributions completed' })
    }
}

module.exports = exports
