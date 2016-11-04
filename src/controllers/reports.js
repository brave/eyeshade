var boom = require('boom')
var braveHapi = require('../brave-hapi')
var dateformat = require('dateformat')
var json2csv = require('json2csv')
var Joi = require('joi')
var Readable = require('stream').Readable
var underscore = require('underscore')
var url = require('url')
var uuid = require('node-uuid')

var datefmt = 'yyyy-mm-dd HH:MM:ss'

var v1 = {}

/*
   GET /v1/reports/file/{reportId}
 */

v1.getFile =
{ handler: function (runtime) {
  return async function (request, reply) {
    var file, reader, writer
    var debug = braveHapi.debug(module, request)
    var reportId = request.params.reportId

    file = await runtime.db.file(reportId, 'r')
    if (!file) return reply(boom.notFound('no such report: ' + reportId))

    reader = runtime.db.source({ filename: reportId })
    reader.on('error', (err) => {
      debug('getFile error', err)
      reply(boom.badImplementation('Sic transit gloria mundi: ' + reportId))
    }).on('open', () => {
      debug('getFile open', underscore.pick(file, [ 'contentType', 'metadata' ]))
      writer = reply(new Readable().wrap(reader))
      if (file.contentType) {
        console.log('contentType=' + file.contentType)
        writer = writer.type(file.contentType)
      }
      underscore.keys(file.metadata || {}).forEach((header) => {
        console.log('header= ' + header + ': ' + file.metadata[header])
        writer = writer.header(header, file.metadata[header])
      })
    })
  }
},

  description: 'Gets a report file',
  tags: [ 'api' ],

  validate:
    { params: { reportId: Joi.string().guid().required().description('the report identifier') } }
}

/*
   GET /v1/reports/publishers/contributions
 */

v1.publishers = {}

v1.publishers.contributions =
{ handler: function (runtime) {
  return async function (request, reply) {
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL }, request.query))
    reply({ reportURL: reportURL })
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
    { query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the report'
                       ),
               summary: Joi.boolean().optional().default(true).description('summarize report')
              } }
}

v1.publishers.status =
{ handler: function (runtime) {
  return async function (request, reply) {
    var data, entries, filename, results
    var debug = braveHapi.debug(module, request)
    var format = request.query.format || 'csv'
    var summaryP = request.query.summary
    var publishers = runtime.db.get('publishers', debug)
    var tokens = runtime.db.get('tokens', debug)

    results = {}
    entries = await tokens.find()
    entries.forEach(async function (entry) {
      var publisher

      publisher = entry.publisher
      if (!publisher) return

      if (!results[publisher]) results[publisher] = underscore.pick(entry, [ 'publisher', 'verified' ])
      if (!summaryP) {
        if (!results[publisher].history) results[publisher].history = []
        results[publisher].history.push(underscore.pick(entry, [ 'verificationId', 'verified', 'reason', 'timestamp' ]))
      }
      if (entry.verified) underscore.extend(results[publisher], underscore.pick(entry, [ 'verified', 'verificationId' ]))
    })
    underscore.keys(results).forEach(async function (publisher) {
      var datum = await publishers.findOne({ publisher: publisher })

      if (!datum) return

    try {
      datum.created = new Date(parseInt(datum._id.substring(0, 8), 16) * 1000)
      datum.modified = new Date((datum.timestamp.high_ * 1000) + datum.timestamp._low_).toISOString()
      results[publisher] = underscore.extend(results[publisher], underscore.omit(datum, [ '_id', 'publisher', 'timestamp' ]))
      debug('status', results[publisher])
    } catch(ex) { console.log(ex) }
    })

    if (format !== 'csv') return reply(results)

    data = []
    results.forEach((result) => {
      data.push(underscore.omit(result, [ 'history' ]))
      if (!summaryP) result.history.forEach((entry) => { data.push(underscore.extend({ publisher: result.publisher }, entry)) })
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

  description: 'Returns information about publisher status',
  tags: [ 'api' ],

  validate:
    { query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the response'
                       ),
               summary: Joi.boolean().optional().default(true).description('summarize report')
              } }
/*
,
  response:
    { schema: Joi.object().keys().unknown(true) }
 */
}

/*
   GET /v1/reports/surveyors
 */

v1.surveyors = {}

v1.surveyors.contributions =
{ handler: function (runtime) {
  return async function (request, reply) {
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-surveyors',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL }, request.query))
    reply({ reportURL: reportURL })
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
    { query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the report'
                       ) } },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

module.exports.routes = [
  braveHapi.routes.async().path('/v1/reports/file/{reportId}').config(v1.getFile),
  braveHapi.routes.async().path('/v1/reports/publishers/contributions').config(v1.publishers.contributions),
  braveHapi.routes.async().path('/v1/reports/publishers/status').config(v1.publishers.status),
  braveHapi.routes.async().path('/v1/reports/surveyors/contributions').config(v1.surveyors.contributions)
]

module.exports.initialize = async function (debug, runtime) {
  await runtime.queue.create('report-publishers')
  await runtime.queue.create('report-surveyors')
}
