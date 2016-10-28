var boom = require('boom')
var braveHapi = require('../brave-hapi')
var Joi = require('joi')
var Readable = require('stream').Readable
var underscore = require('underscore')
var url = require('url')
var uuid = require('node-uuid')

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
   GET /v1/reports/publishers
 */

v1.publishers =
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
    { query: { format: Joi.string().valid('json', 'csv').optional().default('json').description(
                         'the format of the response'
                       ),
               summary: Joi.boolean().optional().default(false).description('summarize results (CSV only)')
              } },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

/*
   GET /v1/reports/surveyors
 */

v1.surveyors =
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
    { query: { format: Joi.string().valid('json', 'csv').optional().default('json').description(
                         'the format of the response'
                       ) } },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

module.exports.routes = [
  braveHapi.routes.async().path('/v1/reports/file/{reportId}').config(v1.getFile),
  braveHapi.routes.async().path('/v1/reports/publishers').config(v1.publishers),
  braveHapi.routes.async().path('/v1/reports/surveyors').config(v1.surveyors)
]

module.exports.initialize = async function (debug, runtime) {
  await runtime.queue.create('report-publishers')
  await runtime.queue.create('report-surveyors')
}
