var boom = require('boom')
var braveHapi = require('../brave-hapi')
var braveJoi = require('../brave-joi')
var Joi = require('joi')
var Readable = require('stream').Readable
var underscore = require('underscore')
var url = require('url')
var uuid = require('uuid')

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

v1.publisher = {}
v1.publishers = {}

/*
   GET /v1/reports/publisher/{publisher}/contributions
   GET /v1/reports/publishers/contributions
 */

v1.publisher.contributions =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-contributions',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.params, request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns information about contributions to a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') },
      query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the report'
                       ),
               summary: Joi.boolean().optional().default(true).description('summarize report')
              } }
}

v1.publishers.contributions =
{ handler: function (runtime) {
  return async function (request, reply) {
    var amount = request.query.amount
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var currency = request.query.currency
    var rate = runtime.wallet.rates[currency.toUpperCase()]
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var threshold = 0
    var debug = braveHapi.debug(module, request)

    if ((amount) && (rate)) threshold = Math.floor((amount / rate) * 1e8)

    await runtime.queue.send(debug, 'report-publishers-contributions',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 { threshold: threshold },
                                                 request.query))
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
               summary: Joi.boolean().optional().default(true).description('summarize report'),
               authorized: Joi.boolean().optional().description('filter on authorization status'),
               amount: Joi.number().integer().min(0).optional().description('the minimum amount in fiat currency'),
               currency: braveJoi.string().currencyCode().optional().default('USD').description('the fiat currency')
              } }
}

/*
   GET /v1/reports/publisher/{publisher}/settlements
   GET /v1/reports/publishers/settlements
 */

v1.publisher.settlements =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-settlements',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.params, request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns information about settlements to a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') },
      query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the report'
                       ),
               summary: Joi.boolean().optional().default(true).description('summarize report')
              } }
}

v1.publishers.settlements =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-settlements',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns information about settlements to publishers',
  tags: [ 'api' ],

  validate:
    { query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the report'
                       ),
               summary: Joi.boolean().optional().default(true).description('summarize report')
              } }
}

/*
   GET /v1/reports/publisher/{publisher}/statements
   GET /v1/reports/publishers/statements/{hash}
 */

v1.publisher.statements =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-statements',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.params, request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns statements for a publisher',
  tags: [ 'api' ],

  validate:
    { params: { publisher: braveJoi.string().publisher().required().description('the publisher identity') },
      query: { summary: Joi.boolean().optional().default(true).description('summarize report') }
    }
}

v1.publishers.statements =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var hash = request.params.hash
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-statements',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 { hash: hash },
                                                 request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger' ],
      mode: 'required'
    },

  description: 'Returns statements for publishers',
  tags: [ 'api' ],

  validate:
    { params: { hash: Joi.string().hex().required().description('transaction hash') },
      query: { summary: Joi.boolean().optional().default(true).description('summarize report') }
    }
}

/*
   GET /v1/reports/publishers/status
 */

v1.publishers.status =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-publishers-status',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.query))
    reply({ reportURL: reportURL })
  }
},

  auth:
    { strategy: 'session',
      scope: [ 'ledger', 'QA' ],
      mode: 'required'
    },

  description: 'Returns information about publisher status',
  tags: [ 'api' ],

  validate:
    { query: { format: Joi.string().valid('json', 'csv').optional().default('csv').description(
                         'the format of the response'
                       ),
               elide: Joi.boolean().optional().default(true).description('elide contact information'),
               summary: Joi.boolean().optional().default(true).description('summarize report'),
               verified: Joi.boolean().optional().description('filter on verification status')
              } },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

/*
   GET /v1/reports/surveyors
 */

v1.surveyors = {}

v1.surveyors.contributions =
{ handler: function (runtime) {
  return async function (request, reply) {
    var authority = request.auth.credentials.provider + ':' + request.auth.credentials.profile.username
    var reportId = uuid.v4().toLowerCase()
    var reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
    var debug = braveHapi.debug(module, request)

    await runtime.queue.send(debug, 'report-surveyors-contributions',
                             underscore.defaults({ reportId: reportId, reportURL: reportURL, authority: authority },
                                                 request.query))
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
                       )
              } },

  response:
    { schema: Joi.object().keys().unknown(true) }
}

module.exports.routes = [
  braveHapi.routes.async().path('/v1/reports/file/{reportId}').config(v1.getFile),
  braveHapi.routes.async().path('/v1/reports/publisher/{publisher}/contributions').config(v1.publisher.contributions),
  braveHapi.routes.async().path('/v1/reports/publishers/contributions').config(v1.publishers.contributions),
  braveHapi.routes.async().path('/v1/reports/publisher/{publisher}/settlements').config(v1.publisher.settlements),
  braveHapi.routes.async().path('/v1/reports/publishers/settlements').config(v1.publishers.settlements),
  braveHapi.routes.async().path('/v1/reports/publisher/{publisher}/statements').config(v1.publisher.statements),
  braveHapi.routes.async().path('/v1/reports/publishers/statements/{hash}').config(v1.publishers.statements),
  braveHapi.routes.async().path('/v1/reports/publishers/status').config(v1.publishers.status),
  braveHapi.routes.async().path('/v1/reports/surveyors/contributions').config(v1.surveyors.contributions)
]

module.exports.initialize = async function (debug, runtime) {
  await runtime.queue.create('report-publishers-contributions')
  await runtime.queue.create('report-publishers-status')
  await runtime.queue.create('report-surveyors-contributions')
}
