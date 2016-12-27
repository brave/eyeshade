var create = require('./reports.js').create
var underscore = require('underscore')
var url = require('url')
var uuid = require('uuid')

var exports = {}

exports.initialize = async function (debug, runtime) {
  await runtime.queue.create('population-report')
}

exports.workers = {
/* sent by ledger PUT /v1/address/{personaId}/validate

    { queue            : 'population-report'
    , message          :
      { paymentId      : '...'
      , address        : '...'
      , satoshis       : 1403982
      , actor          : 'authorize.stripe'
      , transactionId  : '...'
      , amount         : 10.25
      , fee            : 0.25
      , currency       : 'USD'
      }
    }
 */
  'population-report':
    async function (debug, runtime, payload) {
      var file, reportURL
      var reportId = uuid.v4().toLowerCase()

/* TODO: this is temporary until we decide how/if to safely automate */
      file = await create(runtime, 'populates-', { format: 'json', reportId: reportId })
      underscore.extend(payload, { BTC: (payload.satoshis / 1e8).toFixed(8) })
      await file.write(JSON.stringify([ payload ], null, 2), true)

      reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
      runtime.notify(debug, { channel: '#funding-bot', text: reportURL })
    }
}

module.exports = exports
