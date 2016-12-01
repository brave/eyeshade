var bson = require('bson')
var create = require('./reports.js').create
var underscore = require('underscore')
var url = require('url')
var uuid = require('uuid')

var exports = {}

exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('populates', debug),
      name: 'populates',
      property: 'transactionId',
      empty: { transactionId: '',
               paymentId: '',
               address: '',
               actor: '',
               amount: '',
               currency: '',
               satoshis: 0,
               holdUntil: bson.Timestamp.ZERO,
               timestamp: bson.Timestamp.ZERO
             },
      unique: [ { transactionId: 0 } ],
      others: [ { paymentId: 0 }, { address: 0 }, { actor: 1 }, { amount: 1 }, { currency: 1 }, { satoshis: 1 },
                { holdUntil: 1 }, { timestamp: 1 } ]
    }
  ])
}

var ninetyOneDays = 91 * 24 * 60 * 60 * 1000

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
      var file, reportURL, state
      var now = underscore.now()
      var reportId = uuid.v4().toLowerCase()
      var transactionId = payload.transactionId
      var populates = runtime.db.get('populates', debug)

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: underscore.extend(underscore.omit(payload, [ 'transactionId' ]),
                                        { holdUntil: new Date(now + ninetyOneDays) })
              }
      await populates.update({ transactionId: transactionId }, state, { upsert: true })

/* TODO: this is temporary until we decide how/if to safely automate */
      file = await create(runtime, 'populates-', { format: 'json', reportId: reportId })
      underscore.extend(payload, { BTC: (payload.satoshis / 1e8).toFixed(8) })
      await file.write(JSON.stringify([ payload ], null, 2), true)

      reportURL = url.format(underscore.defaults({ pathname: '/v1/reports/file/' + reportId }, runtime.config.server))
      runtime.notify(debug, { channel: '#payments-bot', text: reportURL })
    }
}

module.exports = exports
