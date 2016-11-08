var bson = require('bson')

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
      others: [ { paymentId: 0 }, { address: 1 }, { actor: 1 }, { amount: 1 }, { currency: 1 }, { satoshis: 1 },
                { holdUntil: 1 }, { timestamp: 1 } ]
    }
  ])
}

exports.workers = {
/* sent by ledger PUT /v1/address/{personaId}/validate

    { queue            : 'population-report'
    , message          :
      { paymentId      : '...'
      , address        : '...'
      , actor          : 'authorize.stripe'
      , transactionId  : '...'
      , amount         : 5.00
      , currency       : 'USD'
      }
    }
 */
  'population-report':

    async function (debug, runtime, payload) {
/* TODO:
   record address, amount, description
   generate instructions for bitgo
 */

      debug('population-report', payload)
    }
}

module.exports = exports
