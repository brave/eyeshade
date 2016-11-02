var exports = {}

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
