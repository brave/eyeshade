var underscore = require('underscore')

var exports = {}

exports.workers = {
/* sent by ledger POST /v1/registrar/persona/{personaId}

    { queue            : 'persona-report'
    , message          :
      { paymentId      : '...'
      , address        : '...'
      , provider       : 'bitgo'
      , keychains      :
        { user         : { xpub: '...', encryptedXprv: '...' }
        , backup       : { xpub: '...', encryptedXprv: '...' }
        }
      }
    }
 */
  'persona-report':
    async function (debug, runtime, payload) {
      var state
      var paymentId = payload.paymentId
      var wallets = runtime.db.get('wallets', debug)

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: underscore.extend({ paymentStamp: 0 }, underscore.omit(payload, [ 'paymentId' ]))
              }
      await wallets.update({ paymentId: paymentId }, state, { upsert: true })
    },

/* sent by ledger POST /v1/surveyor/contribution
           ledger PATCH /v1/surveyor/contribution/{surveyorId}
           daily()

    { queue            : 'surveyor-report'
    , message          :
      { surveyorId     : '...'
      , surveyorType   : '...'
      , satoshis       : ...
      , votes          : ...
      }
    }
 */
  'surveyor-report':
    async function (debug, runtime, payload) {
      var state
      var surveyorId = payload.surveyorId
      var surveyors = runtime.db.get('surveyors', debug)

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: underscore.extend({ counts: 0 }, underscore.omit(payload, [ 'surveyorId' ]))
              }
      await surveyors.update({ surveyorId: surveyorId }, state, { upsert: true })
    },

/* sent by PUT /v1/wallet/{paymentId}

    { queue              : 'contribution-report'
    , message            :
      { viewingId        : '...'
      , paymentId        : '...'
      , paymentStamp     : ...
      , surveyorId       : '...'
      , satoshis         : ...
      , fee              : ...
      , votes            : ...
      , hash             : '...'
      }
    }
 */
  'contribution-report':
    async function (debug, runtime, payload) {
      var state
      var paymentId = payload.paymentId
      var viewingId = payload.viewingId
      var contributions = runtime.db.get('contributions', debug)
      var wallets = runtime.db.get('wallets', debug)

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: underscore.omit(payload, [ 'viewingId' ])
              }
      await contributions.update({ viewingId: viewingId }, state, { upsert: true })

      state.$set = { paymentStamp: payload.paymentStamp }
      await wallets.update({ paymentId: paymentId }, state, { upsert: true })
    },

/* sent by PUT /v1/surveyor/viewing/{surveyorId}

{ queue           : 'voting-report'
, message         :
  { surveyorId    : '...'
  , publisher     : '...'
  }
}
 */
  'voting-report':
    async function (debug, runtime, payload) {
      var state
      var publisher = payload.publisher
      var surveyorId = payload.surveyorId
      var voting = runtime.db.get('voting', debug)

      if (!publisher) throw new Error('no publisher specified')

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $inc: { counts: 1 },
                $set: { exclude: false }
              }
      await voting.update({ surveyorId: surveyorId, publisher: publisher }, state, { upsert: true })
    },

/* sent when the wallet balance updates

    { queue            : 'wallet-report'
    , message          :
      { paymentId      : '...'
      , balances       : { ... }
      }
    }
 */
  'wallet-report':
    async function (debug, runtime, payload) {
      var state
      var paymentId = payload.paymentId
      var wallets = runtime.db.get('wallets', debug)

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: { balances: payload.balances }
              }
      await wallets.update({ paymentId: paymentId }, state, { upsert: true })
    }
}

module.exports = exports
