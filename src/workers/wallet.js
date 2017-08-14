var bson = require('bson')
var underscore = require('underscore')

var exports = {}

exports.initialize = async (debug, runtime) => {
  runtime.db.checkIndices(debug, [
    {
      category: runtime.db.get('wallets', debug),
      name: 'wallets',
      property: 'paymentId',
      empty: {
        paymentId: '',
        address: '',
        provider: '',
        balances: {},
        keychains: {},
        paymentStamp: 0,
        timestamp: bson.Timestamp.ZERO
      },
      unique: [ { paymentId: 1 } ],
      others: [ { address: 1 }, { provider: 1 }, { paymentStamp: 1 }, { timestamp: 1 } ]
    },
    {
      category: runtime.db.get('surveyors', debug),
      name: 'surveyors',
      property: 'surveyorId',
      empty: {
        surveyorId: '',
        surveyorType: '',
        satoshis: 0,
        votes: 0,
        counts: 0,
        timestamp: bson.Timestamp.ZERO,
        // added during report runs...
        inputs: 0,
        fee: 0,
        quantum: 0
      },
      unique: [ { surveyorId: 1 } ],
      others: [ { surveyorType: 1 }, { satoshis: 1 }, { votes: 1 }, { counts: 1 }, { timestamp: 1 },
                { inputs: 1 }, { fee: 1 }, { quantum: 1 } ]
    },
    {
      category: runtime.db.get('contributions', debug),
      name: 'contributions',
      property: 'viewingId',
      empty: {
        viewingId: '',
        paymentId: '',
        address: '',
        paymentStamp: 0,
        surveyorId: '',
        satoshis: 0,
        fee: 0,
        votes: 0,
        hash: '',
        timestamp: bson.Timestamp.ZERO
      },
      unique: [ { viewingId: 1 } ],
      others: [ { paymentId: 1 }, { address: 1 }, { paymentStamp: 1 }, { surveyorId: 1 }, { satoshis: 1 }, { fee: 1 },
                { votes: 1 }, { hash: 1 }, { timestamp: 1 } ]
    },
    {
      category: runtime.db.get('voting', debug),
      name: 'voting',
      property: 'surveyorId_1_publisher',
      empty: {
        surveyorId: '',
        publisher: '',
        counts: 0,
        timestamp: bson.Timestamp.ZERO,
        // added by administrator
        exclude: false,
        hash: '',
        // added during report runs...
        satoshis: 0
      },
      unique: [ { surveyorId: 1, publisher: 1 } ],
      others: [ { counts: 1 }, { timestamp: 1 },
                { exclude: 1 }, { hash: 1 },
                { satoshis: 1 } ]
    }
  ])
}

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
    async (debug, runtime, payload) => {
      var state
      var paymentId = payload.paymentId
      var wallets = runtime.db.get('wallets', debug)

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
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
    async (debug, runtime, payload) => {
      var state
      var surveyorId = payload.surveyorId
      var surveyors = runtime.db.get('surveyors', debug)

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
        $set: underscore.extend({ counts: 0 }, underscore.omit(payload, [ 'surveyorId' ]))
      }
      await surveyors.update({ surveyorId: surveyorId }, state, { upsert: true })
    },

/* sent by PUT /v1/wallet/{paymentId}

    { queue              : 'contribution-report'
    , message            :
      { viewingId        : '...'
      , paymentId        : '...'
      , address          : '...'
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
    async (debug, runtime, payload) => {
      var state
      var paymentId = payload.paymentId
      var viewingId = payload.viewingId
      var contributions = runtime.db.get('contributions', debug)
      var wallets = runtime.db.get('wallets', debug)

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
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
    async (debug, runtime, payload) => {
      var state
      var publisher = payload.publisher
      var surveyorId = payload.surveyorId
      var voting = runtime.db.get('voting', debug)

      if (!publisher) throw new Error('no publisher specified')

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
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
    async (debug, runtime, payload) => {
      var state
      var paymentId = payload.paymentId
      var wallets = runtime.db.get('wallets', debug)

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
        $set: { balances: payload.balances }
      }
      await wallets.update({ paymentId: paymentId }, state, { upsert: true })
    }
}

module.exports = exports
