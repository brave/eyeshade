var bson = require('bson')

// var v1 = {}

module.exports.routes = []

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('wallets', debug),
      name: 'wallets',
      property: 'paymentId',
      empty: { paymentId: '',
               address: '',
               provider: '',
               balances: {},
               keychains: {},
               paymentStamp: 0,
               timestamp: bson.Timestamp.ZERO
             },
      unique: [ { paymentId: 0 } ],
      others: [ { address: 0 }, { provider: 1 }, { paymentStamp: 1 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('surveyors', debug),
      name: 'surveyors',
      property: 'surveyorId',
      empty: { surveyorId: '',
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
      unique: [ { surveyorId: 0 } ],
      others: [ { surveyorType: 1 }, { satoshis: 1 }, { votes: 1 }, { counts: 1 }, { timestamp: 1 },
                { inputs: 1 }, { fee: 1 }, { quantum: 1 } ]
    },
    { category: runtime.db.get('contributions', debug),
      name: 'contributions',
      property: 'viewingId',
      empty: { viewingId: '',
               paymentId: '',
               paymentStamp: 0,
               surveyorId: '',
               satoshis: 0,
               fee: 0,
               votes: 0,
               hash: '',
               timestamp: bson.Timestamp.ZERO
             },
      unique: [ { viewingId: 0 } ],
      others: [ { paymentId: 0 }, { paymentStamp: 1 }, { surveyorId: 0 }, { satoshis: 1 }, { fee: 1 },
                { votes: 1 }, { hash: 0 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('voting', debug),
      name: 'voting',
      property: 'surveyorId_0_publisher',
      empty: { surveyorId: '',
               publisher: '',
               counts: 0,
               timestamp: bson.Timestamp.ZERO,
               // added by administrator
               exclude: false,
               // added during report runs...
               satoshis: 0
             },
      unique: [ { surveyorId: 0, publisher: 1 } ],
      others: [ { counts: 1 }, { timestamp: 1 },
                { exclude: 1 },
                { satoshis: 1 } ]
    }
  ])
}
