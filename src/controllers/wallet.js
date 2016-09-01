var bson = require('bson')
var underscore = require('underscore')

// var v1 = {}

module.exports.routes = []

module.exports.initialize = async function (debug, runtime) {
  runtime.db.checkIndices(debug,
  [ { category: runtime.db.get('wallets', debug),
      name: 'wallets',
      property: 'paymentId',
      empty: { paymentId: '', address: '', provider: '', keychains: {}, paymentStamp: 0, timestamp: bson.Timestamp.ZERO },
      unique: [ { paymentId: 0 } ],
      others: [ { address: 0 }, { provider: 1 }, { paymentStamp: 1 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('surveyors', debug),
      name: 'surveyors',
      property: 'surveyorId',
      empty: { surveyorId: '', surveyorType: '', satoshis: 0, votes: 0, timestamp: bson.Timestamp.ZERO,
      // added during report runs...
               counts: 0, inputs: 0, fee: 0, quantum: 0 },
      unique: [ { surveyorId: 0 } ],
      others: [ { surveyorType: 1 }, { satoshis: 1 }, { votes: 1 }, { timestamp: 1 },
                { counts: 1 }, { inputs: 1 }, { fee: 1 }, { quantum: 1 } ]
    },
    { category: runtime.db.get('contributions', debug),
      name: 'contributions',
      property: 'viewingId',
      empty: { viewingId: '', paymentId: '', paymentStamp: 0, surveyorId: '', satoshis: 0, fee: 0,
               votes: 0, hash: '', timestamp: bson.Timestamp.ZERO },
      unique: [ { viewingId: 0 } ],
      others: [ { paymentId: 0 }, { paymentStamp: 1 }, { surveyorId: 0 }, { satoshis: 1 }, { fee: 1 },
                { votes: 1 }, { hash: 0 }, { timestamp: 1 } ]
    },
    { category: runtime.db.get('voting', debug),
      name: 'voting',
      property: 'surveyorId_0_publisher',
      empty: { surveyorId: '', publisher: '', counts: 0, timestamp: bson.Timestamp.ZERO,
      // added during report runs...
               satoshis: 0 },
      unique: [ { surveyorId: 0, publisher: 1 } ],
      others: [ { counts: 1 }, { timestamp: 1 },
                { satoshis: 1 } ]
    }
  ])

  await runtime.queue.create('persona-report')
  runtime.queue.listen('persona-report',
    runtime.newrelic.createBackgroundTransaction('persona-report', async function (err, debug, payload) {
/* sent by POST /v1/registrar/persona/{personaId}

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

      var report

      if (err) return debug('persona-report listen', err)

      report = async function () {
        var state
        var paymentId = payload.paymentId
        var wallets = runtime.db.get('wallets', debug)

        state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                  $set: underscore.extend({ paymentStamp: 0 }, underscore.omit(payload, [ 'paymentId' ]))
                }
        await wallets.update({ paymentId: paymentId }, state, { upsert: true })
      }

      try { await report() } catch (ex) {
        debug('persona-report', { payload: payload, err: ex, stack: ex.stack })
        runtime.newrelic.noticeError(ex, payload)
      }
      runtime.newrelic.endTransaction()
    })
  )

  await runtime.queue.create('surveyor-report')
  runtime.queue.listen('surveyor-report',
    runtime.newrelic.createBackgroundTransaction('surveyor-report', async function (err, debug, payload) {
/* sent by POST /v1/surveyor/contribution, PATCH /v1/surveyor/contribution/{surveyorId}, and daily()

    { queue            : 'surveyor-report'
    , message          :
      { surveyorId     : '...'
      , surveyorType   : '...'
      , satoshis       : ...
      , votes          : ...
      }
    }
 */

      var report

      if (err) return debug('surveyor-report listen', err)

      report = async function () {
        var state
        var surveyorId = payload.surveyorId
        var surveyors = runtime.db.get('surveyors', debug)

        state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                  $set: underscore.omit(payload, [ 'surveyorId' ])
                }
        await surveyors.update({ surveyorId: surveyorId }, state, { upsert: true })
      }

      try { await report() } catch (ex) {
        debug('surveyor-report', { payload: payload, err: ex, stack: ex.stack })
        runtime.newrelic.noticeError(ex, payload)
      }
      runtime.newrelic.endTransaction()
    })
  )

  await runtime.queue.create('contribution-report')
  runtime.queue.listen('contribution-report',
    runtime.newrelic.createBackgroundTransaction('contribution-report', async function (err, debug, payload) {
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

      var report

      if (err) return debug('contribution-report listen', err)

      report = async function () {
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
      }

      try { await report() } catch (ex) {
        debug('walletUpdate', { payload: payload, err: ex, stack: ex.stack })
        runtime.newrelic.noticeError(ex, payload)
      }
      runtime.newrelic.endTransaction()
    })
  )

  await runtime.queue.create('voting-report')
  runtime.queue.listen('voting-report',
    runtime.newrelic.createBackgroundTransaction('voting-report', async function (err, debug, payload) {
/* sent by PUT /v1/surveyor/viewing/{surveyorId}

{ queue           : 'voting-report'
, message         :
  { surveyorId    : '...'
  , publisher     : '...'
  }
}
 */

      var report

      if (err) return debug('voting-report listen', err)

      report = async function () {
        var state
        var publisher = payload.publisher
        var surveyorId = payload.surveyorId
        var voting = runtime.db.get('voting', debug)

        if (!publisher) throw new Error('no publisher specified')

        state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                  $inc: { counts: 1 }
                }
        await voting.update({ surveyorId: surveyorId, publisher: publisher }, state, { upsert: true })
      }

      try { await report() } catch (ex) {
        debug('voting-report', { payload: payload, err: ex, stack: ex.stack })
        runtime.newrelic.noticeError(ex, payload)
      }
      runtime.newrelic.endTransaction()
    })
  )
}
