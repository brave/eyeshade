var create = require('./reports').create
var currencyCodes = require('currency-codes')
var underscore = require('underscore')
var uuid = require('uuid')

var currency = currencyCodes.code('USD')
if (!currency) currency = { digits: 2 }

var exports = {}

exports.workers = {
/* sent by POST /v1/publishers/contributions/exclude

    { queue            : 'publishers-contributions-exclude'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , exclude        :  true  | false
      , exclusionId    : '...'
      }
    }
 */
  'publishers-contributions-exclude':
    async (debug, runtime, payload) => {
      var file, i, entries, satoshis, state, vote
      var authority = payload.authority
      var excludeP = payload.exclude
      var exclusionId = payload.exclusionId
      var publishers = {}
      var tokens = runtime.db.get('tokens', debug)
      var voting = runtime.db.get('voting', debug)

      if (excludeP) {
        entries = await tokens.find({ verified: true })
        entries.forEach((entry) => { publishers[entry.publisher] = true })

        entries = await voting.find({ counts: { $gt: 0 }, exclude: false })
        satoshis = 0
        exclusionId = uuid.v4().toLowerCase()
        for (i = 0; i < entries.length; i++) {
          vote = entries[i]

          if (publishers[vote.publisher]) continue

          satoshis += vote.satoshis
          state = { $set: { exclude: true, exclusionId: exclusionId } }
          await voting.update({ surveyorId: vote.surveyorId, publisher: vote.publisher }, state, { upsert: true })
        }
      } else {
        entries = await voting.find({ exclude: true, exclusionId: exclusionId, hash: { $exists: false } })
        satoshis = 0
        entries.forEach((vote) => { satoshis += vote.satoshis })

        state = { $set: { exclude: false } }
        await voting.update({ exclusionId: exclusionId }, state, { upsert: false, multi: true })
      }

      file = await create(runtime, 'publishers-exclusions-', { format: 'json', reportId: payload.reportId })
      underscore.extend(payload, { satoshis: satoshis, exclusionId: exclusionId })
      await file.write(JSON.stringify([ payload ], null, 2), true)
      return runtime.notify(debug, {
        channel: '#publishers-bot',
        text: authority + ' publishers-contributions-exclude completed'
      })
    },

/* sent by POST /v1/publishers/contributions/exclude

    { queue            : 'publishers-contributions-exclude'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , exclusionId    : '...'
      , settlement     : [ { ... } ]
      }
    }
 */
  'publishers-contributions-prorata':
    async (debug, runtime, payload) => {
      var fee, file, remainder, satoshis, summary, total, usd
      var authority = payload.authority
      var data = payload.settlement
      var exclusionId = payload.exclusionId
      var voting = runtime.db.get('voting', debug)

      summary = await voting.aggregate([
        {
          $match:
          {
            exclusionId: { $eq: exclusionId }
          }
        },
        {
          $group:
          {
            _id: '$exclusionId',
            satoshis: { $sum: '$satoshis' }
          }
        }
      ])
      satoshis = summary.length > 0 ? summary[0].satoshis : 0
      remainder = satoshis

      total = 0
      data.forEach((datum) => { total += datum.satoshis })

      data.forEach((datum) => {
        datum.percentage = ((datum.satoshis * 100) / total).toFixed(4)
        datum.satoshis = Math.floor((datum.satoshis * satoshis) / total)
        remainder -= datum.satoshis

        if (remainder < 0) {
          datum.satoshis += remainder
          remainder = 0
        }
      })
      data.forEach((datum) => {
        if (remainder === 0) return

        datum.satoshis++
        remainder--
      })
      if (remainder > 0) data[0].satoshis += remainder

      usd = runtime.wallet.rates.USD
      usd = (Number.isFinite(usd)) ? (usd / 1e8) : null
      fee = 0
      fee = fee.toFixed(currency.digits)
      data.forEach((datum) => {
        underscore.extend(datum, {
          fees: 0,
          authority: authority,
          transactionId: exclusionId,
          amount: (datum.satoshis * usd).toFixed(currency.digits),
          fee: fee,
          currency: 'USD'
        })
      })

      file = await create(runtime, 'prorata-', { format: 'json', reportId: payload.reportId })
      await file.write(JSON.stringify(data, null, 2), true)
      return runtime.notify(debug, {
        channel: '#publishers-bot',
        text: authority + ' publishers-contributions-exclude completed'
      })
    }
}

module.exports = exports
