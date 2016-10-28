var ledgerPublisher = require('ledger-publisher')
var underscore = require('underscore')

var exports = {}

exports.workers = {
/* sent by POST /v1/publishers/prune

    { queue            : 'prune-publishers'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      }
    }
 */
  'prune-publishers':
    async function (debug, runtime, payload) {
      var file, results, state, votes
      var reportId = payload.reportId
      var reportURL = payload.reportURL
      var voting = runtime.db.get('voting', debug)

      file = await runtime.db.file(reportId, 'w', { content_type: 'application/json' })

      votes = await voting.aggregate([
          { $match: { counts: { $gt: 0 },
                      exclude: false
                    }
          },
          { $group: { _id: '$publisher' } },
          { $project: { _id: 1 } }
      ])

      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: { exclude: true }
              }

      results = []
      votes.forEach(async function (entry) {
        var publisher = entry._id
        var result

        try {
          result = ledgerPublisher.getPublisher('https://' + publisher)
          if (result) return
        } catch (err) {
          return debug('prune', underscore.defaults({ publisher: publisher }, err))
        }

        results.push(publisher)
        await voting.update({ publisher: publisher }, state, { upsert: false, multi: true })
      })

      await file.write(JSON.stringify(results, null, 2), true)
      runtime.notify(debug, { text: 'created ' + reportURL })
    }
}

module.exports = exports
