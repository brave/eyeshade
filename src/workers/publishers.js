var ledgerPublisher = require('ledger-publisher')
var underscore = require('underscore')

var exports = {}

var domainSort = function (domains) {
  var result = []

  domains.forEach(function (domain) { result.push(domain.split('.').reverse()) })
  result.sort()

  domains = []
  result.forEach(function (domain) { domains.push(domain.reverse().join('.')) })

  return domains
}

exports.workers = {
/* sent by POST /v1/publishers/prune

    { queue            : 'prune-publishers'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , reset          : true | false
      , test           : true | false
      }
    }
 */
  'prune-publishers':
    async function (debug, runtime, payload) {
      var file, results, state, votes
      var authority = payload.authority
      var reportId = payload.reportId
      var reset = payload.reset
      var test = payload.test
      var voting = runtime.db.get('voting', debug)

      file = await runtime.db.file(reportId, 'w', { content_type: 'application/json' })

      votes = await voting.aggregate([
          { $match: { counts: { $gt: 0 },
                      exclude: reset
                    }
          },
          { $group: { _id: '$publisher' } },
          { $project: { _id: 1 } }
      ])
      state = { $currentDate: { timestamp: { $type: 'timestamp' } },
                $set: { exclude: !reset }
              }

      results = []
      if (reset) {
        votes.forEach(async function (entry) {
          var publisher = entry._id

          results.push(publisher)
          if (!test) await voting.update({ publisher: publisher }, state, { upsert: false, multi: true })
        })

        await file.write(JSON.stringify(results, null, 2), true)
        runtime.notify(debug, { channel: '#publishers-bot',
                                text: authority + ' prune-publishers completed, count: ' + results.length })
        return
      }

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
        if (!test) await voting.update({ publisher: publisher }, state, { upsert: false, multi: true })
      })

      await file.write(JSON.stringify(domainSort(results), null, 2), true)
      runtime.notify(debug, { channel: '#publishers-bot',
                              text: authority + ' prune-publishers completed, count: ' + results.length })
    }
}

module.exports = exports
