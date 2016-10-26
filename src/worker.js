process.env.NEW_RELIC_NO_CONFIG_FILE = true
if (process.env.NEW_RELIC_APP_NAME && process.env.NEW_RELIC_LICENSE_KEY) { var newrelic = require('newrelic') }
if (!newrelic) {
  newrelic = {
    createBackgroundTransaction: (name, group, cb) => { return (cb || group) },
    noticeError: (ex, params) => {},
    recordCustomEvent: (eventType, attributes) => {},
    endTransaction: () => {}
  }
}

var debug = new (require('sdebug'))('worker')
var ledgerPublisher = require('ledger-publisher')
var path = require('path')
var underscore = require('underscore')

var npminfo = require(path.join(__dirname, '..', 'package'))
var runtime = require('./runtime.js')
runtime.newrelic = newrelic

var main = async function (id) {
  debug.initialize({ worker: { id: id } })

  runtime.npminfo = underscore.pick(npminfo, 'name', 'version', 'description', 'author', 'license', 'bugs', 'homepage')
  runtime.npminfo.children = {}
  runtime.notify(debug, { text: require('os').hostname() + ' ' + npminfo.name + '@' + npminfo.version +
                                  ' started ' + (process.env.DYNO || '') + '/' + id })

  await runtime.queue.create('prune-publishers')
  runtime.queue.listen('prune-publishers',
    runtime.newrelic.createBackgroundTransaction('prune-publishers', async function (err, debug, payload) {
/* sent by POST /v1/publishers/prune

    { queue            : 'prune-publishers'
    , message          :
      {
      }
    }
 */

      var report

      if (err) return debug('prune-publishers listen', err)

      report = async function () {
        var results, state, votes
        var voting = runtime.db.get('voting', debug)

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

        runtime.notify(debug, { text: 'pruned ' + JSON.stringify(results, null, 2) })
      }

      try { await report() } catch (ex) {
        debug('prune-publishers', { payload: payload, err: ex, stack: ex.stack })
        runtime.newrelic.noticeError(ex, payload)
      }
      runtime.newrelic.endTransaction()

      debug('prune-publishers', { message: 'restarting' })
      process.exit(0)
    })
  )
}

main(1)
