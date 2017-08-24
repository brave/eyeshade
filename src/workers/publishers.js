const braveHapi = require('../brave-hapi')
const json2csv = require('json2csv')
const underscore = require('underscore')

const create = require('./reports.js').create

const publish = async (debug, runtime, method, publisher, endpoint, payload) => {
  const prefix = publisher ? ('/' + encodeURIComponent(publisher)) : ''
  let result

  result = await braveHapi.wreck[method](runtime.config.publishers.url + '/api/publishers/' + prefix + (endpoint || ''),
    { headers: { authorization: 'Bearer ' + runtime.config.publishers.access_token,
      'content-type': 'application/json'
    },
      payload: JSON.stringify(payload),
      useProxyP: true
    })
  if (Buffer.isBuffer(result)) result = JSON.parse(result)

  return result
}

var exports = {}

exports.initialize = async (debug, runtime) => {
  await runtime.queue.create('publishers-bulk-create')
}

exports.workers = {
/* sent by POST /v1/publishers

    { queue            : 'publishers-bulk-create'
    , message          :
      { reportId       : '...'
      , reportURL      : '...'
      , authority      : '...:...'
      , format         : 'json' | 'csv'
      , publishers     :
        [ { publisher  : '...'
          , name       : '...'
          , phone      : '...'
          , show_verification_status
                       : true | false
          }
          ...
        ]
      }
    }
 */
  'publishers-bulk-create':
    async (debug, runtime, payload) => {
      const authority = payload.authority
      const format = payload.format || 'csv'
      const publishers = payload.publishers
      const publishersC = runtime.db.get('publishers', debug)
      let file, result, state

      state = {
        $currentDate: { timestamp: { $type: 'timestamp' } },
        $set: { verified: true, reason: 'bulk loaded', authority: authority }
      }
      for (let entry of publishers) {
        try {
          result = await publish(debug, runtime, 'post', '', '', {
            publisher: underscore.extend({ brave_publisher_id: entry.publisher, verified: true },
                                         underscore.omit(entry, [ 'publisher' ]))
          })
          await publishersC.update({ publisher: entry.publisher }, state, { upsert: true })

          entry.message = result && result.message
        } catch (ex) {
          entry.message = ex.toString()
        }
      }

      file = await create(runtime, 'publishers-', payload)
      if (format === 'json') {
        await file.write(JSON.stringify(publishers, null, 2), true)
      } else {
        try { await file.write(json2csv({ data: publishers }), true) } catch (ex) {
          debug('reports', { report: 'bulk-publishers-create', reason: ex.toString() })
          file.close()
        }
      }
      runtime.notify(debug, { channel: '#publishers-bot', text: authority + ' publishers-bulk-create completed' })
    }
}

module.exports = exports
