process.env.FIXIE_URL = 'http://fixie:NsVuEyYuZbb9qf8@velodrome.usefixie.com:80'

/* utilities for Brave's HAPI servers

   not really extensive enough for its own package...

*/

var Netmask = require('netmask').Netmask
var underscore = require('underscore')
var url = require('url')
var wreck = require('wreck')

var exports = {}

exports.debug = function (info, request) {
  var sdebug = new (require('sdebug'))(info.id)

  sdebug.initialize({ request: { id: request.id } })
  return sdebug
}

var whitelist = process.env.IP_WHITELIST && process.env.IP_WHITELIST.split(',')
if (whitelist) {
  var authorizedAddrs = [ '127.0.0.1' ]
  var authorizedBlocks = []

  whitelist.forEach((entry) => {
    if ((entry.indexOf('/') !== -1) || (entry.split('.').length !== 4)) return authorizedBlocks.push(new Netmask(entry))

    authorizedAddrs.push(entry)
  })
}

var AsyncRoute = function () {
  if (!(this instanceof AsyncRoute)) return new AsyncRoute()

  this.internal = {}
  this.internal.method = 'GET'
  this.internal.path = '/'
  this.internal.extras = {}
}

AsyncRoute.prototype.get = function () {
  this.internal.method = 'GET'
  return this
}

AsyncRoute.prototype.post = function () {
  this.internal.method = 'POST'
  return this
}

AsyncRoute.prototype.put = function () {
  this.internal.method = 'PUT'
  return this
}

AsyncRoute.prototype.patch = function () {
  this.internal.method = 'PATCH'
  return this
}

AsyncRoute.prototype.delete = function () {
  this.internal.method = 'DELETE'
  return this
}

AsyncRoute.prototype.path = function (path) {
  this.internal.path = path
  return this
}

AsyncRoute.prototype.whitelist = function () {
  this.internal.extras = {
    ext: {
      onPreAuth: {
        method: require('./hapi-auth-whitelist').authenticate
      }
    }
  }

  return this
}

AsyncRoute.prototype.config = function (config) {
  if (typeof config === 'function') { config = { handler: config } }
  if (typeof config.handler === 'undefined') { throw new Error('undefined handler for ' + JSON.stringify(this.internal)) }

  return runtime => {
    var payload = { handler: { async: config.handler(runtime) } }

    underscore.keys(config).forEach(key => {
      if ((key !== 'handler') && (typeof config[key] !== 'undefined')) payload[key] = config[key]
    })

    return {
      method: this.internal.method,
      path: this.internal.path,
      config: underscore.extend(payload, this.internal.extras)
    }
  }
}

exports.routes = { async: AsyncRoute }

var ErrorInspect = function (err) {
  var i, properties

  if (!err) return

  properties = [ 'message', 'isBoom', 'isServer' ]
  if (!err.isBoom) properties.push('stack')
  i = underscore.pick(err, properties)
  if ((err.output) && (err.output.payload)) { underscore.defaults(i, { payload: err.output.payload }) }

  return i
}

exports.error = { inspect: ErrorInspect }

var WreckProxy = function (server, opts) {
  var headers, proxy, target

  opts = underscore.omit(opts, [ 'useProxyP' ])
  if (!process.env.FIXIE_URL) return { server: server, opts: opts }

  proxy = url.parse(process.env.FIXIE_URL)
  target = url.parse(server)

  server = url.format(underscore.extend(underscore.pick(proxy, [ 'protocol', 'hostname', 'port' ]), { pathname: target.href }))

  headers = underscore.clone(opts.headers || {})
  underscore.extend(headers, { host: target.host, 'proxy-authorization': 'Basic ' + new Buffer(proxy.auth).toString('base64') })
  opts = underscore.defaults(headers, opts)

  console.log('\n' + JSON.stringify({ server: server, opts: opts }, null, 2) + '\n')
  return { server: server, opts: opts }
}

var WreckGet = async function (server, opts) {
  var params = (opts) && (opts.useProxyP) ? WreckProxy(server, opts) : { server: server, opts: opts }

  return new Promise((resolve, reject) => {
    wreck.get(params.server, params.opts, (err, response, body) => {
      if (err) return reject(err)

      resolve(body)
    })
  })
}

var WreckPost = async function (server, opts) {
  var params = (opts) && (opts.useProxyP) ? WreckProxy(server, opts) : { server: server, opts: opts }

  return new Promise((resolve, reject) => {
    wreck.post(params.server, params.opts, (err, response, body) => {
      if (err) return reject(err)

      resolve(body)
    })
  })
}

var WreckPatch = async function (server, opts) {
  var params = (opts) && (opts.useProxyP) ? WreckProxy(server, opts) : { server: server, opts: opts }

  return new Promise((resolve, reject) => {
    wreck.patch(params.server, params.opts, (err, response, body) => {
      if (err) return reject(err)

      resolve(body)
    })
  })
}

exports.wreck = { get: WreckGet, patch: WreckPatch, post: WreckPost }

module.exports = exports
