module.exports =
{ server   : require('url').parse('http://' + '127.0.0.1' + ':' + (process.env.PORT || 3002))
, database : process.env.MONGODB_URI || 'localhost/test'
, queue    : process.env.REDIS_URL   || 'localhost:6379'
}
