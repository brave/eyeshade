module.exports =
{ port     : process.env.PORT || 3002
, database : process.env.MONGODB_URI || 'localhost/test'
, queue    : process.env.REDIS_URL || 'localhost:6379'
}
