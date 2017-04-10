module.exports =
{ server                : require('url').parse('https://' + process.env.HOST)
, database              :
  { mongo               : process.env.MONGODB_URI }
, queue                 :
  { rsmq                : process.env.REDIS_URL }
, wallet                :
  { bitgo               :
    { accessToken       : process.env.BITGO_TOKEN
    , enterpriseId      : process.env.BITGO_ENTERPRISE_ID
    , environment       : process.env.BITGO_ENVIRONMENT
    , fundingAddress    : process.env.BITGO_FUNDING_ADDRESS
    , fundingPassphrase : process.env.BITGO_FUNDING_PASSPHRASE
    }
  , bitcoin_average     :
    { publicKey         : process.env.BITCOIN_AVERAGE_PUBLIC_KEY
    , secretKey         : process.env.BITCOIN_AVERAGE_SECRET_KEY
    }
  }
, funding               :
  { url                 : process.env.FUNDING_URL
  , access_token        : process.env.FUNDING_TOKEN
  }
, publishers            :
  { url                 : process.env.PUBLISHERS_URL
  , access_token        : process.env.PUBLISHERS_TOKEN
  }
, slack                 :
  { webhook             : process.env.SLACK_WEBHOOK
  , channel             : process.env.SLACK_CHANNEL
  , icon_url            : process.env.SLACK_ICON_URL
  }
, login                 :
  { organization        : 'brave'
  , world               : '/documentation'
  , bye                 : 'https://brave.com'
  , clientId            : process.env.GITHUB_CLIENT_ID
  , clientSecret        : process.env.GITHUB_CLIENT_SECRET
  , ironKey             : process.env.IRON_KEYPASS
  , isSecure            : true
  }
}
