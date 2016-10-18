module.exports =
{ port                  : process.env.PORT
, database              :
  { mongo               : process.env.MONGODB_URI }
, queue                 :
  { rsmq                : process.env.REDIS_URL }
, wallet                :
  { bitgo               :
    { environment       : process.env.BITGO_ENVIRONMENT
    }
  }
, ledger                :
  { url                 : process.env.LEDGER_URL
  , access_token        : process.env.LEDGER_TOKEN
  }
, publishers            :
  { url                 : process.env.PUBLISHERS_URL
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
