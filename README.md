# Brave Eyeshade
** NB: this repository is still undergoing active functional development.**

The Brave Eyeshade is the back-end accountant for the
[Brave Ledger](https://github.com/brave/ledger/tree/master/documentation/Ledger-Principles.md).

Note that [travis-ci](https://travis-ci.org/brave/eyeshade) is not yet operational for this repository.

# Initialization
Take a look at the files in the `config/` directory.
When the server starts,
it will look file a file called `config/config.{PROFILE}.js` where `{PROFILE}` is `$NODE_ENV` (defaulting to `"development"`).

Authentication is achieved via a GitHub [OAuth application](https://github.com/settings/developers).
Create a developer application with an authorization callback of the form `https://{DOMAIN:PORT}/v1/login` and update the
`login.clientId` and `login.clientSecret` properties.

Authorization is achieved by verifying that the user is a member of a GitHub organization, i.e.,
`https://github.com/orgs/{ORGANIZATION}/teams`.
Set the `login.organization` property to the name of the organization.

Now start the server with `npm start` and `https://{DOMAIN:PORT}/v1/login` which will start the authentication/authorization
process.
On success,
you will be redirected to `https://{DOMAIN:PORT}/documentation`.

# Setup
Clone the repo: `git clone git@github.com:brave/eyeshade.git`

Install dependencies with `npm install`

Install MongoDB: `brew update && brew install mongodb`

Start MongoDB. There are a variety of ways to do this, one option on a mac: `brew tap homebrew/services && brew services start mongodb`

Install Redis: `brew update && brew install redis`

Start Redis. There are a variety of ways to do this, one option on a mac: `brew tap homebrew/services && brew services start redis`

## StandardJS

For linting we use [StandardJS](https://github.com/feross/standard). It's recommended that you install the necessary IDE plugin. Since this repo uses ES7 features, you'll need a global install of both the standard and babel-eslint packages.

## Configuration

For staging or production environments configuration variables are stored as environment preferences. See config/config.production.js for a list of these variables.

For local development you can copy config/config.development.js.tpl to config/config.development.js and define the local config variables.

## Running the server

Use `gulp` to run the server in development. This also sets up watchers and will restart the server on a file change.

## Proximo

Proximo is currently used as a proxy so we can make outbound BitGo requests using a static IP. 
Please define `process.env.BITGO_USE_PROXY` as appropriate.
