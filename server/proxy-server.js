const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();
const port = 3001;
const { OAuth2Client } = require('google-auth-library');
const uuid =  require('uuid');
const { QueryProcessor } = require('./query_processor');


app.use(cors({
  credentials: true,
}));

// parse application/json
app.use(bodyParser.json());

function checkAuthentication(auth) {
  return new Promise((resolve, _) => {
    // Resole to ture (authenticate) if there's no Google Client ID 
    if (!process.env.GOOGLE_CLIENT_ID)  {
      resolve(true);
      return;
    }

    let googleSession = JSON.parse(auth);
    if (!googleSession.clientId || !googleSession.credential) {
      resolve(false);
      return;
    }
    // Using our own google client ID ensures that we *only* accept Google Id tokens that
    // are valid for our application
    const client = new OAuth2Client(process.env.GOOGLE_CLIENT_ID);

    client.verifyIdToken({
      idToken: googleSession.credential,
      audience: process.env.GOOGLE_CLIENT_ID}).then(ticket => {
        resolve(true);
        return;
      }).catch(err=> {
        resolve(false)
      })
  })
}

app.post('/query', (req, res, next) => {
  req.id = uuid.v4();
  checkAuthentication(req.body.auth).then(isAuth => {
    if (!isAuth)  {
      res.sendStatus(401);    
    }
    else {
      new QueryProcessor(req, res, next).performQuery();
    }
  });
});

// Makes the app less brittle so that it doesn't crash when there's a timeout or a request error
app.on('uncaughtException', (err) => {
  console.log('Error while processing request')
  console.log(err);
  next(err)
})

app.listen(port, () => console.log(`Simple gremlin-proxy server listening on port ${port}!`));
