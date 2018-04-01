const express = require('express')
const app = express()
const fs = require('fs')
const cors = require('cors')
const fetch = require('node-fetch')
const bodyParser = require('body-parser');
app.use(bodyParser.json()); // for parsing application/json
app.use(bodyParser.urlencoded({extended: true})); // for parsing application/x-www-form-urlencoded
app.use(cors())

// ---------  Database  ---------- //
const MongoClient = require('mongodb').MongoClient;
const url = "mongodb://localhost:27017/";
let db
MongoClient.connect(url, (err, client) => {
  if (err) return console.log(err)
  db = client.db('dev') // whatever your database name is
  app.listen(3000, () => {
    getCoins()
    console.log('listening on 3000')
  })
})
// ------------------------------ //

// --- Loading inital currency data into db --- //
function cmcReducer(item, index){
    var obj =  {
        id : item.id,
        rank : item.rank,
        name : item.name,
        symbol : item.symbol,
        price_usd : item.price_usd,
        market_cap_usd : item.market_cap_usd
    }
    return obj
} 

function getCoins(){
    fetch('https://api.coinmarketcap.com/v1/ticker/')
        .then(res => res.json())
        .then(body => {
            var obj = body.map(cmcReducer);
            db.collection("coins").insertMany(obj, function(err, res) {
                if (err) throw err;
                console.log("1 document inserted");
            })
        })
        .catch(err => console.error(err));
}
// ----------------------------------------- //
 
app.get('/', function (req, res) {
    res.send("Hello World!")
});

// Retrieves all currencies from db
app.get('/AllCurrencies', function (req, res) {
    db.collection('coins').find().toArray(function(err, results) {
        console.log(results)
        res.json(results)
        // send HTML file populated with quotes here
      }) 
});

// Retrieves one currency from db
app.post('/OneCurrency', function (req, res) {

    // Specified currency
    var query = { id : req.body.id};

    db.collection("coins").find(query).toArray(function(err, result) {
        if (err) throw err;
        // Create new object to return
        const response = {
            id : result[0].id,
            rank : result[0].rank,
            name : result[0].name,
            symbol : result[0].symbol,
            price_usd : result[0].price_usd,
            market_cap_usd : result[0].market_cap_usd,
            social_volume : result[0].social_volume,
            social_sentiment : result[0].social_sentiment
        }
        res.json(response)
    });
});

//Use this cors config for production, allowing authorized origins to connect
app.use(function (req, res, next) {
    // Website you wish to allow to connect
    var allowedOrigins = ['http://localhost:3000'];
    var origin = req.headers.origin;
    if (allowedOrigins.indexOf(origin) > -1) {
        res.setHeader('Access-Control-Allow-Origin', origin);
    }
    // Request methods you wish to allow
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
    // Request headers you wish to allow
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    // Set to true if you need the website to include cookies in the requests sent
    // to the API (e.g. in case you use sessions)
    res.setHeader('Access-Control-Allow-Credentials', true);
    // Pass to next layer of middleware
    next();
});