import requests
from pymongo import MongoClient
import json
import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client.dev

prices = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0")
prices = json.loads(prices.text)

for coin in prices:

    if int(coin["rank"]) > 250:
        break

    if db.livecoins.find({'id': coin['id']}).count() < 1:
        db.livecoins.insert(
            {"id": coin['id']})

    update = db.livecoins.update_one(
        {"id": coin['id']},
        {
            "$push": {
                "Data": {
                    "Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    "Prices": {
                        "rank": coin["rank"],
                        "name": coin["name"],
                        "symbol": coin["symbol"],
                        "price_usd": coin["price_usd"],
                        "market_cap_usd": coin["market_cap_usd"],
                        "price_btc": coin["price_btc"],
                        "24h_volume_usd": coin["24h_volume_usd"],
                        "available_supply": coin["available_supply"],
                        "total_supply": coin["total_supply"],
                        "max_supply": coin["max_supply"],
                        "percent_change_1h": coin["percent_change_1h"],
                        "percent_change_24h": coin["percent_change_24h"],
                        "percent_change_7d": coin["percent_change_7d"]
                    }
                }
            }
        }
    )
    print(coin["name"])
