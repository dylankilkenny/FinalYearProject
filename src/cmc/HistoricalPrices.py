#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script to gather historical cryptocurrency data from coinmarketcap.com (cmc) """

import json
import requests
from bs4 import BeautifulSoup
import csv
import sys
from pymongo import MongoClient
import pandas as pd

import requests
from pymongo import MongoClient
import json
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017/")
db = client.dev

def CoinNames():
    """Gets ID's of all coins on cmc"""

    names = []
    response = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0")
    respJSON = json.loads(response.text)
    for i in respJSON:
        names.append([i['id'],i['symbol']])
        if int(i['rank']) >250:
            return names

def main(startdate, enddate):
    names = CoinNames()
    

    for coin, symbol in names:
        print("https://graphs2.coinmarketcap.com/currencies/{0}/{1}/{2}/".format(coin[0], startdate, enddate))
        prices = requests.get("https://graphs2.coinmarketcap.com/currencies/{0}/{1}/{2}/".format(coin[0], startdate, enddate))
        prices = json.loads(prices.text)
        if db.historical.find({'id': coin}).count() < 1:
            db.historical.insert(
                {
                "id": coin,
                "symbol": symbol
                }
            )

        update = db.historical.update_one(
            {"id": coin},
            {
                "$push":{
                    "price_usd": {
                        "$each": prices["price_usd"]
                    },
                    "market_cap_by_available_supply": {
                        "$each": prices["market_cap_by_available_supply"]
                    },
                    "price_btc":{
                        "$each": prices["price_btc"]
                    },
                    "volume_usd": {
                        "$each": prices["volume_usd"]
                    }
                }
            }
        )
        
    return  datetime.fromtimestamp(float(startdate)/1000)

if __name__ == '__main__':
    enddate = datetime.fromtimestamp(float(sys.argv[1])/1000)
    for i in range(10):
        new = enddate - timedelta(days=7)
        startdate = round(new.timestamp()* 1000)
        enddate = main(startdate, round(enddate.timestamp()* 1000))




