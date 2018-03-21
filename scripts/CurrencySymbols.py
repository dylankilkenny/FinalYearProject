#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering cryptocurrency historical data"""

import json
import requests
import csv

def CoinNames():
    names = []
    names.append(["ID", "Name", "Symbol"])
    response = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0")
    respJSON = json.loads(response.text)
    for i in respJSON:
        names.append([i['id'],i['name'], i['symbol']])
        if i['rank'] == str(250):
            Save(names)
    
def Save(rows):
    with open('CurrencySymbols.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(row for row in rows if row)

if __name__ == "__main__":
    CoinNames()

