#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering cryptocurrency historical data"""

import json
import requests
from bs4 import BeautifulSoup
import csv

def CoinNames():
    names = []
    response = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0")
    respJSON = json.loads(response.text)
    for i in respJSON:
        names.append(i['id'])
    return names
    
def gather():
    
    names = CoinNames()
    historicaldata = []
    counter = 0

    for coin in names:
        r  = requests.get("https://coinmarketcap.com/currencies/{0}/historical-data/?start=20170127&end=20180126".format(coin))
        data = r.text
        soup = BeautifulSoup(data, "html.parser")
        table = soup.find('table', attrs={ "class" : "table"})
        if len(historicaldata) == 0:
            headers = [header.text for header in table.find_all('th')]
            headers.insert(0, "Coin")
        for row in table.find_all('tr'):
            currentrow = [val.text for val in row.find_all('td')]
            currentrow.insert(0, coin)      
            historicaldata.append(currentrow)
        print(counter, end='\r')
        counter += 1

    Save(headers, historicaldata)

def Save(headers, rows):
    with open('HistoricalCoinData.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(row for row in rows if row)

if __name__ == "__main__":
    gather()

