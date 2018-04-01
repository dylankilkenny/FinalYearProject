#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering cryptocurrency subreddits"""

import json
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
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
    counter = 0
    data = []
    data.append(["Currency","Subreddit"])

    for coin in names:
        browser = webdriver.PhantomJS()
        browser.get("https://coinmarketcap.com/currencies/{0}/#social".format(coin))
        html = browser.page_source
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all('a', href=True):
            if "reddit.com" in a["href"]:
                data.append([coin, a["href"].split('/')[4]])
                break

        print(counter)
        counter += 1

        if(counter > 250):
            break

    Save(data)

def Save(data):
    with open('SubredditList.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(d for d in data if d)

if __name__ == "__main__":
    gather()

