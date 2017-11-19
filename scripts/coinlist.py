import requests
import json
import csv

coinlist = []
url = "https://www.cryptocompare.com/api/data/coinlist/"
r = requests.get(url)
resp = json.loads(r.text)
for coin in resp["Data"]:
    coinlist.append(coin)

print(coinlist)

f = open('../data/coinlist.csv', 'wt')
try:
    writer = csv.writer(f)
    for i in range(len(coinlist)):
        print(coinlist[i])
        writer.writerow([coinlist[i]])
        
finally:
    f.close()