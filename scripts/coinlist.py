import requests
import json
import csv

names = []
names.append(["Rank","id", "Name", "Symbol"])
response = requests.get("https://api.coinmarketcap.com/v1/ticker/?limit=0")
respJSON = json.loads(response.text)
for i in respJSON:
    n = [i['rank'], i['id'], i['name'], i['symbol']]
    names.append(n)
    print(i)


# f = open('../data/coinlist.csv', 'wt')
# try:
#     writer = csv.writer(f)
#     for i in range(len(names)):
#         print(names[i])
#         writer.writerow([names[i]])

        
# finally:
#     f.close()

with open('../data/coinlist.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerows(row for row in names if row)