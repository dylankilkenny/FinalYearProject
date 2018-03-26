from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient("mongodb://localhost:27017/")
db=client.dev
# Issue the serverStatus command and print the results
fivestarcount = db.coins.find()
for document in fivestarcount:
    pprint(document)