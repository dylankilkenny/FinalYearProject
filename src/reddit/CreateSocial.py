import pandas as pd
from pymongo import MongoClient

# Connect to DB
client = MongoClient("mongodb://localhost:27017/")
db = client.dev

c = pd.read_csv('../data/CurrencySymbols.csv')
for coin in c["Symbol"]:
    
    db.social.insert_one({"id": coin})