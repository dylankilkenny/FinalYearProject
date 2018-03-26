from RedditReducer import RedditReducer
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
import json


client = MongoClient("mongodb://localhost:27017/")
db = client.dev

# data = pd.read_csv('../data/comments_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
data = pd.read_csv('../data/small_data.csv', parse_dates=['Date'])
currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
stopwords = pd.read_csv('../data/stopwords.csv')

print("Instantiating Class with Objects...")
RedditReducer = RedditReducer(data, currency_symbols, stopwords)
print("Done.")

print("Word count..")
# word_count = RedditReducer.WordCount()
cbd = RedditReducer.CommentsByDay()
db.subreddits.insert(
    {   
        "id": "btc",
        "comments_by_day": json.loads(cbd)
    })
pprint(RedditReducer.CommentsByDay())



    

