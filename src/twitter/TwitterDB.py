#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import TwitterAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
from os import listdir
import datetime

def GetTweetDocument(db):
    cursor = db.tweets.find()
    return cursor

def GetCoinDocument(db, coin):
    cursor = db.coins.find(
        {"id": coin}
    )
    return cursor

def SetTotalTweets(TA, db):
    total_tweets = TA.TotalTweets()
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "total_tweets" in doc:
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$set": {
                        "total_tweets": total_tweets + doc["total_tweets"]
                    }
                }
            )
        else:
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$set": {
                        "total_tweets": total_tweets
                    }
                }
            )

def SetSentimentByCurrency(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "sentiment_by_currency.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        oldsbc = GetSentimentByCurrency(db)
        sbc = TA.SentimentByCurrency(oldsbc)
        db.tweets.update_one(
                {
                    "sentiment_by_currency.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "sentiment_by_currency.$.SA": sbc
                    }
                }
            )
    # else create new object
    else:
        sbc = TA.SentimentByCurrency(None)
        db.tweets.update_one(
                {
                    "id": "tweets"
                },
                {
                    "$push": {
                        "sentiment_by_currency": {
                            "Date": str(datetime.datetime.now().strftime('%Y-%m-%d')),
                            "SA": sbc
                        }
                    }
                }
            )

def GetSentimentByCurrency(db):
    cursor = db.tweets.find()
    for doc in cursor:
        for day in doc["sentiment_by_currency"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["SA"]
    
def SetSentimentByDay(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "sentiment_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        olds = GetSentimentByDay(db)
        sbd = TA.SentimentByDay(olds)
        db.subreddits.update_one(
                {
                    "sentiment_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
                },
                {
                    "$set": {
                        "sentiment_by_day.$.SA": sbd[0]["SA"]
                    }
                }
            )
    # else create new object
    else:
        sbd = TA.SentimentByDay(0)
        db.tweets.update_one(
                {
                    "id": "tweets"
                },
                {
                    "$set": {
                        "sentiment_by_day": sbd
                    }
                }
            )

def GetSentimentByDay(db):
    cursor = db.tweets.find()
    for doc in cursor:
        for day in doc["sentiment_by_day"]:
            if day["Date"] == str(datetime.datetime.now().date()):
                return day["SA"]

def SetMostActiveUsers(TA, db):
    
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "most_active_users" in doc:
            mau = TA.MostActiveUsers(doc["most_active_users"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "most_active_users": ""} }
            )
            # Add new data
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "most_active_users": {
                            "$each": mau,
                            "$sort": { "Activity": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )
        else:
            mau = TA.MostActiveUsers(None)
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "most_active_users": {
                            "$each": mau,
                            "$sort": { "Activity": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )

def SetWordCount(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "word_count" in doc:
            wc = TA.WordCount(doc["word_count"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "word_count": ""} }
            )
            # Add new data
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "word_count": {
                            "$each": wc,
                            "$sort": { "n": -1 },
                            "$slice": 500
                        }
                    }
                }
            )
        else:
            wc = TA.WordCount(None)
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "word_count": {
                            "$each": wc,
                            "$sort": { "n": -1 },
                            "$slice": 500
                        }
                    }
                }
            )

def SetWordCountByDay(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "wordcount_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        print(True)
        oldwordcount = GetWordCountByDay(db)
        wcbd = TA.WordCountByDay(oldwordcount)
        db.tweets.update_one(
                {
                    "id": "tweets",
                    "wordcount_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "wordcount_by_day.$.counts": wcbd
                    }
                }
            )
    # else create new object
    else:
        wcbd = TA.WordCountByDay(None)
        db.tweets.update_one(
                {
                    "id": "tweets"
                },
                {
                    "$push": {
                        "wordcount_by_day": {
                            "$each": wcbd
                        }
                    }
                }
            )

def GetWordCountByDay(db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        for day in doc["wordcount_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["counts"]

def SetBigrams(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "bigram_count" in doc:
            bc = TA.Bigram(doc["bigram_count"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "bigram_count": ""} }
            )
            # Add new data
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "bigram_count": {
                            "$each": bc,
                            "$sort": { "n": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )
        else:
            bc = TA.Bigram(None)
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$push": {
                        "bigram_count": {
                            "$each": bc,
                            "$sort": { "n": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )

def SetBigramsByDay(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "bigram_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        oldbigrams = GetBigramByDay(db)
        bbd = TA.BigramByDay(oldbigrams)
        db.tweets.update_one(
                {
                    "id": "tweets",
                    "bigram_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "bigram_by_day.$.bigrams": bbd
                    }
                }
            )
    # else create new object
    else:
        bbd = TA.BigramByDay(None)
        db.tweets.update_one(
                {
                    "id": "tweets"
                },
                {
                    "$push": {
                        "bigram_by_day": {
                            "Date": str(datetime.datetime.now().strftime('%Y-%m-%d')),
                            "bigrams": bbd
                        },
                        
                    }
                }
            )

def GetBigramByDay(db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        for day in doc["bigram_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["bigrams"]
    

def SetCurrencyMentions(TA, db):
    
    cursor = GetTweetDocument(db)
    for doc in cursor:
        
        if "currency_mentions" in doc:
            cm = TA.CurrencyMentions(doc["currency_mentions"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "currency_mentions": ""} }
            )
            # Add new data
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$set": {
                        "currency_mentions": cm
                    }
                }
            )
        else:
            cm = TA.CurrencyMentions(None)
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$set": {
                        "currency_mentions": cm
                    }
                }
            )

def SetCurrencyMentionsByDay(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "currency_mentions_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        oldcmbd = GetCurrencyMentionsByDay(db)
        cmbd = TA.CurrencyMentionsByDay(oldcmbd)
        db.tweets.update_one(
                {
                    "id": "tweets",
                    "currency_mentions_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "currency_mentions_by_day.$.mentions": cmbd
                    }
                }
            )
    # else create new object
    else:
        cmbd = TA.CurrencyMentionsByDay(None)
        db.tweets.update_one(
                {
                    "id": "tweets"
                },
                {
                    "$push": {
                        "currency_mentions_by_day": {
                            "Date": str(datetime.datetime.now().strftime('%Y-%m-%d')),
                            "mentions": cmbd
                        },
                        
                    }
                }
            )

def GetCurrencyMentionsByDay(db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        for day in doc["currency_mentions_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["mentions"]

def main():

    for file in listdir("../data/twitter"):
        if ".csv" in file:
            print(file)
            # Connect to DB
            client = MongoClient("mongodb://localhost:27017/")
            db = client.dev


            # Load Data
            tweets = pd.read_csv("../data/twitter/"+file, parse_dates=['Date'])
            currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
            stopwords = pd.read_csv('../data/stopwords.csv')


            # Check if currency is already in subreddits
            if db.tweets.find({'id': "tweets"}).count() < 1:
                db.tweets.insert(
                    {"id": "tweets"})
                
            SetTotalTweets
            print("\nInstantiating twitter analyser...")
            start = time.time()
            TA = TwitterAnalyser.TwitterAnalyser(tweets, currency_symbols, stopwords)
            end = time.time()
            print("Done | Time elapsed: " + str(end - start))
            print()

            # print("Total tweets...", end="\r")    
            # start = time.time()
            # SetTotalTweets(TA, db)
            # end = time.time()
            # print("Time elapsed: " + str(end - start)) 

            # print("Counting bigrams...", end="\r")    
            # start = time.time()
            # SetBigrams(TA, db)
            # end = time.time()
            # print("Time elapsed: " + str(end - start)) 

            # print("bigrams by day...")     
            # start = time.time()
            # SetBigramsByDay(TA, db)
            # end = time.time()
            # print("Time elapsed: " + str(end - start)) 


            # print("Gathering most active users...")    
            # start = time.time()
            # SetMostActiveUsers(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))
            # print()

            # print("Calcuating sentiment by day...")    
            # start = time.time()
            # SetSentimentByDay(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))
            # print()
            
            # print("performing word count...")    
            # start = time.time()
            # SetWordCount(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))
            # print()

            print("performing word count by day...")    
            start = time.time()
            SetWordCountByDay(TA, db)
            end = time.time()
            print("Time elapsed: " + str(end - start))
                    
            # print("Gathering currency mentions...")    
            # start = time.time()
            # SetCurrencyMentions(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))

            # print("Currency mentions by day...")    
            # start = time.time()
            # SetCurrencyMentionsByDay(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))
            
            # print("Sentiment by currency...")    
            # start = time.time()
            # SetSentimentByCurrency(TA, db)
            # end = time.time()
            # print("Done | Time elapsed: " + str(end - start))
            # SetSentimentByCurrency
            
            # try:
            #     os.remove("../data/twitter/"+file)
            # except OSError:
            #     pass

main()
