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
            "sentiment_by_currency.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        oldsbc = GetSentimentByCurrency(db)
        sbc = TA.SentimentByCurrency(oldsbc)
        db.tweets.update_one(
                {
                    "sentiment_by_currency.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
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
                            "Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')),
                            "SA": sbc
                        }
                    }
                }
            )

def GetSentimentByCurrency(db):
    cursor = db.tweets.find()
    for doc in cursor:
        for day in doc["sentiment_by_currency"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d  %H:00:00')):
                return day["SA"]
    
def SetSentimentByDay(TA, db):
    cursor = db.tweets.find()
    for doc in cursor:
        if "sentiment_by_day" in doc:
            sbd = TA.SentimentByDay(doc["sentiment_by_day"])
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
        # else create new object
        else:
            sbd = TA.SentimentByDay(None)
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
                    "$set": {
                        "most_active_users": mau
                    }
                }
            )
        else:
            mau = TA.MostActiveUsers(None)
            db.tweets.update_one(
                {"id": "tweets"},
                {
                    "$set": {
                        "most_active_users": mau
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
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "wordcount_by_day" in doc:
            wcbd = TA.WordCountByDay(doc["wordcount_by_day"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "wordcount_by_day": ""} }
            )
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
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "bigram_by_day" in doc:
            bbd = TA.BigramByDay(doc["bigram_by_day"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "bigram_by_day": ""} }
            )
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$push": {
                            "bigram_by_day": {
                                "$each": bbd
                            }
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
                                "$each": bbd
                            }
                        }
                    }
                )

def GetBigramByDay(db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        for day in doc["bigram_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["counts"]
    

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
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "currency_mentions_by_day" in doc:
            cmbd = TA.CurrencyMentionsByDay(doc["currency_mentions_by_day"])
            # Remove old data
            db.tweets.update(
                {"id": "tweets"},
                { "$unset": { "currency_mentions_by_day": ""} }
            )
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$push": {
                            "currency_mentions_by_day": {
                                "$each": cmbd
                            }
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
                                "$each": cmbd
                            }
                        }
                    }
                )

def main():

    for file in listdir("../data/twitter"):
        if ".csv" in file:
            print("\n"+file)
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
                
            print("\nInstantiating twitter analyser...", end="\r")
            start = time.time()
            TA = TwitterAnalyser.TwitterAnalyser(tweets, currency_symbols, stopwords)
            end = time.time()
            print("Instantiating twitter analyser... Time elapsed: " + str(end - start))


            print("Total tweets...", end="\r")    
            start = time.time()
            SetTotalTweets(TA, db)
            end = time.time()
            print("Total tweets... Time elapsed: " + str(end - start)) 

            print("Counting bigrams...", end="\r")    
            start = time.time()
            SetBigrams(TA, db)
            end = time.time()
            print("Counting bigrams... Time elapsed: " + str(end - start)) 

            print("bigrams by day...", end="\r")     
            start = time.time()
            SetBigramsByDay(TA, db)
            end = time.time()
            print("bigrams by day... Time elapsed: " + str(end - start)) 


            print("Gathering most active users...", end="\r")    
            start = time.time()
            SetMostActiveUsers(TA, db)
            end = time.time()
            print("Gathering most active users... Time elapsed: " + str(end - start))

            print("Calcuating sentiment by day...", end="\r")    
            start = time.time()
            SetSentimentByDay(TA, db)
            end = time.time()
            print("Calcuating sentiment by day... Time elapsed: " + str(end - start))
            
            print("performing word count...", end="\r")    
            start = time.time()
            SetWordCount(TA, db)
            end = time.time()
            print("performing word count... Time elapsed: " + str(end - start))

            print("performing word count by day...", end="\r")    
            start = time.time()
            SetWordCountByDay(TA, db)
            end = time.time()
            print("erforming word count by day... Time elapsed: " + str(end - start))
                    
            print("Gathering currency mentions...", end="\r")    
            start = time.time()
            SetCurrencyMentions(TA, db)
            end = time.time()
            print("Gathering currency mentions... Time elapsed: " + str(end - start))

            print("Currency mentions by day...", end="\r")    
            start = time.time()
            SetCurrencyMentionsByDay(TA, db)
            end = time.time()
            print("Currency mentions by day... Time elapsed: " + str(end - start))
            
            print("Sentiment by currency...", end="\r")    
            start = time.time()
            SetSentimentByCurrency(TA, db)
            end = time.time()
            print("Sentiment by currency... Time elapsed: " + str(end - start))
            
            try:
                os.remove("../data/twitter/"+file)
            except OSError:
                pass

main()
