#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""
import sys
sys.path.append('../utility')
from logger import log

import TwitterAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
from os import listdir
import datetime
from bson import ObjectId


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

def QuerySBD(db, _id):
    sbd = db.sentimentbd.find(
        {
            "_id": _id
        },
        {
            "sentiment_by_day": 1
        }
    )
    for doc in sbd:
       return doc["sentiment_by_day"]
    
def SetSentimentByDay(TA, db):
    cursor = db.tweets.find()
    for doc in cursor:
        if "sentiment_by_day" in doc:
            _id =  ObjectId(doc["sentiment_by_day"])
            sbd_prev = QuerySBD(db, _id)                               
            sbd = TA.SentimentByDay(sbd_prev)
            # Remove old data
            db.sentimentbd.update(
                {"_id": _id},
                { "$unset": { "sentiment_by_day": ""} }
            )
            db.sentimentbd.update_one(
                    {
                        "_id": _id
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
            objectid = ObjectId()
            wcbdresult = db.sentimentbd.insert_one(
                        {      
                            "_id": objectid,
                            "sentiment_by_day": sbd
                        }
                    ) 
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$set": {
                            "sentiment_by_day": objectid
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

def QueryWCBD(db, _id):
    wcbd = db.wordcountbd.find(
        {
            "_id": _id
        },
        {
            "wordcount_by_day": 1
        }
    )
    for doc in wcbd:
       return doc["wordcount_by_day"]

def SetWordCountByDay(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "wordcount_by_day" in doc:
            _id =  ObjectId(doc["wordcount_by_day"])
            wcbd_prev = QueryWCBD(db, _id)                                 
            
            wcbd = TA.WordCountByDay(wcbd_prev)
            # Remove old data
            db.wordcountbd.update(
                {"_id": _id},
                { "$unset": { "wordcount_by_day": ""} }
            )
            db.wordcountbd.update_one(
                    {
                        "_id": _id
                    },
                    {
                        "$set": {
                            "wordcount_by_day": wcbd
                        }
                    }
                )
        # else create new object
        else:
            wcbd = TA.WordCountByDay(None)
            objectid = ObjectId()
            wcbdresult = db.wordcountbd.insert_one(
                        {      
                            "_id": objectid,
                            "wordcount_by_day": wcbd
                        }
                    )
            print(wcbdresult)                                
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$set": {
                            "wordcount_by_day": objectid
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

def QueryBBD(db, _id):
    bbd = db.bigramsbd.find(
        {
            "_id": _id
        },
        {
            "bigram_by_day": 1
        }
    )
    for doc in bbd:
       return doc["bigram_by_day"]

def SetBigramsByDay(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "bigram_by_day" in doc:
            _id =  ObjectId(doc["bigram_by_day"])
            bbd_prev = QueryBBD(db, _id)                     
            
            bbd = TA.BigramByDay(bbd_prev)
            # Remove old data
            db.bigramsbd.update(
                {"_id": _id},
                { "$unset": { "bigram_by_day": ""} }
            )
            db.bigramsbd.update_one(
                    {
                        "_id": _id
                    },
                    {
                        "$set": {
                            "bigram_by_day": bbd
                        }
                    }
                )
            
        # else create new object
        else:
            bbd = TA.BigramByDay(None)
            objectid = ObjectId()      
            bbdresult = db.bigramsbd.insert_one(
                        {      
                            "_id": objectid,
                            "bigram_by_day": bbd
                        }
                    )
            print(bbdresult)
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$set": {
                            "bigram_by_day": objectid
                        }
                    }
                )
    

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

def QueryCMBD(db, _id):
    cmbd = db.currencymentionsbd.find(
        {
            "_id": _id
        },
        {
            "currency_mentions_by_day": 1
        }
    )
    for doc in cmbd:
       return doc["currency_mentions_by_day"]

def SetCurrencyMentionsByDay(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "currency_mentions_by_day" in doc:
            _id =  ObjectId(doc["currency_mentions_by_day"])
            cmbd_prev = QueryCMBD(db, _id)
            cmbd = TA.CurrencyMentionsByDay(cmbd_prev)
            # Remove old data
            db.currencymentionsbd.update(
                {"_id": _id},
                { "$unset": { "currency_mentions_by_day": ""} }
            )
            db.currencymentionsbd.update_one(
                        {
                            "_id": _id
                        },
                        {
                            "$set": {
                                "currency_mentions_by_day": cmbd
                            }
                        }
                    )

        # else create new object
        else:
            cmbd = TA.CurrencyMentionsByDay(None)
            objectid = ObjectId()
            cmbdresult = db.currencymentionsbd.insert_one(
                        {      
                            "_id": objectid,
                            "currency_mentions_by_day": cmbd
                        }
                    )
            print(cmbdresult)
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$set": {
                            "currency_mentions_by_day": objectid
                        }
                    }
                )

def QueryCBA(db, _id):
    cba = db.currencybyauthor.find(
        {
            "_id": _id
        },
        {
            "currency_by_author": 1
        }
    )
    for doc in cba:
       return doc["currency_by_author"]

def SetCurrencyByAuthor(TA, db):
    cursor = GetTweetDocument(db)
    for doc in cursor:
        if "currency_by_author" in doc:
            _id =  ObjectId(doc["currency_by_author"])
            cba_prev = QueryCBA(db, _id)
            cba = TA.CurrencyByAuthor(cba_prev)
            # Remove old data
            db.currencybyauthor.update(
                {"_id": _id},
                { "$unset": { "currency_by_author": ""} }
            )
            db.currencybyauthor.update_one(
                        {
                            "_id": _id
                        },
                        {
                            "$set": {
                                "currency_by_author": cba
                            }
                        }
                    )

        # else create new object
        else:
            cba = TA.CurrencyByAuthor(None)
            objectid = ObjectId()
            cbaresult = db.currencybyauthor.insert_one(
                        {      
                            "_id": objectid,
                            "currency_by_author": cba
                        }
                    )
            print(cbaresult)
            db.tweets.update_one(
                    {
                        "id": "tweets"
                    },
                    {
                        "$set": {
                            "currency_by_author": objectid
                        }
                    }
                )

def main():

    for file in listdir("../data/twitter"):
        if ".csv" in file:
            log(file, newline=True)
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
                
            log("Instantiating twitter analyser...", newline=True)
            TA = TwitterAnalyser.TwitterAnalyser(tweets, currency_symbols, stopwords)

            log("Total tweets...")    
            SetTotalTweets(TA, db)


            log("Counting bigrams...")    
            SetBigrams(TA, db)

            log("bigrams by day...")     
            SetBigramsByDay(TA, db)


            log("Gathering most active users...")    
            SetMostActiveUsers(TA, db)

            log("Calcuating sentiment by day...")    
            SetSentimentByDay(TA, db)
            
            log("performing word count...")    
            SetWordCount(TA, db)

            log("performing word count by day...")    
            SetWordCountByDay(TA, db)

                    
            log("Gathering currency mentions...")    
            SetCurrencyMentions(TA, db)


            log("Currency mentions by day...")    
            SetCurrencyMentionsByDay(TA, db)


            log("Currency by author...")    
            SetCurrencyByAuthor(TA, db)

            
            # print("Sentiment by currency...")    
            # start = time.time()
            # SetSentimentByCurrency(TA, db)
            # end = time.time()
            # print("Sentiment by currency... Time elapsed: " + str(end - start))
            
            try:
                os.remove("../data/twitter/"+file)
            except OSError:
                pass

main()
