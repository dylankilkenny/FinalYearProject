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

def QuerySBD(db):
    sbd = db.sentimentbd.find(
        {
            "id": "tweets"
        },
        {
            "sentiment_by_day": 1
        }
    )
    sbd_list = []
    for doc in sbd:
        for obj in doc["sentiment_by_day"]:
            sbd_list.append(obj)

    return sbd_list
    
def SetSentimentByDay(TA, db):
    # Find all docs for subreddit 
    cursor = db.sentimentbd.find({"id": "tweets"})
    # if more than 0
    if cursor.count() > 0:
        # Array limit per doc
        LIMIT = 500
        sbd_prev = QuerySBD(db)                     
        sbd = TA.SentimentByDay(sbd_prev)
        array_list = [sbd[i:i + LIMIT] for i in range(0, len(sbd), LIMIT)]
        # Remove old data
        db.sentimentbd.remove(
            {"id": "tweets"}
        )
        for arr in array_list:
            db.sentimentbd.update_one(
                {
                    "id": "tweets", 
                    "count": { "$lt" : LIMIT}
                },
                {
                    "$set": {
                        "sentiment_by_day": arr
                    }, 
                    "$inc": { 
                        "count": len(arr)
                    }
                },upsert=True
            )
    else:
        sbd = TA.SentimentByDay(None)
        db.sentimentbd.insert_one(
            {      
                "id": "tweets",
                "sentiment_by_day": sbd,
                "count":len(sbd)
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

def QueryWCBD(db):
    wcbd = db.wordcountbd.find(
        {
            "id": "tweets"
        },
        {
            "wordcount_by_day": 1
        }
    )
    wcbd_list = []
    for doc in wcbd:
        for obj in doc["wordcount_by_day"]:
            wcbd_list.append(obj)
    return wcbd_list

def SetWordCountByDay(TA, db):
     # Find all docs for subreddit 
    cursor = db.wordcountbd.find({"id": "tweets"})
    # if more than 0
    if cursor.count() > 0:
        # Array limit per doc
        LIMIT = 500
        wcbd_prev = QueryWCBD(db)                     
        wcbd = TA.WordCountByDay(wcbd_prev)
        array_list = [wcbd[i:i + LIMIT] for i in range(0, len(wcbd), LIMIT)]
        # Remove old data
        db.wordcountbd.remove(
            {"id": "tweets"}
        )
        for arr in array_list:
            db.wordcountbd.update_one(
                {
                    "id": "tweets", 
                    "count": { "$lt" : LIMIT}
                },
                {
                    "$set": {
                        "wordcount_by_day": arr
                    }, 
                    "$inc": { 
                        "count": len(arr)
                    }
                },upsert=True
            )
    else:
        wcbd = TA.WordCountByDay(None)
        db.wordcountbd.insert_one(
            {      
                "id": "tweets",
                "wordcount_by_day": wcbd,
                "count":len(wcbd)
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

def QueryBBD(db):
    bbd = db.bigramsbd.find(
        {
            "id": "tweets"
        },
        {
            "bigram_by_day": 1
        }
    )
    bbd_list = []
    for doc in bbd:
        for obj in doc["bigram_by_day"]:
            bbd_list.append(obj)

    return bbd_list


def SetBigramsByDay(TA, db):
    # Find all docs for subreddit 
    cursor = db.bigramsbd.find({"id": "tweets"})
    LIMIT = 500
    # if more than 0
    if cursor.count() > 0:
        # Array limit per doc
        bbd_prev = QueryBBD(db)                     
        bbd = TA.BigramByDay(bbd_prev)
        array_list = [bbd[i:i + LIMIT] for i in range(0, len(bbd), LIMIT)]
        # Remove old data
        db.bigramsbd.remove(
            {"id": "tweets"}
        )
        for arr in array_list:
            db.bigramsbd.update_one(
                {
                    "id": "tweets", 
                    "count": { "$lt" : LIMIT}
                },
                {
                    "$set": {
                        "bigram_by_day": arr
                    }, 
                    "$inc": { 
                        "count": len(arr)
                    }
                },upsert=True
            )
    else:
        bbd = TA.BigramByDay(None)
        array_list = [bbd[i:i + LIMIT] for i in range(0, len(bbd), LIMIT)]
        for arr in array_list:
            bbdresult = db.bigramsbd.insert_one(
                        {      
                            "id": "tweets",
                            "bigram_by_day": bbd,
                            "count": len(bbd)
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

def QueryCMBD(db):
    cmbd = db.currencymentionsbd.find(
        {
            "id": "tweets"
        },
        {
            "currency_mentions_by_day": 1
        }
    )
    cmbd_list = []
    for doc in cmbd:
        for obj in doc["currency_mentions_by_day"]:
            cmbd_list.append(obj)
    return cmbd_list


def SetCurrencyMentionsByDay(TA, db):
     # Find all docs for subreddit 
    cursor = db.currencymentionsbd.find({"id": "tweets"})
    LIMIT = 500    
    # if more than 0
    if cursor.count() > 0:
        # Array limit per doc
        cmbd_prev = QueryCMBD(db)                     
        cmbd = TA.CurrencyMentionsByDay(cmbd_prev)
        # return if no mentions
        if cmbd == None:
            return
        array_list = [cmbd[i:i + LIMIT] for i in range(0, len(cmbd), LIMIT)]
        # Remove old data
        db.currencymentionsbd.remove(
            {"id": "tweets"}
        )
        for arr in array_list:
            db.currencymentionsbd.update_one(
                {
                    "id": "tweets", 
                    "count": { "$lt" : LIMIT}
                },
                {
                    "$set": {
                        "currency_mentions_by_day": arr
                    }, 
                    "$inc": { 
                        "count": len(arr)
                    }
                },upsert=True
            )
    else:
        cmbd = TA.CurrencyMentionsByDay(None)
        # return if no mentions
        if cmbd == None:
            return
        array_list = [cmbd[i:i + LIMIT] for i in range(0, len(cmbd), LIMIT)]
        for arr in array_list:   
            db.currencymentionsbd.insert_one(
                {      
                    "id": "tweets",
                    "currency_mentions_by_day": arr,
                    "count":len(arr)
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


            # log("Currency by author...")    
            # SetCurrencyByAuthor(TA, db)

            
            # print("Sentiment by currency...")    
            # start = time.time()
            # SetSentimentByCurrency(TA, db)
            # end = time.time()
            # print("Sentiment by currency... Time elapsed: " + str(end - start))
            
            # try:
            #     os.remove("../data/twitter/"+file)
            # except OSError:
            #     pass

main()