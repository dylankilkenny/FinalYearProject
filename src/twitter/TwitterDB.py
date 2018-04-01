#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import TwitterAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
import datetime

def GetTweetDocument(db):
    cursor = db.tweets.find()
    return cursor

def GetCoinDocument(db, coin):
    cursor = db.coins.find(
        {"id": coin}
    )
    return cursor

def SetNoPostComments(TA, db, subreddit):
    (nc, np) = TA.NoPostComments()
    # cursor = GetSubredditDocument(db, subreddit)
    # for doc in cursor:
    #     if "no_comments" in doc:
    #         db.subreddits.update_one(
    #             {"id": subreddit},
    #             {
    #                 "$set": {
    #                     "no_comments": nc + doc["no_comments"],
    #                     "no_posts": np + doc["no_posts"]
    #                 }
    #             }
    #         )
    #     else:
    #         db.subreddits.update_one(
    #             {"id": subreddit},
    #             {
    #                 "$set": {
    #                     "no_comments": nc,
    #                     "no_posts": np 
    #                 }
    #             }
    #         )
            
def SetMostActiveUsers(TA, db, subreddit):
    
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "most_active_users" in doc:
            mau = TA.MostActiveUsers(doc["most_active_users"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "most_active_users": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "most_active_users": {
                            "$each": mau,
                            "$sort": { "Activity": -1 },
                            "$slice": 500
                        }
                    }
                }
            )
        else:
            mau = TA.MostActiveUsers(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "most_active_users": mau
                    }
                }
            )

def SetCommentsPostsByDay(TA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "comments_posts_by_day.Date": str(datetime.datetime.now().date())
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        (oldp, oldc) = GetCommentsPostsByDay(db, subreddit)
        cpbd = TA.CommentsPostsByDay(oldc, oldp)
        db.subreddits.update_one(
                {
                    "id": subreddit,
                    "comments_posts_by_day.Date": str(datetime.datetime.now().date())
                },
                {
                    "$set": {
                        "comments_posts_by_day.$.n_comment": cpbd[0]["n_comment"],
                        "comments_posts_by_day.$.n_post": cpbd[0]["n_post"]
                    }
                }
            )
    # else create new object
    else:
        cpbd = TA.CommentsPostsByDay(0, 0)
        db.subreddits.update_one(
                {
                    "id": subreddit
                },
                {
                    "$set": {
                        "comments_posts_by_day": cpbd
                    }
                }
            )
        
def GetCommentsPostsByDay(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        for day in doc["comments_posts_by_day"]:
            if day["Date"] == str(datetime.datetime.now().date()):
                return (day["n_post"], day["n_comment"])

def SetOveTAllUserScore(TA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "oveTAll_user_score" in doc:
            ous = TA.OveTAllUserScore(doc["oveTAll_user_score"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "oveTAll_user_score": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "oveTAll_user_score": {
                            "$each": ous,
                            "$sort": { "TotalScore": -1 },
                            "$slice": 500
                        }
                    }
                }
            )
        else:
            ous = TA.OveTAllUserScore(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "oveTAll_user_score": ous
                    }
                }
            )
    
def SetSentimentByDay(TA, db):
    # Get objects with todays date
    cursor = db.tweets.find(
        {
            "sentiment_by_day.Date": str(datetime.datetime.now().date())
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        olds = GetSentimentByDay(db)
        sbd = TA.SentimentByDay(olds)
        db.subreddits.update_one(
                {
                    "sentiment_by_day.Date": str(datetime.datetime.now().date())
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

def SetCurrencyMentions(TA, db):
    
    cursor = GetTweetDocument(db)
    for doc in cursor:
        print(doc)
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


def main():
    
    # comments = pd.read_csv('../data/comments_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # comments = pd.read_csv('../data/small_data.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/post_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/small_data_post.csv', parse_dates=['Date'])

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    # Load Data
    tweets = pd.read_csv('../data/twitter/tweets1.csv', parse_dates=['Date'])
    currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
    stopwords = pd.read_csv('../data/stopwords.csv')


    # Check if currency is already in subreddits
    if db.tweets.find({'id': "tweets"}).count() < 1:
        db.tweets.insert(
            {"id": "tweets"})
        

    print("Instantiating twitter analyser...")
    start = time.time()
    TA = TwitterAnalyser.TwitterAnalyser(tweets, currency_symbols, stopwords)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    # print("Count number comments and posts...")    
    # start = time.time()
    # SetNoPostComments(TA, db, subreddit)
    # end = time.time()
    # print("Done | Time elapsed: " + str(end - start))
    # print()

    # print("Gathering most active users...")    
    # start = time.time()
    # SetMostActiveUsers(TA, db, subreddit)
    # end = time.time()
    # print("Done | Time elapsed: " + str(end - start))
    # print()

    # print("Gathering comments and posts per day...")    
    # start = time.time()
    # SetCommentsPostsByDay(TA, db, subreddit)
    # end = time.time()
    # print("Done | Time elapsed: " + str(end - start))
    # print()
    
    # print("Gathering oveTAll user score...")    
    # start = time.time()
    # SetOveTAllUserScore(TA, db, subreddit)
    # end = time.time()
    # print("Done | Time elapsed: " + str(end - start))
    # print()
    
    print("Calcuating sentiment by day...")    
    start = time.time()
    SetSentimentByDay(TA, db)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("performing word count...")    
    start = time.time()
    SetWordCount(TA, db)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Gathering currency mentions...")    
    start = time.time()
    SetCurrencyMentions(TA, db)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    
    
    # try:
    #     os.remove('../data/comments_'+subreddit+'.csv')
    #     os.remove('../data/posts_'+subreddit+'.csv')
    # except OSError:
    #     pass

main()