#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import RedditAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
import datetime

def GetSubredditDocument(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    return cursor

def GetCoinDocument(db, coin):
    cursor = db.coins.find(
        {"id": coin}
    )
    return cursor

def SetNoPostComments(RA, db, subreddit):
    (nc, np) = RA.NoPostComments()
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "no_comments" in doc:
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "no_comments": nc + doc["no_comments"],
                        "no_posts": np + doc["no_posts"]
                    }
                }
            )
        else:
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "no_comments": nc,
                        "no_posts": np 
                    }
                }
            )
            
def SetMostActiveUsers(RA, db, subreddit):
    
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "most_active_users" in doc:
            mau = RA.MostActiveUsers(doc["most_active_users"])
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
                            "$slice": 1000
                        }
                    }
                }
            )
        else:
            mau = RA.MostActiveUsers(None)
            db.subreddits.update_one(
                {"id": subreddit},
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

def SetCommentsPostsByDay(RA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "comments_posts_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        (oldp, oldc) = GetCommentsPostsByDay(db, subreddit)
        cpbd = RA.CommentsPostsByDay(oldc, oldp)
        db.subreddits.update_one(
                {
                    "id": subreddit,
                    "comments_posts_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
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
        cpbd = RA.CommentsPostsByDay(0, 0)
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
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')):
                return (day["n_post"], day["n_comment"])

def SetOverallUserScore(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "overall_user_score" in doc:
            ous = RA.OverallUserScore(doc["overall_user_score"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "overall_user_score": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "overall_user_score": {
                            "$each": ous,
                            "$sort": { "TotalScore": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )
        else:
            ous = RA.OverallUserScore(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "overall_user_score": {
                            "$each": ous,
                            "$sort": { "TotalScore": -1 },
                            "$slice": 1000
                        }
                    }
                }
            )
    
def SetSentimentByDay(RA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "sentiment_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        (oldpsa, oldcsa, olds) = GetSentimentByDay(db, subreddit)
        sbd = RA.SentimentByDay(oldpsa, oldcsa, olds)
        db.subreddits.update_one(
                {
                    "id": subreddit,
                    "sentiment_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00'))
                },
                {
                    "$set": {
                        "sentiment_by_day.$.Post_SA": sbd[0]["Post_SA"],
                        "sentiment_by_day.$.Comment_SA": sbd[0]["Comment_SA"],
                        "sentiment_by_day.$.Sentiment": sbd[0]["Sentiment"]
                    }
                }
            )
    # else create new object
    else:
        sbd = RA.SentimentByDay(0, 0, 0)
        db.subreddits.update_one(
                {
                    "id": subreddit
                },
                {
                    "$set": {
                        "sentiment_by_day": sbd
                    }
                }
            )



def GetSentimentByDay(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        for day in doc["sentiment_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')):
                return (day["Post_SA"], day["Comment_SA"], day["Sentiment"])

def SetWordCount(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "word_count" in doc:
            wc = RA.WordCount(doc["word_count"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "word_count": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
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
            wc = RA.WordCount(None)
            db.subreddits.update_one(
                {"id": subreddit},
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

def SetWordCountByDay(RA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "wordcount_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
    
        oldwordcount = GetWordCountByDay(db, subreddit)
        wcbd = RA.WordCountByDay(oldwordcount)
        db.subreddits.update_one(
                {
                    "id": subreddit,
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
        wcbd = RA.WordCountByDay(None)
        db.subreddits.update_one(
                {
                    "id": subreddit
                },
                {   
                    "$push": {
                        "wordcount_by_day": {
                            "$each": wcbd
                        }
                    }
                }
            )

def GetWordCountByDay(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        for day in doc["wordcount_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["counts"]

def SetCurrencyMentions(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "currency_mentions" in doc:
            cm = RA.CurrencyMentions(doc["currency_mentions"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "currency_mentions": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "currency_mentions": cm
                    }
                }
            )
        else:
            cm = RA.CurrencyMentions(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "currency_mentions": cm
                    }
                }
            )

def SetCurrencyMentionsByDay(RA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "currency_mentions_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        
        oldcmbd = GetCurrencyMentionsByDay(db, subreddit)
        cmbd = RA.CurrencyMentionsByDay(oldcmbd)
        db.subreddits.update_one(
                {
                    "id": subreddit,
                    "currency_mentions_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "currency_mentions_by_day.$.counts": cmbd
                    }
                }
            )
    # else create new object
    else:
        cmbd = RA.CurrencyMentionsByDay(None)
        db.subreddits.update_one(
                {
                    "id": subreddit
                },
                {
                    "$push": {
                        "currency_mentions_by_day": {
                            "$each": cmbd
                        }
                    }
                }
            )

def GetCurrencyMentionsByDay(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        for day in doc["currency_mentions_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["counts"]

def SetBigrams(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "bigram_count" in doc:
            bc = RA.Bigram(doc["bigram_count"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "bigram_count": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "bigram_count": {
                            "$each": bc,
                            "$sort": { "n": -1 },
                            "$slice": 500
                        }
                    }
                }
            )
        else:
            bc = RA.Bigram(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$push": {
                        "bigram_count": {
                            "$each": bc,
                            "$sort": { "n": -1 },
                            "$slice": 500
                        }
                    }
                }
            )

def SetBigramsByDay(RA, db, subreddit):
    # Get objects with todays date
    cursor = db.subreddits.find(
        {
            "id": subreddit,
            "bigram_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
        }
    )
    # if todays date is already present, add old values to new
    if cursor.count() > 0:
        oldbigrams = GetBigramByDay(db, subreddit)
        bbd = RA.BigramByDay(oldbigrams)
        db.subreddits.update_one(
                {
                    "id": subreddit,
                    "bigram_by_day.Date": str(datetime.datetime.now().strftime('%Y-%m-%d'))
                },
                {
                    "$set": {
                        "bigram_by_day.$.counts": bbd
                    }
                }
            )
    # else create new object
    else:
        bbd = RA.BigramByDay(None)
        db.subreddits.update_one(
                {
                    "id": subreddit
                },
                {
                    "$push": {
                        "bigram_by_day": {
                            "$each": bbd
                        }
                    }
                }
            )

def GetBigramByDay(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        for day in doc["bigram_by_day"]:
            if day["Date"] == str(datetime.datetime.now().strftime('%Y-%m-%d')):
                return day["counts"]
    
    


def main(subreddit, symbol):
    
    # comments = pd.read_csv('../data/reddit/comments_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # comments = pd.read_csv('../data/reddit/small_data.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/reddit/post_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/small_data_post.csv', parse_dates=['Date'])

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    # Load Data
    comments = pd.read_csv('../data/reddit/comments_'+subreddit+'.csv', parse_dates=['Date'])
    posts = pd.read_csv('../data/reddit/posts_'+subreddit+'.csv', parse_dates=['Date'])
    currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
    stopwords = pd.read_csv('../data/stopwords.csv')

    try:
        os.remove('../data/reddit/comments_'+subreddit+'.csv')
        os.remove('../data/reddit/posts_'+subreddit+'.csv')
    except OSError:
        pass
    
    if posts.size < 1 and comments.size < 1:
        print("\nNo comments or posts found, returning.")
        return

    # Check if currency is already in subreddits
    if db.subreddits.find({'id': subreddit}).count() < 1:
        db.subreddits.insert(
            {"id": subreddit})
    
    print("\nInstantiating reddit analyser...", end="\r")
    start = time.time()
    RA = RedditAnalyser.RedditAnalyser(comments, posts, currency_symbols, stopwords)
    end = time.time()
    print("Instantiating reddit analyser... Time elapsed: " + str(end - start))

    print("Counting bigrams...", end="\r")    
    start = time.time()
    SetBigrams(RA, db, subreddit)
    end = time.time()
    print("Counting bigrams... Time elapsed: " + str(end - start)) 

    print("bigrams by day...", end="\r")    
    start = time.time()
    SetBigramsByDay(RA, db, subreddit)
    end = time.time()
    print("bigrams by day... Time elapsed: " + str(end - start)) 

    print("Count number comments and posts...", end="\r")    
    start = time.time()
    SetNoPostComments(RA, db, subreddit)
    end = time.time()
    print("Count number comments and posts... Time elapsed: " + str(end - start))

    print("Gathering most active users...", end="\r")    
    start = time.time()
    SetMostActiveUsers(RA, db, subreddit)
    end = time.time()
    print("Gathering most active users... Time elapsed: " + str(end - start))

    print("Gathering comments and posts per day...", end="\r")    
    start = time.time()
    SetCommentsPostsByDay(RA, db, subreddit)
    end = time.time()
    print("Gathering comments and posts per day... Time elapsed: " + str(end - start))
    
    print("Gathering overall user score...", end="\r")    
    start = time.time()
    SetOverallUserScore(RA, db, subreddit)
    end = time.time()
    print("Gathering overall user score... Time elapsed: " + str(end - start))
    
    print("Calcuating sentiment by day...", end="\r")    
    start = time.time()
    SetSentimentByDay(RA, db, subreddit)
    end = time.time()
    print("Calcuating sentiment by day... Time elapsed: " + str(end - start))
    
    print("performing word count...", end="\r")    
    start = time.time()
    SetWordCount(RA, db, subreddit)
    end = time.time()
    print("performing word count... Time elapsed: " + str(end - start))

    print("performing word count by day...", end="\r")    
    start = time.time()
    SetWordCountByDay(RA, db, subreddit)
    end = time.time()
    print("performing word count by day... Time elapsed: " + str(end - start))
    
    print("Gathering currency mentions...", end="\r")    
    start = time.time()
    SetCurrencyMentions(RA, db, subreddit)
    end = time.time()
    print("Gathering currency mentions... Time elapsed: " + str(end - start))

    print("Currency mentions by day...", end="\r")    
    start = time.time()
    SetCurrencyMentionsByDay(RA, db, subreddit)
    end = time.time()
    print("Currency mentions by day... Time elapsed: " + str(end - start)) 

