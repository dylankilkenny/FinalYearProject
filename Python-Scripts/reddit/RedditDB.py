#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import RedditAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
import datetime

def SetNoPostComments(RA, db, subreddit):
    (nc, np) = RA.NoPostComments()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "no_comments": nc,
                "no_posts": np 
            }
        }
    )

# def SetNoPostComments(RA, db, subreddit):
#     (nc, np) = RA.NoPostComments()
#     (oldnc, oldnp) = GetNoPostComments(db, subreddit)
#     db.subreddits.update_one(
#         {"id": subreddit},
#         {
#             "$set": {
#                 "no_comments": nc + oldnc,
#                 "no_posts": np + oldnp
#             }
#         }
#     )

def GetNoPostComments(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        return (doc["no_comments"], doc["no_posts"])
        


def SetMostActiveUsers(RA, db, subreddit):
    mau = RA.MostActiveUsers()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "most_active_users": mau
            }
        }
    )

def GetMostActiveUsers(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
    )
    for doc in cursor:
        return doc["most_active_users"]
        

# def SetCommentsPostsByDay(RA, db, subreddit):
#     cpbd = RA.CommentsPostsByDay()    
#     db.subreddits.update_one(
#         {"id": subreddit},
#         {
#             "$set": {
#                 "comments_posts_by_day": cpbd
#             }
#         }
#     )
def SetCommentsPostsByDay(RA, db, subreddit):
    (oldp, oldc) = GetCommentsPostsByDay(db, subreddit)
    cpbd = RA.CommentsPostsByDay(oldc, oldp)
    db.subreddits.update_one(
        {
            "id": subreddit,
            "comments_posts_by_day.Date": str(datetime.datetime.now().date())
        },
        {
            "$set": {
                "comments_posts_by_day.$.n_comment": cpbd["n_comment"],
                "comments_posts_by_day.$.n_post": cpbd["n_post"]
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


def SetOverallUserScore(RA, db, subreddit):
    ous = RA.OverallUserScore()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "overall_user_score": ous
            }
        }
    )

def SetSentimentByDay(RA, db, subreddit):
    sbd = RA.SentimentByDay()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "sentiment_by_day": sbd
            }
        }
    )

def SetWordCount(RA, db, subreddit):
    wc = RA.WordCount()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "word_count": wc
            }
        }
    )

def SetCurrencyMentions(RA, db, subreddit):
    cm = RA.CurrencyMentions()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "currency_mentions": cm
            }
        }
    )




def main(subreddit):
    
    # comments = pd.read_csv('../data/comments_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # comments = pd.read_csv('../data/small_data.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/post_btc_2017-01-26_2018-01-26.csv', parse_dates=['Date'])
    # posts = pd.read_csv('../data/small_data_post.csv', parse_dates=['Date'])

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    # Load Data
    comments = pd.read_csv('../data/comments_'+subreddit+'.csv', parse_dates=['Date'])
    posts = pd.read_csv('../data/posts_'+subreddit+'.csv', parse_dates=['Date'])
    currency_symbols = pd.read_csv('../data/CurrencySymbols.csv')
    stopwords = pd.read_csv('../data/stopwords.csv')

    try:
        os.remove('../data/comments_'+subreddit+'.csv')
        os.remove('../data/posts_'+subreddit+'.csv')
    except OSError:
        pass

    # Check if currency is already in subreddits
    if db.subreddits.find({'id': subreddit}).count() < 1:
        db.subreddits.insert(
            {"id": subreddit})
        
    

    print("Instantiating reddit analyser...")
    start = time.time()
    RA = RedditAnalyser.RedditAnalyser(comments, posts, currency_symbols, stopwords)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    print("Count number comments and posts...")    
    start = time.time()
    SetNoPostComments(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    print("Gathering most active users...")    
    start = time.time()
    SetMostActiveUsers(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    print("Gathering comments and posts per day...")    
    start = time.time()
    SetCommentsPostsByDay(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Gathering overall user score...")    
    start = time.time()
    SetOverallUserScore(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Calcuating sentiment by day...")    
    start = time.time()
    SetSentimentByDay(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("performing word count...")    
    start = time.time()
    SetWordCount(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Gathering currency mentions...")    
    start = time.time()
    SetCurrencyMentions(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))

    
