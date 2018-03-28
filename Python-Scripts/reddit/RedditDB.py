#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import RedditAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os

def UpdateNCP(RA, db, subreddit):
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

def UpdateMAU(RA, db, subreddit):
    mau = RA.MostActiveUsers()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "most_active_users": mau
            }
        }
    )

def UpdateCPBD(RA, db, subreddit):
    cpbd = RA.CommentsPostsByDay()    
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "comments_posts_by_day": cpbd
            }
        }
    )


def UpdateOUS(RA, db, subreddit):
    ous = RA.OverallUserScore()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "overall_user_score": ous
            }
        }
    )

def UpdateSBD(RA, db, subreddit):
    sbd = RA.SentimentByDay()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "sentiment_by_day": sbd
            }
        }
    )

def UpdateWC(RA, db, subreddit):
    wc = RA.WordCount()
    db.subreddits.update_one(
        {"id": subreddit},
        {
            "$set": {
                "word_count": wc
            }
        }
    )

def UpdateCM(RA, db, subreddit):
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
    UpdateNCP(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    print("Gathering most active users...")    
    start = time.time()
    UpdateMAU(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()

    print("Gathering comments and posts per day...")    
    start = time.time()
    UpdateCPBD(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Gathering overall user score...")    
    start = time.time()
    UpdateOUS(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Calcuating sentiment by day...")    
    start = time.time()
    UpdateSBD(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("performing word count...")    
    start = time.time()
    UpdateWC(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
    print()
    
    print("Gathering currency mentions...")    
    start = time.time()
    UpdateCM(RA, db, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))

    try:
        os.remove('../data/comments_'+subreddit+'.csv')
        os.remove('../data/posts_'+subreddit+'.csv')
    except OSError:
        pass

