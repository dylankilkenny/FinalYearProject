#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Save reddit posts to db"""


import RedditAnalyser
import pandas as pd
from pymongo import MongoClient
import time
import os
import datetime
from pathlib import Path

def GetSubredditDocument(db, subreddit):
    cursor = db.subreddits.find(
        {"id": subreddit}
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
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "comments_posts_by_day" in doc:
            cpbd = RA.CommentsPostsByDay(doc["comments_posts_by_day"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "comments_posts_by_day": ""} }
            )
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
        # else create new object
        else:
            cpbd = RA.CommentsPostsByDay(None)
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
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "sentiment_by_day" in doc:
            # (oldpsa, oldcsa, olds) = GetSentimentByDay(db, subreddit)
            sbd = RA.SentimentByDay(doc["sentiment_by_day"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "overall_user_score": ""} }
            )
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
        # else create new object
        else:
            sbd = RA.SentimentByDay(None)
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
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "wordcount_by_day" in doc:
            wcbd = RA.WordCountByDay(doc["wordcount_by_day"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "wordcount_by_day": ""} }
            )
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
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "currency_mentions_by_day" in doc:
            cmbd = RA.CurrencyMentionsByDay(doc["currency_mentions_by_day"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "currency_mentions_by_day": ""} }
            )
            if cmbd != None:
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
        # else create new object
        else:
            cmbd = RA.CurrencyMentionsByDay(None)
            if cmbd != None:
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
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "bigram_by_day" in doc:
            bbd = RA.BigramByDay(doc["bigram_by_day"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "bigram_by_day": ""} }
            )
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

    


def main(subreddit, symbol, stream):

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    if stream:
        PRE_PATH = '/home/dylan/python/'
        FILE_PATH = PRE_PATH + 'data/reddit/old/'
    else:
        PRE_PATH = '../'        
        FILE_PATH = PRE_PATH + 'data/reddit/'
        
    # Load comments and posts
    comments = Path(FILE_PATH + 'comments_'+subreddit+'.csv')
    # if file exists load it
    if comments.is_file():
        comments = pd.read_csv(FILE_PATH+'comments_'+subreddit+'.csv')
    # else create empty dataframe
    else:
        comments = pd.DataFrame(columns=['Author','Body','Date','Score'])

    posts = Path(FILE_PATH+'posts_'+subreddit+'.csv')
    # if file exists load it    
    if posts.is_file():
        posts = pd.read_csv(FILE_PATH+'posts_'+subreddit+'.csv')
    # else create empty dataframe
    else:
        posts = pd.DataFrame(columns=['Author','Title','Date','Score'])
        
    # Load currencys and stop words
    currency_symbols = pd.read_csv(PRE_PATH + 'data/CurrencySymbols.csv')
    stopwords = pd.read_csv(PRE_PATH + 'data/stopwords.csv')

    # Remove comments and posts .csv
    try:
        os.remove(FILE_PATH+'comments_'+subreddit+'.csv')
        os.remove(FILE_PATH+'posts_'+subreddit+'.csv')
    except OSError:
        pass
    
    # if no comments and posts in dataframes return
    if posts.size < 1 and comments.size < 1:
        print("\nNo comments or posts found, returning.")
        return

    # Check if currency is already in subreddits
    if db.subreddits.find({'id': subreddit}).count() < 1:
        db.subreddits.insert(
            {"id": subreddit})
    
    print("\nInstantiating reddit analyser...", end="\r")
    start = time.time()
    RA = RedditAnalyser.RedditAnalyser(comments, posts, currency_symbols, stopwords, PRE_PATH)
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

