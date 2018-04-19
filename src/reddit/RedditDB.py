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
from bson import ObjectId

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

def QueryCPBD(db, _id):
    cpbd = db.commentpostbd.find(
        {
            "_id": _id
        },
        {
            "comments_posts_by_day": 1
        }
    )
    for doc in cpbd:
       return doc["comments_posts_by_day"]

def SetCommentsPostsByDay(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "comments_posts_by_day" in doc:
            _id =  ObjectId(doc["comments_posts_by_day"])
            cpbd_prev = QueryCPBD(db, _id)                               
                      
            cpbd = RA.CommentsPostsByDay(cpbd_prev)
            # Remove old data
            db.commentpostbd.update(
                {"_id": _id},
                { "$unset": { "comments_posts_by_day": ""} }
            )
            db.commentpostbd.update_one(
                    {
                        "_id": _id
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
            objectid = ObjectId()
            cpbdresult = db.commentpostbd.insert_one(
                        {      
                            "_id": objectid,
                            "comments_posts_by_day": cpbd
                        }
                    )            
            db.subreddits.update_one(
                    {
                        "id": subreddit
                    },
                    {
                        "$set": {
                            "comments_posts_by_day": objectid
                        }
                    }
                )

def SetOverallUserScoreTail(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "overall_user_score_tail" in doc:
            ous = RA.OverallUserScoreTail(doc["overall_user_score_tail"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "overall_user_score_tail": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "overall_user_score_tail": ous
                    }
                }
            )
        else:
            ous = RA.OverallUserScoreTail(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "overall_user_score_tail": ous
                    }
                }
            )

def SetOverallUserScoreHead(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "overall_user_score_head" in doc:
            ous = RA.OverallUserScoreHead(doc["overall_user_score_head"])
            # Remove old data
            db.subreddits.update(
                {"id": subreddit},
                { "$unset": { "overall_user_score_head": ""} }
            )
            # Add new data
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "overall_user_score_head": ous
                    }
                }
            )
        else:
            ous = RA.OverallUserScoreHead(None)
            db.subreddits.update_one(
                {"id": subreddit},
                {
                    "$set": {
                        "overall_user_score_head": ous
                    }
                }
            )

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

def SetSentimentByDay(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "sentiment_by_day" in doc:
            _id =  ObjectId(doc["sentiment_by_day"])
            sbd_prev = QuerySBD(db, _id)                               
            sbd = RA.SentimentByDay(sbd_prev)
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
            sbd = RA.SentimentByDay(None)
            objectid = ObjectId()
            wcbdresult = db.sentimentbd.insert_one(
                        {      
                            "_id": objectid,
                            "sentiment_by_day": sbd
                        }
                    )     
            db.subreddits.update_one(
                    {
                        "id": subreddit
                    },
                    {
                        "$set": {
                            "sentiment_by_day": objectid
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

def SetWordCountByDay(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "wordcount_by_day" in doc:
            _id =  ObjectId(doc["wordcount_by_day"])
            wcbd_prev = QueryWCBD(db, _id)                                 
            wcbd = RA.WordCountByDay(wcbd_prev)
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
            wcbd = RA.WordCountByDay(None)
            objectid = ObjectId()
            wcbdresult = db.wordcountbd.insert_one(
                        {      
                            "_id": objectid,
                            "wordcount_by_day": wcbd
                        }
                    )
            print(wcbdresult)                
            db.subreddits.update_one(
                    {
                        "id": subreddit
                    },
                    {
                        "$set": {
                            "wordcount_by_day": objectid
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

def SetCurrencyMentionsByDay(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "currency_mentions_by_day" in doc:
            
            _id =  ObjectId(doc["currency_mentions_by_day"])
            cmbd_prev = QueryCMBD(db, _id)
            cmbd = RA.CurrencyMentionsByDay(cmbd_prev)

            # Remove old data
            db.currencymentionsbd.update(
                {"_id": _id},
                { "$unset": { "currency_mentions_by_day": ""} }
            )
            if cmbd != None:
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
            cmbd = RA.CurrencyMentionsByDay(None)
            objectid = ObjectId()
            if cmbd != None:
                cmbdresult = db.currencymentionsbd.insert_one(
                        {      
                            "_id": objectid,
                            "currency_mentions_by_day": cmbd
                        }
                    )
                print(cmbdresult)
                db.subreddits.update_one(
                        {
                            "id": subreddit
                        },
                        {
                            "$set": {
                                "currency_mentions_by_day": objectid
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

def SetBigramsByDay(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "bigram_by_day" in doc:
            _id =  ObjectId(doc["bigram_by_day"])
            bbd_prev = QueryBBD(db, _id)                     
            bbd = RA.BigramByDay(bbd_prev)
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
            bbd = RA.BigramByDay(None)
            objectid = ObjectId()      
            bbdresult = db.bigramsbd.insert_one(
                        {      
                            "_id": objectid,
                            "bigram_by_day": bbd
                        }
                    )
            print(bbdresult)      
            db.subreddits.update_one(
                    {
                        "id": subreddit
                    },
                    {
                        "$set": {
                            "bigram_by_day": objectid
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

def SetCurrencyByAuthor(RA, db, subreddit):
    cursor = GetSubredditDocument(db, subreddit)
    for doc in cursor:
        if "currency_by_author" in doc:
            _id =  ObjectId(doc["currency_by_author"])
            cba_prev = QueryCBA(db, _id)
            cba = RA.CurrencyByAuthor(cba_prev)
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
            cba = RA.CurrencyByAuthor(None)
            if cba == None:
                return
            objectid = ObjectId()
            cbaresult = db.currencybyauthor.insert_one(
                        {      
                            "_id": objectid,
                            "currency_by_author": cba
                        }
                    )
            print(cbaresult)
            db.subreddits.update_one(
                    {
                        "id": subreddit
                    },
                    {
                        "$set": {
                            "currency_by_author": objectid
                        }
                    }
                )


def main(subreddit, comments_path, posts_path, currency_symbols_path, stopwords_path, banned_path):

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    # Load comments and posts
    c_path = Path(comments_path)
    # if file exists load it
    if c_path.is_file():
        comments = pd.read_csv(comments_path)
    # else create empty dataframe
    else:
        comments = pd.DataFrame(columns=['Author','Body','Date','Score'])

    p_path = Path(posts_path)
    # if file exists load it    
    if p_path.is_file():
        posts = pd.read_csv(posts_path)
    # else create empty dataframe
    else:
        posts = pd.DataFrame(columns=['Author','Title','Date','Score'])
        
    # Load currencys and stop words
    currency_symbols = pd.read_csv(currency_symbols_path)
    stopwords = pd.read_csv(stopwords_path)

    # Remove comments and posts .csv
    try:
        os.remove(comments_path)
        os.remove(posts_path)
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
    RA = RedditAnalyser.RedditAnalyser(comments, posts, currency_symbols, stopwords, banned_path)
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
    
    print("Gathering overall user score head...", end="\r")    
    start = time.time()
    SetOverallUserScoreHead(RA, db, subreddit)
    end = time.time()
    print("Gathering overall user score head... Time elapsed: " + str(end - start))

    print("Gathering overall user score tail...", end="\r")    
    start = time.time()
    SetOverallUserScoreTail(RA, db, subreddit)
    end = time.time()
    print("Gathering overall user score tail... Time elapsed: " + str(end - start))
    
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

    print("Currency by author...", end="\r")    
    start = time.time()
    SetCurrencyByAuthor(RA, db, subreddit)
    end = time.time()
    print("Currency by author... Time elapsed: " + str(end - start))

# PATH = "../"
# comments_path = "{0}data/reddit/comments_{1}.csv".format(PATH, "btc")
# posts_path = "{0}data/reddit/posts_{1}.csv".format(PATH,  "btc")
# currency_symbols_path = "{0}data/CurrencySymbols.csv".format(PATH)
# stopwords_path = "{0}data/stopwords.csv".format(PATH)
# banned_path = "{0}data/banned_users.json".format(PATH)
# main("btc", comments_path, posts_path, currency_symbols_path, stopwords_path, banned_path)