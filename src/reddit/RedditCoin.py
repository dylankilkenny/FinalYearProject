from pymongo import MongoClient
import RedditDB
import time

def UpdateCoinSentiment(db, symbol, subreddit):
    cursor_coin = RedditDB.GetCoinDocument(db, symbol)
    cursor_subreddit = RedditDB.GetSubredditDocument(db, subreddit)
    (mentions_name, mentions_symbol) = GetAllMentionsCurrrency(db, symbol)

    total_sentiment = []
    for day in cursor_subreddit[0]["sentiment_by_day"]:
        total_sentiment.append(day["Sentiment"])
    

    if "social.sentiment_by_day" in cursor_coin:
        db.coins.update(
            {"symbol": symbol},
            { 
                "$unset": { "social.sentiment_by_day": ""},
                "$unset": { "social.total_sentiment": ""}                
            }
        )

    db.coins.update(    
        {"symbol": symbol},
        {
            "$set": {          
                "social.sentiment_by_day": cursor_subreddit[0]["sentiment_by_day"],
                "social.total_sentiment": sum(total_sentiment)       
            }
        }
    )

def UpdateCoinVolume(db, symbol, subreddit):
    cursor_coin = RedditDB.GetCoinDocument(db, symbol)
    cursor_subreddit = RedditDB.GetSubredditDocument(db, subreddit)
    (mentions_name, mentions_symbol) = GetAllMentionsCurrrency(db, symbol)

    if "social.volume_by_day" in cursor_coin:
        db.coins.update(
            {"symbol": symbol},
            { 
                "$unset": { "social.volume_by_day": ""},
                "$unset": { "social.mentions_name": ""}, 
                "$unset": { "social.mentions_symbol": ""}
            }
        )

    db.coins.update(    
        {"symbol": symbol},
        {
            "$set": {          
                "social.volume_by_day": cursor_subreddit[0]["comments_posts_by_day"],
                "social.mentions_name": mentions_name,
                "social.mentions_symbol": mentions_symbol   
            }
        }
    )
    

def GetAllMentionsCurrrency(db, symbol):
    cursor = db.subreddits.find()
    mentnamelist = []
    mentsymlist = []
    for doc in cursor:
        for currency in doc["currency_mentions"]:
            if currency["Symbol"] == symbol.lower():
                mentnamelist.append(currency["Mentions_Name"])
                mentsymlist.append(currency["Mentions_Sym"])
    
    mentions_name = sum(mentnamelist)
    mentions_symbol = sum(mentsymlist)
    return (mentions_name, mentions_symbol)


def main(subreddit, symbol):

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    print("Updating "+symbol+" social volume...")    
    start = time.time()
    UpdateCoinVolume(db, symbol, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))

    print("Updating "+symbol+" social volume...")    
    start = time.time()
    UpdateCoinSentiment(db, symbol, subreddit)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))
