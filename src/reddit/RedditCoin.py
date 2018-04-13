from pymongo import MongoClient
import pandas as pd
import RedditDB
import time
import json

def GetSocialDocument(db, symbol):
    cursor = db.social.find(
        {"id": symbol}
    )
    return cursor


def UpdateCoinSentiment(db, symbol, subreddit):
    cursor_social = GetSocialDocument(db, symbol)
    cursor_subreddit = RedditDB.GetSubredditDocument(db, subreddit)
    sbd = cursor_subreddit[0]["sentiment_by_day"]
    total_sentiment = []

    for day in cursor_subreddit[0]["sentiment_by_day"]:
        total_sentiment.append(day["Sentiment"])
    

    if "sentiment_by_day" in cursor_social[0].keys():
        old = cursor_social[0]["total_sentiment"]
        sbd = UpdateSentimentByDay(cursor_subreddit[0]["sentiment_by_day"], cursor_social[0]["sentiment_by_day"])
        db.social.update(
            {"id": symbol},
            { 
                "$unset": { "sentiment_by_day": ""},
                "$unset": { "total_sentiment": ""}                
            }
        )
    else:
        old = 0

    db.social.update(    
        {"id": symbol},
        {
            "$set": {          
                "sentiment_by_day": sbd,
                "total_sentiment": sum(total_sentiment) + old      
            }
        }
    )

def UpdateCoinVolume(db, symbol, subreddit):
    cursor_social = GetSocialDocument(db, symbol)
    cursor_subreddit = RedditDB.GetSubredditDocument(db, subreddit)

    (vbd, volume) = GetVolume(db, symbol, cursor_subreddit)
    mentions = GetAllMentionsCurrrency(db, symbol)

    if "total_volume" in cursor_social[0].keys():
        
        old = cursor_social[0]["total_volume"]
        vbd = UpdateVolumeByDay(vbd, cursor_social[0]["volume_by_day"])
        
        db.social.update(
            {"id": symbol},
            {
                "$unset": { "total_volume": ""},
                "$unset": { "volume_by_day": ""},
            }
        )
    else:
        old = 0
    
    db.social.update(    
        {"id": symbol},
        {
            "$set": {          
                "total_volume": volume + old
            },
            "$push": {
                "volume_by_day": {
                    "$each": vbd
                }
            }
        }
    )

def UpdateSentimentByDay(sbd, alt_sbd):
    sbd = pd.DataFrame.from_records(sbd)
    alt_sbd = pd.DataFrame.from_records(alt_sbd)
    print(sbd)
    print(alt_sbd)
    merged = pd.concat([sbd,alt_sbd])
    merged = merged.groupby('Date').sum().reset_index()
    merged = merged.to_json(orient='records', date_format=None)
    merged = json.loads(merged)

    return merged

def UpdateVolumeByDay(vbd, alt_vbd):
    vbd = pd.DataFrame.from_records(vbd)
    alt_vbd = pd.DataFrame.from_records(alt_vbd)
    merged = pd.concat([vbd,alt_vbd])
    merged = merged.groupby('Date').sum().reset_index()
    merged = merged.to_json(orient='records', date_format=None)
    merged = json.loads(merged)

    return merged

def GetVolume(db, symbol, subreddit):
    
    cpbd = pd.DataFrame.from_records(subreddit[0]["comments_posts_by_day"])

    if cpbd.size < 1:
        return ([], 0)
    
    cpbd["Date"] = pd.to_datetime(cpbd['Date']).dt.strftime('%Y-%m-%d %H:00:00')
    cpbd =  cpbd.groupby('Date').sum().reset_index()
    cpbd["n"] = cpbd["n_comment"] + cpbd["n_post"]
    cpbd = cpbd.drop(['n_comment', 'n_post'], 1)

    cursor = db.subreddits.find()
    cmbd = []
    for doc in cursor:
        for day in doc["currency_mentions_by_day"]:
            for currency in day["counts"]:
                if currency["Symbol"] == symbol.lower():
                    cmbd.append([day["Date"], currency["n"]])
    
    cmbd = pd.DataFrame(cmbd, columns = ['Date','n'])
    merged = pd.concat([cpbd,cmbd])
    merged = merged.groupby('Date').sum().reset_index()
    volume = merged["n"].sum()
    merged = merged.to_json(orient='records', date_format=None)
    merged = json.loads(merged)

    return (merged, volume)

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
    return mentions_name + mentions_symbol


def main(subreddit, symbol):

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    if symbol == '0':
        return
    print("Updating "+symbol+" social volume...")    
    start = time.time()
    UpdateCoinVolume(db, symbol, subreddit)
    end = time.time()
    # print("Done | Time elapsed: " + str(end - start))

    # print("Updating "+symbol+" social sentiment...")    
    start = time.time()
    UpdateCoinSentiment(db, symbol, subreddit)
    end = time.time()
    # print("Done | Time elapsed: " + str(end - start))

main("btc", "BCH")

# if __name__ == '__main__':
#     # Load subreddit list 
#     subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
#     for i, row in subreddits.iterrows():
#         main(row["Subreddit"], row["Symbol"])