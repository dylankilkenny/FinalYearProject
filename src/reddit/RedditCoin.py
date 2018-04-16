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


def UpdateCoinSentiment(db, symbol):
    cursor_social = GetSocialDocument(db, symbol)

    if "sentiment_by_day" in cursor_social[0].keys():
        old = cursor_social[0]["total_sentiment"]
        (sbd, total_sentiment) = GetSentiment(db, symbol)
        sbd = UpdateSentimentByDay(sbd, cursor_social[0]["sentiment_by_day"])
        db.social.update(
            {"id": symbol},
            { 
                "$unset": { "sentiment_by_day": ""},
                "$unset": { "total_sentiment": ""}                
            }
        )
    else:
        (sbd, total_sentiment) = GetSentiment(db, symbol)
        old = 0

    db.social.update(    
        {"id": symbol},
        {
            "$set": {          
                "sentiment_by_day": sbd,
                "total_sentiment": total_sentiment + old      
            }
        }
    )

def UpdateCoinVolume(db, symbol):
    cursor_social = GetSocialDocument(db, symbol)
    (vbd, volume) = GetVolume(db, symbol)


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
                "total_volume": int(volume + old)
            },
            "$push": {
                "volume_by_day": {
                    "$each": vbd
                }
            }
        }
    )

def GetSentiment(db, symbol):
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    total_sentiment = []
    sbd = []
    for i, row in subreddits.iterrows():
        s = row["Symbol"]        
        if s.lower() == symbol.lower():
            subreddit = db.subreddits.find({ "id": row["Subreddit"]}, { "sentiment_by_day": 1})
            if subreddit.count() < 1:
                continue
            for day in subreddit[0]["sentiment_by_day"]:
                sbd.append([day["Date"], day["Sentiment"]])
    
    sbd = pd.DataFrame(sbd, columns = ['Date','Sentiment'])
    sbd_grouped = sbd.groupby('Date').sum().reset_index()
    total_sentiment = sbd_grouped["Sentiment"].sum()
    sbd_json = sbd_grouped.to_json(orient='records', date_format=None)
    sbd_json = json.loads(sbd_json)
    return (sbd_json, total_sentiment)
    
def GetVolume(db, symbol):
    
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    cpbd_list = []
    cmbd_list = []
    for i, row in subreddits.iterrows():
        s = row["Symbol"]

        if s.lower() == symbol.lower():
            subreddit = db.subreddits.find({ "id": row["Subreddit"]}, { "comments_posts_by_day": 1})
            if subreddit.count() < 1:
                continue
            
            for day in subreddit[0]["comments_posts_by_day"]:
                cpbd_list.append([day["Date"], day["n_post"]+day["n_comment"]])

        cursor = db.subreddits.find({ "id": row["Subreddit"]}, { "currency_mentions_by_day": 1})
        if cursor.count() < 1:
            continue
        if "currency_mentions_by_day" in cursor[0]:
            for day in cursor[0]["currency_mentions_by_day"]:
                for currency in day["counts"]:
                    if currency["Symbol"] == symbol.lower():
                        cmbd_list.append([day["Date"], currency["n"]])
    
    cmbd = pd.DataFrame(cmbd_list, columns = ['Date','n'])
    cpbd = pd.DataFrame(cpbd_list, columns = ['Date','n'])
    merged = pd.concat([cpbd,cmbd])
    merged = merged.groupby('Date').sum().reset_index()
    merged["n"] = merged["n"].astype(int)
    volume = merged["n"].sum()
    merged = merged.to_json(orient='records', date_format=None)
    merged = json.loads(merged)
    return (merged, volume)

def UpdateSentimentByDay(sbd, alt_sbd):
    sbd = pd.DataFrame.from_records(sbd)
    alt_sbd = pd.DataFrame.from_records(alt_sbd)
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

def main(symbol):

    # Connect to DB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.dev

    if symbol == '0':
        return
    print("Updating "+symbol+" social volume...")    
    start = time.time()
    UpdateCoinVolume(db, symbol)
    end = time.time()
    # print("Done | Time elapsed: " + str(end - start))

    # print("Updating "+symbol+" social sentiment...")    
    start = time.time()
    UpdateCoinSentiment(db, symbol)
    end = time.time()
    # print("Done | Time elapsed: " + str(end - start))


if __name__ == '__main__':
    # Load subreddit list 
    # subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    # for i, row in subreddits.iterrows():
    #     main(row["Subreddit"], row["Symbol"])

    currency = pd.read_csv('../data/CurrencySymbols.csv')
    for i, row in currency.iterrows():
        main(row["Symbol"])