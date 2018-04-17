from pymongo import MongoClient
import pandas as pd
import RedditDB
import time
from datetime import datetime, timedelta
import json
import scipy.stats as stats

def GetSocialDocument(db, symbol):
    cursor = db.social.find(
        {"id": symbol}
    )
    return cursor


def UpdateCoinSentiment(db, symbol):
    cursor_social = GetSocialDocument(db, symbol)

    if "sentiment_by_day" in cursor_social[0].keys():
        old = cursor_social[0]["total_sentiment"]
        sbd, total_sentiment, one_day, seven_day, thirty_day = GetSentiment(db, symbol)
        sbd = UpdateSentimentByDay(sbd, cursor_social[0]["sentiment_by_day"])
        db.social.update(
            {"id": symbol},
            { 
                "$unset": { "sentiment_by_day": ""},
                "$unset": { "total_sentiment": ""}                
            }
        )
    else:
        sbd, total_sentiment, one_day, seven_day, thirty_day = GetSentiment(db, symbol)
        old = 0

    db.social.update(    
        {"id": symbol},
        {
            "$set": {
                "24hr_sentiment": one_day,
                "7day_sentiment": seven_day,
                "30day_sentiment": thirty_day,
                "sentiment_by_day": sbd,
                "total_sentiment": total_sentiment + old      
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

    one_day = PercentChangeSentiment(sbd_grouped.copy(), 1)    
    seven_day = PercentChangeSentiment(sbd_grouped.copy(), 7)
    thirty_day = PercentChangeSentiment(sbd_grouped.copy(), 30)

    sbd_json = sbd_grouped.to_json(orient='records', date_format=None)
    sbd_json = json.loads(sbd_json)
    return sbd_json, total_sentiment, one_day, seven_day, thirty_day

def PercentChangeSentiment(sbd, days):
    # dates = sbd.pop('Date')
    # sbd.index = dates
    # df2 = (sbd - sbd.mean())/sbd.std()
    # print(df2)
    # # print(df1)
    # # df2 = (sbd - sbd.min()) / (sbd.max() - sbd.min())
    # sbd = df2.reset_index()
    
    last_row = sbd.tail(1) # get the last row of dataframe
    last_row_date = last_row.iloc[0]["Date"] # get the date in last row        
    last_row_date = datetime.strptime(last_row_date,'%Y-%m-%d %H:%M:%S') # to object
    lrd_minus = last_row_date - timedelta(days=days) # get datetime 24 hours ago
    
    df_1 = sbd.copy()    
    df_1['Date'] = pd.to_datetime(df_1['Date'])
    mask = (df_1['Date'] > lrd_minus) & (df_1['Date'] <= last_row_date)
    df_1 = df_1.loc[mask]

    previous_period = lrd_minus - timedelta(days=days) # get datetime 24 hours ago
    df_2 = sbd.copy()    
    df_2['Date'] = pd.to_datetime(df_2['Date'])
    mask = (df_2['Date'] > previous_period) & (df_2['Date'] <= lrd_minus)
    df_2 = df_2.loc[mask]

    df_2['dist'] = abs(df_2['Sentiment'] - df_2['Sentiment'].median())
    df_1['dist'] = abs(df_1['Sentiment'] - df_1['Sentiment'].median())
    # print(df_1['Sentiment'].median())
    # print( df_2['Sentiment'].median())

    current_period = df_1['dist'].median()
    previous_period = df_2['dist'].median()

    pc_change = round(100 * (current_period - previous_period) / (previous_period + 1))
    
    # dtime = pd.to_datetime(sbd['Date']) # create new dataframe with dates in datetime format
    
    # diff = (dtime - lrd_minus) # get difference
    # indexmax = (diff[(diff < pd.to_timedelta(1))].idxmax()) # locate index of closest date
    
    # last_row_n = last_row.iloc[0]["Sentiment"] # get the number of mentions in last row    
    # item = sbd.ix[[indexmax]] # get item with closest date
    # item_n = item.iloc[0]["Sentiment"] # get number of mentions
    # print(last_row_n)
    # print(item_n)
    
    return pc_change

def UpdateCoinVolume(db, symbol):
    cursor_social = GetSocialDocument(db, symbol)
    vbd, volume, one_day, seven_day, thirty_day = GetVolume(db, symbol)

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
                "total_volume": int(volume + old),
                "24hr_volume": one_day,
                "7day_volume": seven_day,
                "30day_volume": thirty_day
            },
            "$push": {
                "volume_by_day": {
                    "$each": vbd
                }
            }
        }
    )

def PercentChangeVolume(merged, days):
    last_row = merged.tail(1) # get the last row of dataframe
    last_row_date = last_row.iloc[0]["Date"] # get the date in last row        
    last_row_date = datetime.strptime(last_row_date,'%Y-%m-%d %H:%M:%S') # to object
    lrd_minus = last_row_date - timedelta(days=days) # get datetime 24 hours ago
    
    df_1 = merged.copy()    
    df_1['Date'] = pd.to_datetime(df_1['Date'])
    mask = (df_1['Date'] > lrd_minus) & (df_1['Date'] <= last_row_date)
    df_1 = df_1.loc[mask]

    previous_period = lrd_minus - timedelta(days=days) # get datetime 24 hours ago
    df_2 = merged.copy()    
    df_2['Date'] = pd.to_datetime(df_2['Date'])
    mask = (df_2['Date'] > previous_period) & (df_2['Date'] <= lrd_minus)
    df_2 = df_2.loc[mask]

    df_2['dist'] = abs(df_2['n'] - df_2['n'].median())
    df_1['dist'] = abs(df_1['n'] - df_1['n'].median())
    # last_row = merged.tail(1) # get the last row of dataframe
    # last_row_date = last_row.iloc[0]["Date"] # get the number of mentions in last row        
    # dt = datetime.strptime(last_row_date,'%Y-%m-%d %H:%M:%S') # to object
    # new_dt = dt - timedelta(days=days) # get datetime 24 hours ago
    # dtime = pd.to_datetime(merged['Date']) # create new dataframe with dates in datetime format
    # diff = (dtime - new_dt) # get difference
    # indexmax = (diff[(diff < pd.to_timedelta(1))].idxmax()) # locate index of closest date
    # last_row_n = last_row.iloc[0]["n"] # get the number of mentions in last row    
    # item = merged.ix[[indexmax]] # get item with closest date
    # item_n = item.iloc[0]["n"] # get number of mentions
    last_row_n = df_1['dist'].median()
    item_n = df_2['dist'].median()
    pc_change = round(100 * (last_row_n - item_n) / item_n)
    return pc_change


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

    one_day = PercentChangeVolume(merged, 1) # 24 hour change
    seven_day = PercentChangeVolume(merged, 7) # 3 day change
    thirty_day = PercentChangeVolume(merged, 30) # 7 day change
    
    merged = merged.to_json(orient='records', date_format=None)
    merged = json.loads(merged)
    return merged, volume, one_day, seven_day, thirty_day

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
    print("Done | Time elapsed: " + str(end - start))

    print("Updating "+symbol+" social sentiment...")    
    start = time.time()
    UpdateCoinSentiment(db, symbol)
    end = time.time()
    print("Done | Time elapsed: " + str(end - start))


if __name__ == '__main__':
    # Load subreddit list 
    # subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    # for i, row in subreddits.iterrows():
    #     main(row["Subreddit"], row["Symbol"])

    currency = pd.read_csv('../data/CurrencySymbols.csv')
    for i, row in currency.iterrows():
        main(row["Symbol"])
        break