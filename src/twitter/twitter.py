"""
Stream tweets containing crypto related hastags
"""
import time
import json
import csv
import pandas as pd
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener


PERSONAL_KEY = "E9mLn9PsZsIDt8LUxaThLZSkF"
PERSONAL_SECRET = "v8tAJogrZyD17QcaQc2YBnkPLnVdps6p0EoF3ZHfmm3o7BQIxR"

ACCESS_TOKEN_KEY = "416505215-w88alVLSBIfZdn7vYYeBgkcIpETDXXxWx3kP56qo"
ACCESS_TOKEN_SECRET = "oTuzCcvLUSyNj1ZSTznllhcxgGQuBf5RgH3WlX4Ize2JQ"

count = 1

class MyListener(StreamListener):
    """
    This listerner handles tweets that are recieved from the stream
    """
    def __init__(self):
        self.count = 0
        self.tweets = []
        self.file_number = 1
        self.tweets.append(["Date", "Author", "Text", "Retweets", "Favourites"])
    
    def SaveTweet(self, tweets):
        #save posts
        f = open('../data/twitter/tweets'+str(self.file_number)+'.csv', 'a')
        self.file_number += 1
        try:
            writer = csv.writer(f)
            for i in range(len(tweets)):
                writer.writerow(tweets[i])
        finally:
            f.close()
            

    def on_data(self, data):
        tweet = json.loads(data)
        if "extended_tweet" in tweet:
            print("Count: " + str(self.count), end="\r")
            self.count += 1
            date = time.strftime('%Y-%m-%d %H:00:00', time.strptime(tweet["created_at"],
                                                           '%a %b %d %H:%M:%S +0000 %Y'))
            self.tweets.append([
                date,
                tweet["user"]["screen_name"],
                tweet["extended_tweet"]["full_text"],
                tweet["retweet_count"],
                tweet["favorite_count"]
                ])

        if len(self.tweets) == 500:
            t = self.tweets
            self.tweets = self.tweets[:1]
            self.SaveTweet(t)

        return True


    def on_error(self, status):
        print(status)
        return True



if __name__ == "__main__":
    
    hashtags = []
    CURRENCYS = pd.read_csv('../data/CurrencySymbols.csv')

    for i, row in CURRENCYS.iterrows():
        hashtags.append("#"+row["Name"])
        hashtags.append("#"+row["Symbol"])

    AUTH = tweepy.OAuthHandler(PERSONAL_KEY, PERSONAL_SECRET)
    AUTH.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
    
    
    TWITTER_STREAM = Stream(AUTH, MyListener())
    TWITTER_STREAM.filter(track=hashtags[:400])