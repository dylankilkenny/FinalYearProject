import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import time
import json

personal_key = "E9mLn9PsZsIDt8LUxaThLZSkF"
personal_secret = "v8tAJogrZyD17QcaQc2YBnkPLnVdps6p0EoF3ZHfmm3o7BQIxR"

access_token_key = "416505215-w88alVLSBIfZdn7vYYeBgkcIpETDXXxWx3kP56qo"
access_token_secret = "oTuzCcvLUSyNj1ZSTznllhcxgGQuBf5RgH3WlX4Ize2JQ"

class MyListener(StreamListener):
    
    def on_data(self, data):
        tweet = json.loads(data)
        if "extended_tweet" in tweet:
            count()
            
            date = time.strftime(
                '%Y-%m-%d', 
                time.strptime(tweet["created_at"],
                '%a %b %d %H:%M:%S +0000 %Y'))

            tweets.append([
                date,
                tweet["user"]["screen_name"], 
                tweet["extended_tweet"]["full_text"], 
                tweet["retweet_count"], 
                tweet["favorite_count"]
                ])

        print(tweets)
        return True
 
    def on_error(self, status):
        print(status)
        return True

def SaveTweet(data):
    
    #save posts
    f = open('../data/posts_'+subreddit+'.csv', 'a')
    try:
        writer = csv.writer(f)
        for i in range(len(posts)):
            writer.writerow(posts[i])
    finally:
        f.close()

def count():
    global tweet_count
    tweet_count += 1
    print("Count: " + str(tweet_count), end="\r")

if __name__ == "__main__":
    
    auth = tweepy.OAuthHandler(personal_key, personal_secret)
    auth.set_access_token(access_token_key, access_token_secret)
    
    tweets = []
    tweets.append(["Date", "Author", "Text", "Retweets", "Favourites"])
    tweet_count = 1
    twitter_stream = Stream(auth, MyListener())
    twitter_stream.filter(track=['#bitcoin', '#ethereum', '#btc', '#eth'])