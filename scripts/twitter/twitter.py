import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener

auth = tweepy.OAuthHandler("E9mLn9PsZsIDt8LUxaThLZSkF", "v8tAJogrZyD17QcaQc2YBnkPLnVdps6p0EoF3ZHfmm3o7BQIxR")
auth.set_access_token("416505215-w88alVLSBIfZdn7vYYeBgkcIpETDXXxWx3kP56qo", "oTuzCcvLUSyNj1ZSTznllhcxgGQuBf5RgH3WlX4Ize2JQ")

class MyListener(StreamListener):
    def on_data(self, data):
        count()    
        try:
            with open('python.json', 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True

def count():
    global tweet_count
    tweet_count += 1
    print("Count: " + str(tweet_count), end="\r")

if __name__ == "__main__":
    tweet_count = 1
    twitter_stream = Stream(auth, MyListener())
    twitter_stream.filter(track=['#bitcoin', '#ethereum', '#btc', '#eth'])