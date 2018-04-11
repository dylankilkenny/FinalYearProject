import RedditDB
import redditapi
import RedditCoin
import pandas as pd

subreddits = pd.read_csv('../data/reddit/SubredditList.csv')

for i, row in subreddits.iterrows():

    print("Fetching("+str(i+1)+"/"+str(len(subreddits))+"): " + row["Subreddit"])
    #1483228800
    (comments, posts) = redditapi.GetData(1522793862, 1522799999, row["Subreddit"])
    if comments or posts:
        RedditDB.main(row["Subreddit"], row["Symbol"])

for i, row in subreddits.iterrows():
    RedditCoin.main(row["Subreddit"], row["Symbol"])
    

