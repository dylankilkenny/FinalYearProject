import RedditDB
import redditapi
import RedditCoin
import RedditPosts
import pandas as pd
import sys

subreddits = pd.read_csv('../data/reddit/SubredditList.csv')

# post_ids = RedditPosts.main(sys.argv[1])

for i, row in subreddits.iterrows():

    print("Fetching("+str(i+1)+"/"+str(len(subreddits))+"): " + row["Subreddit"])

    (comments, posts) = redditapi.GetData(row["Subreddit"])
    if comments or posts:
        RedditDB.main(row["Subreddit"], row["Symbol"])

# for i, row in subreddits.iterrows():
#     RedditCoin.main(row["Subreddit"], row["Symbol"])
    
