import RedditDB
import redditapi
import RedditCoin
import pandas as pd

subreddits = pd.read_csv('../data/SubredditList.csv')

for i, row in subreddits.iterrows():
    (comments, posts) = redditapi.GetData(1522315759, 1522322959, row["Subreddit"])
    if comments or posts:
        RedditDB.main(row["Subreddit"], row["Symbol"])
    if i == 5:
        break

for i, row in subreddits.iterrows():
    RedditCoin.main(row["Subreddit"], row["Symbol"])
    if i == 5:
        break

