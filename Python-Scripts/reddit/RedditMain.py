import RedditDB
import redditapi
import pandas as pd

subreddits = pd.read_csv('../data/SubredditList.csv')

for i, row in subreddits.iterrows():
    (comments, posts) = redditapi.GetData(1522228474, 1522239274, row["Subreddit"])

    if comments or posts:
        RedditDB.main(row["Subreddit"])

    