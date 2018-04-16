import pandas as pd
import praw
reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

subreddits = pd.read_csv('../data/reddit/SubredditList.csv')

# post_ids = RedditPosts.main(sys.argv[1])
subs = []
# Loop through subreddits
for i, row in subreddits.iterrows():
    subs.append(row["Subreddit"])

subs = '+'.join(subs)
subreddit = reddit.subreddit(subs)
for submission in subreddit.stream.submissions():
    print(submission.subreddit)