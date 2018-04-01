
import praw
import csv
import pandas as pd

CURRENCYS = pd.read_csv('../data/reddit/subredditlist.csv')
subs = []
for i, row in CURRENCYS.iterrows():
    subs.append(row["Subreddit"])

subs = '+'.join(subs)
reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

for comment in reddit.subreddit(subs).stream.comments():
    print(comment.subreddit)