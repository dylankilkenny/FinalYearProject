import time
import praw

reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

submissionlist = reddit.subreddit("learnpython").submissions(519862400, 1522886399)

for submission in submissionlist:
    print(submission)