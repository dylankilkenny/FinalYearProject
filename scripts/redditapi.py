import praw
import csv
import sys


subreddits = ["CryptoCurrency", "Bitcoin", "btc", "Altcoins", "BitcoinMarkets", "CryptoMarkets"]
titles = []
shortlink = []
comments = []

# init Reddit instance
reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To",
                     password='cryptoscraping999', user_agent='USERAGENT',
                     username='CryptoScraper')

# for subreddit in subreddits:

#     for submission in reddit.subreddit(subreddit).hot(limit=5):
        
#         print(submission.title)

posts = 0
for submission in reddit.subreddit("CryptoCurrency").submissions(start=1510761773, end=1510848173, extra_query=None):
    
    titles.append(submission.title)
    shortlink.append(submission.shortlink)
    postcomments = submission.comments.list()
    comments.append(postcomments)
    posts+=1
    print("Posts found" + str(posts))
    if(posts ==10):
        break



f = open('../data/post.csv', 'wt')
try:
    writer = csv.writer(f)
    writer.writerow( ('ID', 'Title', 'Shortlink') )
    for i in range(len(titles)):
        writer.writerow([i+1, titles[i], shortlink[i]])
finally:
    f.close()

f = open('../data/comments.csv', 'wt')
try:
    writer = csv.writer(f)
    writer.writerow( ('ID', 'Comment') )
    for i in range(len(comments)):
        for k in range(len(comments[i])):
            writer.writerow([i+1, comments[i][k]])
finally:
    f.close()

# print (open('test.csv', 'rt').read())