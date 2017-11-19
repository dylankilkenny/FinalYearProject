import praw
import csv
import sys


# init Reddit instance
reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To",
                     password='cryptoscraping999', user_agent='USERAGENT',
                     username='CryptoScraper')
subreddits = "CryptoCurrency+Bitcoin+btc+Altcoins+BitcoinMarkets+CryptoMarkets"

post = []
comments = []





def GetData():
    
    startdate = 1510761773
    enddate = 1510848173
    post.append(["ID", "Title", "Subreddit", "Date", "Score", "No. Comments", "Author", "Shortlink"])
    comments.append(["ID", "PostID", "Body", "Date", "Score", "Author"])
    postscount = 0

    for submission in reddit.subreddit(subreddits).submissions(start=startdate, end=enddate, extra_query=None):
        
        if submission.num_comments == 0:
            continue
            
        post.append([submission.id, submission.title, submission.subreddit_name_prefixed, submission.created_utc, submission.score, submission.num_comments, submission.author, submission.shortlink])

        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            comments.append([comment.id, submission.id, comment.body, comment.created_utc, comment.score, comment.author])

        postscount+=1
        print("Posts found " + str(postscount))

        if(postscount ==1000):
            SaveData()
            break


def SaveData():
    
    #save posts
    f = open('../data/post.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(post)):
            writer.writerow(post[i])
    finally:
        f.close()

    #save comments
    f = open('../data/comments.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(comments)):
            writer.writerow(comments[i])
    finally:
        f.close()

if __name__ == "__main__":
    GetData()