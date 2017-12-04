import praw
import csv
import sys
import os
import datetime
import multiprocessing as mp

def GetData(startdate, enddate, subreddit):

    #Set submission list
    submissionlist = reddit.subreddit(subreddit).submissions(start=startdate, end=enddate, extra_query=None)
    #Set .csv Headers
    posts.append(["ID", "Title", "Subreddit", "Date", "Score", "No. Comments", "Author", "Shortlink"])
    comments.append(["ID", "PostID", "Body", "Date", "Score", "Author"])

    for submission in submissionlist:

        #If post does not have any comments skip it
        if submission.num_comments == 0:
            continue    
        #Add post to the list
        posts.append([submission.id, submission.title, submission.subreddit_name_prefixed, submission.created_utc, submission.score, submission.num_comments, submission.author, submission.shortlink])
        #If comments have alot of replies this will load them also
        submission.comments.replace_more(limit=0)
        #Add comments to list
        for comment in submission.comments.list():
            comments.append([comment.id, submission.id, comment.body, comment.created_utc, comment.score, comment.author])

        Logger(submission.created_utc)
    
    SaveData()
        

            


#Display mining information to the terminal
def Logger(date):

    date = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
    print("Posts processed: " + str(len(posts)) + " - Comments processed: " + str(len(comments)) + " - Last Post: " + str(date), end="\r" )
    
    

    


def SaveData():
    
    #save posts
    f = open('../data/post_'+filename+'.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(posts)):
            writer.writerow(posts[i])
    finally:
        f.close()

    #save comments
    f = open('../data/comments_'+filename+'.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(comments)):
            writer.writerow(comments[i])
    finally:
        f.close()


# class myThread (threading.Thread):
#     def __init__(self, startdate, enddate, subreddit):
#         threading.Thread.__init__(self)
#         self.startdate = startdate
#         self.enddate = enddate
#         self.subreddit = subreddit

#     def run(self):
#         print ("Starting " + self.subreddit)
#         # Get lock to synchronize threads
#         # threadLock.acquire()
#         GetData(self.startdate, self.enddate, self.subreddit)
#         # Free lock to release next thread
#         # threadLock.release()


if __name__ == "__main__":
    
    # init Reddit instance
    reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

    #store posts and comments
    posts = []
    comments = []
    postscount = 0
    commentcount = 0

    jobs = []

    subreddit = sys.argv[1]
    startdate = sys.argv[2]
    enddate = sys.argv[3]

    filename = subreddit + "_" + startdate + "_" + enddate

    GetData(startdate, enddate, subreddit)
    
    # worker_1 = mp.Process(name='worker_1', target=GetData, args=("worker_1", "1510272000",  "1511136000", "CryptoCurrency",))
    # worker_2 = mp.Process(name='worker_2', target=GetData, args=("worker_2", "1510272000",  "1511136000", "Bitcoin",))
    # worker_3 = mp.Process(name='worker_3', target=GetData, args=("worker_3", "1510272000",  "1511136000", "btc",))
    # worker_4 = mp.Process(name='worker_4', target=GetData, args=("worker_4", "1510272000",  "1511136000", "Altcoins",))
    # worker_5 = mp.Process(name='worker_5', target=GetData, args=("worker_5", "1510272000",  "1511136000", "BitcoinMarkets",))
    # worker_6 = mp.Process(name='worker_6', target=GetData, args=("worker_6", "1510272000", "1511136000", "CryptoMarkets",))


    # worker_1.start()
    # worker_2.start()
    # worker_3.start()
    # worker_4.start()
    # worker_5.start()
    # worker_6.start()

    # jobs.append(worker_1)
    # jobs.append(worker_2)
    # jobs.append(worker_3)
    # jobs.append(worker_4)
    # jobs.append(worker_5)
    # jobs.append(worker_6)

    # for job in jobs:
    #     job.join()

    print ("Exiting Main Thread")

    