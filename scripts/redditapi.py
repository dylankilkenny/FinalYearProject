#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering reddit posts"""

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
        global CommentCounter
        global PostCounter
        CommentCounter += len(posts)        
        PostCounter += len(comments)        
        SaveData()
        Logger(submission.created_utc)
    
    
        

            


#Display mining information to the terminal
def Logger(date):

    date = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
    print("Posts processed: " + str(CommentCounter) + " - Comments processed: " + str(PostCounter) + " - Last Post: " + str(date), end="\r")
    
    

    


def SaveData():
    
    #save posts
    f = open('data/post_'+filename+'.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(posts)):
            writer.writerow(posts[i])
    finally:
        f.close()
        posts[:] = []
        

    #save comments
    f = open('data/comments_'+filename+'.csv', 'wt')
    try:
        writer = csv.writer(f)
        for i in range(len(comments)):
            writer.writerow(comments[i])
    finally:
        f.close()
        comments[:] = []
        
    


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
    CommentCounter = 0
    PostCounter = 0

    jobs = []

    subreddit = sys.argv[1]
    startdate = sys.argv[2]
    enddate = sys.argv[3]
    sd = datetime.datetime.fromtimestamp(float(startdate)).strftime('%Y-%m-%d')
    ed = datetime.datetime.fromtimestamp(float(enddate)).strftime('%Y-%m-%d')

    # orig = datetime.datetime.fromtimestamp(float(startdate))
    # timestamps = []
    # for i in range(13):
    #     new = orig - datetime.timedelta(days=30)
    #     timestamps.append(new.timestamp())
    #     orig = new


    filename = subreddit + "_" + sd + "_" + ed 
    GetData(startdate, enddate, subreddit)
    
    # worker_1 = mp.Process(name='worker_1', target=GetData, args=(startdate,  timestamps[0], subreddit))
    # worker_2 = mp.Process(name='worker_2', target=GetData, args=(timestamps[0],  timestamps[1], subreddit,))
    # worker_3 = mp.Process(name='worker_3', target=GetData, args=(timestamps[1],  timestamps[2],subreddit,))
    # worker_4 = mp.Process(name='worker_4', target=GetData, args=(timestamps[2],  timestamps[3], subreddit,))
    # worker_5 = mp.Process(name='worker_5', target=GetData, args=( timestamps[3],  timestamps[4], subreddit,))
    # worker_6 = mp.Process(name='worker_6', target=GetData, args=(timestamps[4], timestamps[5], subreddit,))
    # worker_7 = mp.Process(name='worker_7', target=GetData, args=( timestamps[5], timestamps[6], subreddit,))
    # worker_8 = mp.Process(name='worker_8', target=GetData, args=(timestamps[6], timestamps[7], subreddit,))
    # worker_9 = mp.Process(name='worker_9', target=GetData, args=(timestamps[7], timestamps[8], subreddit,))
    # worker_10 = mp.Process(name='worker_10', target=GetData, args=(timestamps[8], timestamps[9], subreddit,))
    # worker_11 = mp.Process(name='worker_11', target=GetData, args=(timestamps[9], timestamps[10], subreddit,))
    # worker_12 = mp.Process(name='worker_12', target=GetData, args=(timestamps[10], timestamps[11], subreddit,))
    # worker_13 = mp.Process(name='worker_12', target=GetData, args=(timestamps[11], timestamps[12], subreddit,))



    # worker_1.start()
    # worker_2.start()
    # worker_3.start()
    # worker_4.start()
    # worker_5.start()
    # worker_6.start()
    # worker_7.start()
    # worker_8.start()
    # worker_9.start()
    # worker_10.start()
    # worker_11.start()
    # worker_12.start()
    # worker_13.start()

    # jobs.append(worker_1)
    # jobs.append(worker_2)
    # jobs.append(worker_3)
    # jobs.append(worker_4)
    # jobs.append(worker_5)
    # jobs.append(worker_6)
    # jobs.append(worker_7)
    # jobs.append(worker_8)
    # jobs.append(worker_9)
    # jobs.append(worker_10)
    # jobs.append(worker_11)
    # jobs.append(worker_12)
    # jobs.append(worker_13)

    # for job in jobs:
    #     job.join()

    # print ("Exiting Main Thread")

    