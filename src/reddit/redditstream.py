#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering reddit posts"""


import csv
import sys
import os
import datetime
from pathlib import Path
from prawcore.exceptions import RequestException
import praw
import json
import pandas as pd





def GetData(subs):
    
    # init Reddit instance
    reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

    #store posts and comments
    posts = []
    comments = []
    CommentCounter = 0
    PostCounter = 0

    subreddits = reddit.subreddit(subs)
    for comment in subreddits.stream.comments():

        submission = comment.submission        
        comments.append([comment.id, comment.id, comment.body, comment.created_utc, comment.score, comment.author])
        CommentCounter += 1        
        SaveComments(str(submission.subreddit), comments)
        Logger(submission.created_utc, CommentCounter, PostCounter)
        comments[:] = []

        PostIDs = pd.read_csv('../data/reddit/PostIDs.csv')

        if PostIDs['ID'].str.contains(submission.id).any():
            continue
        PostIDs.loc[len(PostIDs)] = submission.id 
        PostIDs.to_csv('../data/reddit/PostIDs.csv', sep=',', index=False)
        #Add post to the list
        posts.append([submission.id, submission.title, submission.created_utc, submission.score, submission.num_comments, submission.author])
        SavePosts(str(submission.subreddit), posts)
        PostCounter += 1
        Logger(submission.created_utc,CommentCounter,PostCounter)
        posts[:] = []
        
        
    



    
        
#Display mining information to the terminal
def Logger(date,CommentCounter,PostCounter):

    date = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
    print("Posts processed: " + str(PostCounter) + " - Comments processed: " + str(CommentCounter) + " - Last Post: " + str(date), end="\r")

    

def SavePosts(subreddit, posts):
    p = Path('../data/reddit/latest/posts_'+subreddit+'.csv')
    if p.is_file() == False:
        posts.insert(0, ["ID", "Title", "Date", "Score", "No. Comments", "Author"])
        
    #save posts
    f = open('../data/reddit/latest/posts_'+subreddit+'.csv', 'a')
    try:
        writer = csv.writer(f)
        for i in range(len(posts)):
            writer.writerow(posts[i])
    finally:
        f.close()
        
def SaveComments(subreddit, comments):
    c = Path('../data/reddit/latest/comments_'+subreddit+'.csv')
    if c.is_file() == False:
        #Set .csv Headers
        comments.insert(0, ["ID", "PostID", "Body", "Date", "Score", "Author"])
    #save comments
    f = open('../data/reddit/latest/comments_'+subreddit+'.csv', 'a')
    try:
        writer = csv.writer(f)
        for i in range(len(comments)):
            writer.writerow(comments[i])
    finally:
        f.close()


if __name__ == '__main__':
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    # post_ids = RedditPosts.main(sys.argv[1])
    subs = []
    # Loop through subreddits
    for i, row in subreddits.iterrows():
        subs.append(row["Subreddit"])
    subs = '+'.join(subs)
    GetData(subs)
    