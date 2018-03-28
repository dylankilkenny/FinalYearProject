#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Gathering reddit posts"""

import praw
import csv
import sys
import os
import datetime
import multiprocessing as mp
from pathlib import Path



def GetData(startdate, enddate, subreddit):
    
    # init Reddit instance
    reddit = praw.Reddit(client_id='-EoTqkifUHRcgQ', client_secret="S0OuT5JY-NQKu9DBeOtF7gd72To", password='cryptoscraping999', user_agent='USERAGENT', username='CryptoScraper')

    #store posts and comments
    posts = []
    comments = []
    CommentCounter = 0
    PostCounter = 0

    sd = datetime.datetime.fromtimestamp(float(startdate)).strftime('%Y-%m-%d')
    ed = datetime.datetime.fromtimestamp(float(enddate)).strftime('%Y-%m-%d')

    #Set submission list
    submissionlist = reddit.subreddit(subreddit).submissions(start=startdate, end=enddate, extra_query=None)
    #Set .csv Headers
    posts.append(["ID", "Title", "Subreddit", "Date", "Score", "No. Comments", "Author", "Shortlink"])
    comments.append(["ID", "PostID", "Body", "Date", "Score", "Author"])
    #Save Headers and reset lists to empty
    #This allows pandas to carry on as normal if 
    #no comments are present 
    SavePosts(subreddit, posts)
    SaveComments(subreddit, comments) 
    posts[:] = []
    comments[:] = []
    

    for submission in submissionlist:
        #Add post to the list
        posts.append([submission.id, submission.title, submission.subreddit_name_prefixed, submission.created_utc, submission.score, submission.num_comments, submission.author, submission.shortlink])
        SavePosts(subreddit, posts)
        PostCounter += len(posts)
        Logger(submission.created_utc,subreddit,CommentCounter,PostCounter)
        posts[:] = []

        #If post does not have any comments skip it
        if submission.num_comments == 0:
            continue
              
        #If comments have alot of replies this will load them also
        submission.comments.replace_more(limit=0)
        #Add comments to list
        for comment in submission.comments.list():
            comments.append([comment.id, submission.id, comment.body, comment.created_utc, comment.score, comment.author])
        CommentCounter += len(comments)        
        SaveComments(subreddit, comments)
        Logger(submission.created_utc,subreddit,CommentCounter,PostCounter)
        comments[:] = []
    
    p = Path('../data/posts_'+subreddit+'.csv')
    c = Path('../data/comments_'+subreddit+'.csv')

    return (c.is_file(),p.is_file())



    
        
#Display mining information to the terminal
def Logger(date,subreddit,CommentCounter,PostCounter):

    date = datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')
    print("Posts processed ("+subreddit+"): " + str(PostCounter) + " - Comments processed: " + str(CommentCounter) + " - Last Post: " + str(date), end="\r")
    
    

def SavePosts(subreddit, posts):
    
    #save posts
    f = open('../data/posts_'+subreddit+'.csv', 'a')
    try:
        writer = csv.writer(f)
        for i in range(len(posts)):
            writer.writerow(posts[i])
    finally:
        f.close()
        
def SaveComments(subreddit, comments):
    
    #save comments
    f = open('../data/comments_'+subreddit+'.csv', 'a')
    try:
        writer = csv.writer(f)
        for i in range(len(comments)):
            writer.writerow(comments[i])
    finally:
        f.close()
        
    