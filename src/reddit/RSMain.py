import RedditDB
import redditapi
import RedditPosts
import pandas as pd
import sys
import json
from distutils.dir_util import copy_tree
import os, shutil
from os import listdir

def main():
    # Load subreddit list 
    subreddits = pd.read_csv(PATH + 'data/reddit/SubredditList.csv')

    # post_ids = RedditPosts.main(sys.argv[1])
    copy_tree(PATH + "data/reddit/latest", PATH + "data/reddit/old")
    RemoveFiles()

    FILES = listdir(PATH + "data/reddit/old")
    FILES_LENGTH = len(FILES)
    j = 1
    # Loop through subreddits
    for i, row in subreddits.iterrows():
        FILE_NAME = "comments_"+row["Subreddit"]+".csv"
        if FILE_NAME in FILES:
            print("Processing ("+str(j)+"/"+str(FILES_LENGTH)+"): " + row["Subreddit"])
            RedditDB.main(row["Subreddit"], row["Symbol"], True)
            j += 1

def RemoveFiles():
    LATEST_PATH = PATH + "data/reddit/latest"
    shutil.rmtree(LATEST_PATH)
    if not os.path.exists(LATEST_PATH):
        os.makedirs(LATEST_PATH)

if __name__ == '__main__':
    if "prod" in sys.argv:
        PATH = "/home/dylan/python/"
    else:
        PATH = "../"
    main()

    
