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
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')

    # post_ids = RedditPosts.main(sys.argv[1])
    copy_tree("../data/reddit/latest", "../data/reddit/old")
    RemoveFiles()

    FILES = listdir("../data/reddit/old")
    print(FILES)
    FILES_LENGTH = len(FILES)
    # Loop through subreddits
    for i, row in subreddits.iterrows():
        FILE_NAME = "comments_"+row["Subreddit"]+".csv"
        j = 1
        if FILE_NAME in FILES:
            print("Processing ("+str(j)+"/"+str(FILES_LENGTH)+"): " + row["Subreddit"])
            RedditDB.main(row["Subreddit"], row["Symbol"], True)
            j += 1

def RemoveFiles():
    PATH = "../data/reddit/latest"
    shutil.rmtree(PATH)
    if not os.path.exists(PATH):
        os.makedirs(PATH)

if __name__ == '__main__':
    main()

    
