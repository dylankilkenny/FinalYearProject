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
    j = 2
    # Loop through subreddits
    for i, row in subreddits.iterrows():
        FILE_NAME = "comments_"+row["Subreddit"]+".csv"
        if FILE_NAME in FILES:
            comments_path = "{0}data/reddit/old/comments_{1}.csv".format(PATH, row["Subreddit"])
            posts_path = "{0}data/reddit/old/posts_{1}.csv".format(PATH, row["Subreddit"])
            currency_symbols_path = "{0}data/CurrencySymbols.csv".format(PATH)
            stopwords_path = "{0}data/stopwords.csv".format(PATH)
            banned_path = "{0}data/banned_users.json".format(PATH)
            print("\nProcessing ("+str(j)+"/"+str(FILES_LENGTH)+"): " + row["Subreddit"])
            RedditDB.main(row["Subreddit"], comments_path, posts_path, currency_symbols_path, stopwords_path, banned_path)
            j += 2

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

    
