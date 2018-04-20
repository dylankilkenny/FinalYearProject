import sys
sys.path.append('../utility')
from logger import log

import RedditDB
import redditapi
import RedditPosts
import pandas as pd
import sys
import json




def main():
    # Load subreddit list 
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')

    # post_ids = RedditPosts.main(sys.argv[1])

    # Loop through subreddits
    for i, row in subreddits.iterrows():
        
        # if already processed continue 
        if row["done"] == 1:
            continue
        if row["Subreddit"] != "btc":
            continue

        log("Fetching("+str(i+1)+"/"+str(len(subreddits))+"): " + row["Subreddit"])
        
        # Gather posts and save to csv
        (comments, posts) = redditapi.GetData(row["Subreddit"])

        # if any posts or comments process them
        if comments or posts:
            comments_path = "{0}data/reddit/comments_{1}.csv".format(PATH, row["Subreddit"])
            posts_path = "{0}data/reddit/posts_{1}.csv".format(PATH, row["Subreddit"])
            currency_symbols_path = "{0}data/CurrencySymbols.csv".format(PATH)
            stopwords_path = "{0}data/stopwords.csv".format(PATH)
            banned_path = "{0}data/banned_users.json".format(PATH)
            RedditDB.main(row["Subreddit"], comments_path, posts_path, currency_symbols_path, stopwords_path, banned_path)
         
        # Mark as done
        subreddits.loc[subreddits['Subreddit'] == row["Subreddit"], 'done'] = 1
        subreddits.to_csv('../data/reddit/SubredditList.csv' , sep=',', index=False)

    # When loop is finished set all rows back to undone
    subreddits["done"] = 0
    # save
    subreddits.to_csv('../data/reddit/SubredditList.csv' , sep=',', index=False)

    # Empty submissions file
    obj = {}
    obj['data'] = []
    with open("submissions.json", "w") as jsonFile:
        json.dump(obj, jsonFile)

    # for i, row in subreddits.iterrows():
    #     RedditCoin.main(row["Subreddit"], row["Symbol"])

if __name__ == '__main__':
    if "prod" in sys.argv:
        PATH = "/home/dylan/python/"
    else:
        PATH = "../"

    main()

    
