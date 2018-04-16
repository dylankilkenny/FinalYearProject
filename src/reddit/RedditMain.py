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


        print("Fetching("+str(i+1)+"/"+str(len(subreddits))+"): " + row["Subreddit"])
        
        # Gather posts and save to csv
        (comments, posts) = redditapi.GetData(row["Subreddit"])

        # if any posts or comments process them
        if comments or posts:
            RedditDB.main(row["Subreddit"], row["Symbol"], False)
        
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
    main()

    
