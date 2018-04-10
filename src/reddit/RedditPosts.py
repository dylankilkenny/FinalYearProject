import time
import praw
import pandas as pd
import requests
import json
import sys


def getPushshiftData(after, sub):
    url = 'https://api.pushshift.io/reddit/search/submission?&size=1000&after='+str(after)+'&subreddit='+str(sub)
    r = requests.get(url)
    data = json.loads(r.text)
    return data['data']





def main(date):
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    post_ids = []

    for i, row in subreddits.iterrows():
        
        sub = row["Subreddit"]
        print(sub)

        data = getPushshiftData(sub=sub, after=date)
        while len(data) > 0:
            for submission in data:
                post_ids.append(submission["id"])
            data = getPushshiftData(sub=sub, after=data[-1]['created_utc'])

        with open("submissions.json", "r") as jsonFile:
            js = json.load(jsonFile)

        obj = {}
        obj['subreddit'] = sub
        obj['id'] = post_ids
        js["data"].append(obj)

        with open("submissions.json", "w") as jsonFile:
            json.dump(js, jsonFile)
        print(len(post_ids))
        post_ids = []

if __name__ == '__main__':
    main(sys.argv[1])
    
