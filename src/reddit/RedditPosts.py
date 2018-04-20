import sys
sys.path.append('../utility')
from logger import log

import time
import praw
import pandas as pd
import requests
import json
import sys


def getPushshiftData(after, sub, before):
    url = 'https://api.pushshift.io/reddit/search/submission?&size=1000&after='+str(after)+'&before='+str(before)+'&subreddit='+str(sub)
    r = requests.get(url)
    data = json.loads(r.text)
    return data['data']





def main(after, before):
    subreddits = pd.read_csv('../data/reddit/SubredditList.csv')
    data = []
    post_ids = []

    for i, row in subreddits.iterrows():
        
        sub = row["Subreddit"]
        log("Fetching post ID's for "+ row["Subreddit"]+" ("+str(i+1)+"/"+str(len(subreddits))+")")

        data = getPushshiftData(sub=sub, after=after, before=before)
        while len(data) > 0:
            for submission in data:
                post_ids.append(submission["id"])
            data = getPushshiftData(sub=sub, after=data[-1]['created_utc'], before=before)

        with open("submissions.json", "r") as jsonFile:
            js = json.load(jsonFile)

        obj = {}
        obj['subreddit'] = sub
        obj['id'] = post_ids
        # data.append(obj)
        js["data"].append(obj)

        with open("submissions.json", "w") as jsonFile:
            json.dump(js, jsonFile)
        
        log("No. Posts: "+ str(len(post_ids)))
        post_ids = []
    
    return data

if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2])
    
