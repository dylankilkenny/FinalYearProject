import pandas as pd
import requests
import json

def getPushshiftData(after, sub):
    url = 'https://api.pushshift.io/reddit/search/submission?&size=1000&after='+str(after)+'&subreddit='+str(sub)
    r = requests.get(url)
    data = json.loads(r.text)
    return data['data']

post_ids = []
sub='btc'
data = getPushshiftData(sub=sub, after='1519914799')

while len(data) > 0:

    for submission in data:
        post_ids.append(submission["id"])
    data = getPushshiftData(sub=sub, after=data[-1]['created_utc'])

obj = {}
obj['sub'] = "BTC"
obj['id'] = post_ids

with open("submissions.json", "w") as jsonFile:
    json.dump(obj, jsonFile)