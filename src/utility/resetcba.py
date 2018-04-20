from pymongo import MongoClient
import time
# Connect to DB
client = MongoClient("mongodb://localhost:27017/")
db = client.dev

result = db.currencybyauthor.update_one(
    {},
    {   
        "$set":{
            "currency_by_author": []
        }
    }
)
t = time.strftime("%Y-%m-%d %H:%M:%S")
print("[{0}] Modified: {1}".format(t, result.modified_count))