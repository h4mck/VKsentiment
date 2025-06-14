from time import sleep
from datetime import datetime, timezone
import pprint
import pymongo
from transformers import pipeline
from pymongo import UpdateOne, MongoClient

def getUserDomain(url: str):
    domain = url.split('/')[-1].split('?')[0]
    return domain

def getUndoneComments(collection: pymongo.collection):
    pipeline = [
        {"$unwind": "$comments"},
        {"$match": {"comments.status":
                        {"$in": ["unprocessed", "in progress"]}
                    }
         },
        {"$group": {
            "_id": "$_id",
            "undone_comments": {"$push": "$comments"}
        }}
    ]

    posts = collection.aggregate(pipeline)
    return posts

def commentsHandler(post: {}, collection: pymongo.collection):
    comments_to_analyze = []
    for comment in post["undone_comments"]:
        if comment["status"] == "in progress":
            if (datetime.now(timezone.utc) - comment["last_updated"].replace(tzinfo=timezone.utc)).total_seconds() <= 300:
                comments_to_analyze.append(comment)
        else:
            comments_to_analyze.append(comment)
        if len(comments_to_analyze) == 50:
            break

    if len(comments_to_analyze) == 0:
        return None

    for n in range(10):
        time = datetime.now(timezone.utc)
        result = collection.update_one(
        {"_id": post["_id"]},
        {"$set": {
            "comments.$[elem].status": "in progress",
            "comments.$[elem].last_updated": time}},
        array_filters=[{"elem._id": {"$in": [comment["_id"] for comment in comments_to_analyze]}}]
        )
        if result.modified_count == 1:
            break
        if n == 9:
            return None
        sleep(0.5)

    for comment_id in range(len(comments_to_analyze)):
        analyze_result = model(comments_to_analyze[comment_id]["text"])[0]
        comments_to_analyze[comment_id]["sentiment"] = analyze_result["label"]
        comments_to_analyze[comment_id]["score"] = round(analyze_result["score"], 3)
        comments_to_analyze[comment_id]["status"] = "processed"

    for n in range(10):
        # time = datetime.now(timezone.utc)
        result = collection.update_one({"_id": post["_id"]},
                                       {
                                           "$pull":
                                               {"comments": {"_id": {"$in": [comment["_id"] for comment in comments_to_analyze]}}},
                                           "$push":
                                               {"processed_comments": {"$each": comments_to_analyze}}
                                       })
        if result.modified_count == 1:
            break
        if n == 9:
            return None
        sleep(0.5)

    return None

def mongoMain(UserUrl: str, model):
    mongoClient = MongoClient("localhost", 2017)

    with mongoClient:

        db = mongoClient.sentiment_database
        username = getUserDomain(UserUrl)

        collection = db[username]

        # count = 0
        while True:
            posts = getUndoneComments(collection)
            try:
                current_post = next(posts)
                commentsHandler(current_post, collection)

                # count += 50
                # if count >= 200:
                #     break
            except StopIteration:
                sleep(5)
                # delete next line!
                break

        for post in collection.find():
            print(f"{len(post["comments"])=}")
            try:
                print(f"{len(post["processed_comments"])=}")
                print(post["processed_comments"])
            except KeyError:
                pass

model = pipeline(model="seara/rubert-tiny2-russian-sentiment")
mongoMain("https://vk.com/torrent_igruha", model)