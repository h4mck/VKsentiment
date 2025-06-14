import asyncio
import time
from datetime import datetime, timezone

import pymongo.errors
from holoviews.operation import operation
from lazy_object_proxy.utils import await_
from pymongo import AsyncMongoClient, ReplaceOne
import pprint

import aiohttp
from aiohttp import ClientSession


app_storage = {}

def postLink(owner_id: int, post_id: int):
    return f"https://vk.com/wall{owner_id}_{post_id}"

def getUserDomain(url: str):
    domain = url.split('/')[-1].split('?')[0]
    return domain

    #domain is a short name!
async def getUserId(domain: str):
    url = "https://api.vk.com/method/utils.resolveScreenName"
    params = {
        "access_token": 'b3dcd37bb3dcd37bb3dcd37b9fb0ec6657bb3dcb3dcd37bdbcaeb2bed76c4982af9d199',
        "screen_name": domain,
        "v": "5.199"
    }
    async with app_storage["session"].get(url=url, params=params) as response:
        data = await response.json()
        if "error" in data:
            raise Exception(f"VK API Error: {data['error']['error_msg']}")
        return data["response"]

def getPostId(url: str):
    url = url.split("wall")[-1]
    post_id = url.split("_")[1]
    owner_id = url.split("_")[0]
    return int(owner_id), int(post_id)

async def getPostsInv(domain: str, count: int, offset: int = 0):
    url = "https://api.vk.com/method/wall.get"
    params = {
        "access_token": 'b3dcd37bb3dcd37bb3dcd37b9fb0ec6657bb3dcb3dcd37bdbcaeb2bed76c4982af9d199',
        "domain": domain,
        "offset": offset,
        "count": count,
        # "filter": "all",
        "filter": "owner",
        "v": "5.199"
    }
    while True:
        async with app_storage["session"].get(url=url, params=params) as response:
            # print(response.status)
            data = await response.json()
            if "error" in data:
                await asyncio.sleep(0.5)
                continue
            return data["response"]["items"], data["response"]["count"]

async def getCommentsInv(owner_id: int, post_id: int, count: int, offset: int = 0, comment_id: int = None):
    url = "https://api.vk.com/method/wall.getComments"
    if comment_id is not None:
        params = {
            "access_token": 'b3dcd37bb3dcd37bb3dcd37b9fb0ec6657bb3dcb3dcd37bdbcaeb2bed76c4982af9d199',
            "owner_id": owner_id,
            "post_id": post_id,
            "need_likes": 1,
            "offset": offset,
            "count": count,
            "sort": "desc",
            "preview_length": 0,
            "comment_id": comment_id,
            "v": "5.199"
        }
    else:
        params = {
            "access_token": 'b3dcd37bb3dcd37bb3dcd37b9fb0ec6657bb3dcb3dcd37bdbcaeb2bed76c4982af9d199',
            "owner_id": owner_id,
            "post_id": post_id,
            "need_likes": 1,
            "offset": offset,
            "count": count,
            "sort": "desc",
            "preview_length": 0,
            "v": "5.199"
        }

    while True:
        async with app_storage["session"].get(url=url, params=params) as response:
            data = await response.json()
            if "error" in data:
                await asyncio.sleep(0.5)
                continue
            return data["response"]["items"], data["response"]["count"]

        #слудующий шаг: напиши (по примеру getPosts()) getComments() - в ней будешь вызывать getCommentsInv для нынешнего поста через TaskGroup +
#       commentWorker() (работает по примеру postWorker, только с коммами); так как нам надо получить все коммы, нужно не просто вызывать с оффсетом для поста,
#       а залазить в ветки коммов - для этого будем (наверно) рекурсивно вызывать getComments (а не getCommentsInv!) с новым параметром comment_id (добавь и
#       в getCommentsInv) который и поможет получать ветки (просто проверка: если у комма в поле thread в поле count != 0, запоминаем его id
#       и запускаем ещё один getComments)

async def commentWorker(queue_in, result):
    while True:
        try:
            owner_id, post_id, count, offset, comment_id = queue_in.get_nowait()
            # print(f'Task {owner_id=}, {post_id=}, {count=}, {offset=}, {comment_id=} started!')

            comments = (await getCommentsInv(owner_id, post_id, count, offset, comment_id))[0]

            # print(result)
            result.extend(comments)
            # print(f'Task {owner_id=}, {post_id=}, {count=}, {offset=}, {comment_id=} done!')
        except asyncio.QueueEmpty:
            # print(f'Queue is empty. Shutting down...')
            break

async def postWorker(queue_in, result):
    while True:
        try:
            domain, count, offset = queue_in.get_nowait()
            # print(f'Task {domain=}, {count=}, {offset=} started!')

            posts = (await getPostsInv(domain, count, offset))[0]
            # print(len(posts))
            result.extend(posts)
            # print(f'Task {domain=}, {count=}, {offset=} done!')
        except asyncio.QueueEmpty:
            # print(f'Queue is empty. Shutting down...')
            break

async def getPosts(domain: str, count: int, n_workers: int = 10):

    #domain is a short name!
    # if count == 0:
        # get all posts
    posts, n_posts = await getPostsInv(domain, 100)
    if count < 100:
        return posts[:count]
    if count != 0:
        n_posts = min(n_posts, count)
    all_posts = list(posts)

    task_queue = asyncio.Queue()
    for offset in range(len(posts), n_posts, 100):
        # all_posts.extend((await getPostsInv(domain, 100, offset))[0])
        task_queue.put_nowait((domain, 100, offset))

    print(task_queue)
    async with asyncio.TaskGroup() as tg:
        for _ in range(n_workers):
            tg.create_task(postWorker(task_queue, all_posts))

    return all_posts

# в данном случае url - ссылка на пост
async def getComments(url: str, count: int, comment_id: int = None, n_workers: int = 5):
    # summ = 0
    owner_id, post_id = getPostId(url)
    coms, n_coms = await getCommentsInv(owner_id, post_id, 100, comment_id = comment_id)
    # summ += n_coms
    # print("n_coms: ", summ)


    if count < 100 and count != 0:
        count = min(n_coms, count)
        return coms[:count]

    if count != 0:
        n_coms = min(n_coms, count)

    all_coms = list(coms)
    if len(all_coms) < 100:
        n_coms = len(coms)

    task_queue = asyncio.Queue()

    for offset in range(len(all_coms), n_coms, 100):
        task_queue.put_nowait((owner_id, post_id, 100, offset, comment_id))


    async with asyncio.TaskGroup() as tg:
        for _ in range(n_workers):
            tg.create_task(commentWorker(task_queue, all_coms))

    thread_coms = {}
    # print(all_coms)
    for comment in all_coms:
        try:
            if int(comment["thread"]["count"]) != 0:
                thread_coms[str(comment["id"])] = int(comment["thread"]["count"])
        except KeyError:
            break
    # print("thread_coms: ", thread_coms)

    async with asyncio.TaskGroup() as tg:
        inner_tasks = [tg.create_task(getComments(url, thread_coms[id], int(id))) for id in thread_coms]

    for task in inner_tasks:
        all_coms.extend(task.result())


    return all_coms



async def mainComms():
    app_storage['session'] = ClientSession()

    async with app_storage["session"]:
        # coms, count = await getCommentsInv(-126357200, 220994)
        # print(coms[-3:])
        # print(len(coms))
        # print(count)
        res = await getComments("https://vk.com/igm?from=groups&w=wall-30602036_11598793", 0)

        print(len(res))
        print(type(res))



        # coms2 = []
        # task_queue = asyncio.Queue()
        # task_queue.put_nowait((-126357200, 220994, 0, 100, None))
        # await commentWorker(task_queue, coms2)
        # print(len(coms2))
        # for com in coms:
        #     print(com["text"])

async def mongoMain(UserUrl: str, vk_post: {}, comms: []):
    mongoClient = AsyncMongoClient("localhost", 2017)

    async with mongoClient:
        await mongoClient.aconnect()


        db = mongoClient.sentiment_database
        username = getUserDomain(UserUrl)

        collection = db[username]
        # post = {
        #
        #     "author": "Mike",
        #
        #     "text": "My first blog post!",
        #
        #     "comms": [{"id": 1, "text": "hey"}, {"id": 2, "text": "python"}, {"id": 3, "text": "pymongo"}],
        #
        # }

        comments = []

        for comment in comms:
            try:
                comments.append(
                    {

                        "_id": comment["id"],

                        "date": comment["date"],

                        "text": comment["text"],

                        "likes": comment["likes"]["count"],

                        "replies": comment["thread"]["count"],

                        "status": "added"
                    }
                )

            except KeyError:
                comments.append(
                    {

                        "_id": comment["id"],

                        "date": comment["date"],

                        "text": comment["text"],

                        "likes": comment["likes"]["count"],

                        "replies": -1,

                        "status": "added"
                    }
                )


        post = {

            "_id": vk_post["id"],

            "text": vk_post["text"],

            "date": vk_post["date"],

            "reactions": vk_post["reactions"]["count"],

            "reposts": vk_post["reposts"]["count"],

            "views": vk_post["views"]["count"],

            "from_user": vk_post["from_id"],

            "comments": comments

        }

        posts = []
        posts.append(post)
        # print(await mongoClient.list_database_names())
        # print(await db.list_collection_names())


        res = (await collection.insert_many( posts, ordered = False ))

        print(res)

        # delete_res = await collection.delete_one({'author': 'Mike'})
        # print(delete_res)

        # async for post in collection.find():
        #     pprint.pprint(post)
        #
        print(await collection.count_documents({}))

async def mongoMain2(UserUrl: str, vk_posts: {}, posts_info: {}):
    mongoClient = AsyncMongoClient("localhost", 2017)

    async with mongoClient:
        await mongoClient.aconnect()
        db = mongoClient.sentiment_database
        username = getUserDomain(UserUrl)
        collection = db[username]

        posts_mongo = []

        for vk_post in vk_posts:

            comments = vk_posts[vk_post]
            comments_mongo = []

            for comment in comments:
                try:
                    comments_mongo.append(
                        {

                            "_id": comment["id"],

                            "date": comment["date"],

                            "text": comment["text"],

                            "likes": comment["likes"]["count"],

                            "replies": comment["thread"]["count"],

                            "status": "unprocessed",

                            "last_updated": datetime.now(timezone.utc)

                        }
                    )

                except KeyError:
                    comments_mongo.append(
                        {

                            "_id": comment["id"],

                            "date": comment["date"],

                            "text": comment["text"],

                            # "likes": comment["likes"]["count"],

                            "replies": -1,

                            "status": "unprocessed",

                            "last_updated": datetime.now(timezone.utc)

                        }
                    )

            # print(f"{posts_info[vk_post]["id"]=}, num_of_comms={len(comments_mongo)}")
            try:
                post = {

                    "_id": posts_info[vk_post]["id"],

                    "text": posts_info[vk_post]["text"],

                    "date": posts_info[vk_post]["date"],

                    "reactions": posts_info[vk_post]["reactions"]["count"],

                    "reposts": posts_info[vk_post]["reposts"]["count"],

                    "views": posts_info[vk_post]["views"]["count"],

                    "from_user": posts_info[vk_post]["from_id"],

                    "comments": comments_mongo,

                    "last_updated": datetime.now(timezone.utc)
                }
            except KeyError:
                post = {

                    "_id": posts_info[vk_post]["id"],

                    "text": posts_info[vk_post]["text"],

                    "date": posts_info[vk_post]["date"],

                    "reposts": posts_info[vk_post]["reposts"]["count"],

                    "views": posts_info[vk_post]["views"]["count"],

                    "from_user": posts_info[vk_post]["from_id"],

                    "comments": comments_mongo,

                    "last_updated": datetime.now(timezone.utc)
                }


            posts_mongo.append(post)
            # print(await mongoClient.list_database_names())
            # print(await db.list_collection_names())

        try:
            res1 = (await collection.insert_many( posts_mongo, ordered = False ))
        except pymongo.errors.BulkWriteError as e:
            records_to_update = []
            # print(f"{e.details["writeErrors"]=}")
            # print(f"{type(e)=}")
            for post in e.details["writeErrors"]:
                records_to_update.append(post["keyValue"]["_id"])
            for post in posts_mongo:
                if post["_id"] not in records_to_update:
                    posts_mongo.remove(post)
            operations = [
                ReplaceOne({"_id": post["_id"]}, post)
                for post in posts_mongo
            ]
            try:
                res2 = await collection.bulk_write(operations)
                # print(f"{res2.modified_count=}")
            except e:
                print("Something went wrong!")
                print(e)

    # delete_res = await collection.delete_one({'author': 'Mike'})
    # print(delete_res)

        async for post in collection.find():
            pprint.pprint(post)

        print(await collection.count_documents({}))
# unprocess, in progress, processed
# for comment: last updated

async def mongoDropCollections():
    mongoClient = AsyncMongoClient("localhost", 2017)

    async with mongoClient:
        await mongoClient.aconnect()
        db = mongoClient.sentiment_database

        print(await db.list_collection_names())
        collections = await db.list_collection_names()
        for collection_name in collections:
            await db[collection_name].drop()

        print(await db.list_collection_names())

async def main(UserUrl: str, PostID: int = -1):
    app_storage['session'] = ClientSession()

    async with app_storage["session"]:
        UserID = await getUserId(getUserDomain(UserUrl))
        # print(getUserDomain(UserUrl))
        # print(UserID["object_id"])
        posts = await getPosts(getUserDomain(UserUrl), 5)


        # print(posts[0])
        # posts_sample = posts[:15]
        postLinks = []
        posts_info = {}
        for post in posts:
            link = postLink(post["owner_id"], post["id"])
            postLinks.append(link)
            posts_info[link] = post
        # print(len(postLinks))
        # print(postLinks)
        # print(posts_info)

        res = {}
        async with asyncio.TaskGroup() as tg:
            inner_tasks = [tg.create_task(getComments(post, 0)) for post in postLinks]

        for i in range(len(inner_tasks)):
            res[postLinks[i]] = inner_tasks[i].result()
            # print(postLinks[i], len(inner_tasks[i].result()))


        # for key in res["https://vk.com/wall-30602036_11630731"][0]:
        #     print(f'{key=}')
        #     print(f'{res["https://vk.com/wall-30602036_11630731"][0][key]}', type(res["https://vk.com/wall-30602036_11630731"][0][key]))

        # await mongoMain(UserUrl, posts[0], res["https://vk.com/wall-30602036_11630830"])
        await mongoMain2(UserUrl, res, posts_info)




        #     Работает!!!!!!! Наливай!
        # for key in res:
        #     print(key)
        #     print(len(res[key]))
        #
        # for key in res["https://vk.com/wall-30602036_11630731"][0]:
        #     print(f'{key=}')
        #     print(f'{res["https://vk.com/wall-30602036_11630731"][0][key]}', type(res["https://vk.com/wall-30602036_11630731"][0][key]))



        # for key in posts[0]:
        #     print(f'{key=}')
        #
        #     print(f'{posts[0][key]=}', type(posts[0][key]))
            # print(res[key][0])

        #чо дальше: подключись к mongo с помощью pymongo.AsyncMongoClient;
        #возможная архитектура mongo:
        #вариант 1: каждый пользователь - отдельная датабаза, каждый пост - отдельная таблица (коллекция), каждый комментарий - отдельная строка-документ (коллекция), а его id и текст - просто поля
        #вариант 2: одна датабаза, каждый пользователь - отдельная таблица, каждый пост - отдельная строка (коллекция), каждый комментарий - поле в строке-документе (по сути комментарий - и сам строка-документ,
        #но это поддерживается mongo, так что пофиг)

        # mongoClient = AsyncMongoClient("localhost", 27017)

async def mainLink(UserUrls: [str]):
    for url in UserUrls:
        print("https://vk.com/" + getUserDomain(url))





        # unique = set()
        # for post in posts:
        #     unique.add(post["id"])
        # print(len(unique))

        # print(posts[0]["id"])
        # task = asyncio.create_task(getPostsInv(getUserDomain(UserUrl), 3, 0))
        # await task
        # print(task.result()[0])
        # print(task.result()[1])
        # views = posts["items"][0]["views"]
        # reactions = posts["items"][0]["reactions"]
        # print(views)
        # print(reactions)
        # print(f"{reactions["count"] / views["count"] * 100} % of viewers reacted.")
        # print(f"{reactions["items"][0]["count"] / reactions["count"]  * 100} % of reactors liked.")
        # print(f"{reactions["items"][1]["count"] / reactions["count"]  * 100} % of reactors were angry.")


print(time.strftime('%X'))

asyncio.run(main("https://vk.com/torrent_igruha"))
# asyncio.run(mainComms())
# asyncio.run(mongoDropCollections())
# asyncio.run(mongoMain("https://vk.com/igm?from=groups"))
# asyncio.run(mainLink(("https://vk.com/idmilosbikovicc88", "https://vk.com/torrent_igruha", "https://vk.com/igm?from=groups", "https://vk.com/zyzzfan", "https://vk.com/iddaryadaryada?from=search")))

print(time.strftime('%X'))