import pandas as pd
from markdown_it.common.html_re import comment
from matplotlib import pyplot as plt
from pymongo import UpdateOne, MongoClient
import seaborn as sns
from transformers import pipeline

def plot_general_sentiment_pie(data):
    # Создаем фигуру перед построением графиков
    plt.figure(figsize=(12, 6))

    # 1. Подготовка данных для countplot
    plt.subplot(1, 2, 1)
    sns.countplot(data=data, x='sentiment', palette=['#9E9E9E', '#4CAF50', '#F44336'])
    plt.title('Количество комментариев по тональности')

    # 2. Подготовка данных для pie chart
    plt.subplot(1, 2, 2)
    sentiment_counts = data['sentiment'].value_counts()
    plt.pie(
        sentiment_counts,
        labels=sentiment_counts.index,
        autopct=lambda p: f'{p:.1f}%' if p > 0 else '',
        colors=['#9E9E9E', '#4CAF50', '#F44336'],
        startangle=90
    )
    plt.title('Процентное соотношение')

    plt.tight_layout()
    plt.show()

def plot_post_sentiment_pie(post_id, data):
    plt.figure(figsize=(6, 6))
    counts = data.loc[post_id]
    plt.pie(
        counts,
        labels=counts.index,
        autopct='%1.1f%%',
        colors=['red', 'gray', 'green'],
        startangle=90
    )
    plt.title(f'Распределение тональности (Пост {post_id})')
    plt.show()

def getUserDomain(url: str):
    domain = url.split('/')[-1].split('?')[0]
    return domain

def mainAnalyze(UserUrl: str):
    mongoClient = MongoClient("localhost", 2017)

    with mongoClient:

        db = mongoClient.sentiment_database
        username = getUserDomain(UserUrl)

        collection = db[username]

        pipeline = [
            {"$unwind": "$processed_comments"},
            {"$group": {
                "_id": "$_id",
                "comments": {"$push": "$processed_comments"},
                "post_date": {"$first": "$date"},
                "reactions": {"$first": "$reactions"},
                "reposts": {"$first": "$reposts"},
                "views": {"$first": "$views"}
            }}
        ]
        posts = collection.aggregate(pipeline)

        data = []
        posts_id = set()
        for post in posts:
            for comment in post["comments"]:
                row = {
                    "post_id": post["_id"],
                    "post_date": post["post_date"],
                    'comment_text': comment['text'],
                    'sentiment': comment['sentiment']
                }
                data.append(row)
                posts_id.add(post["_id"])
        df = pd.DataFrame(data)

        sentiment_dist = df.groupby(['post_id', 'sentiment']).size().unstack(fill_value=0)
        print(sentiment_dist)

        # print(sentiment_dist.loc['post_id'])

        for id in posts_id:
            plot_post_sentiment_pie(id, sentiment_dist)

        plot_general_sentiment_pie(df)



        # sentiments_count = df.groupby("post_id").agg({
        #     "sentiment": "count"
        # }).rename(columns={"sentiment": "comments_count"})
        # print(sentiments_count)



        # print(data)
        # sns.countplot(x='sentiment', data=comments_sentiment, palette=['green', 'red', 'gray'])
        # plt.title('Соотношение тональностей')

mainAnalyze("https://vk.com/torrent_igruha")