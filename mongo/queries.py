from getpass import getpass
from pymongo import MongoClient

def top_5_comments(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$content",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$limit": 5
        }
    ]

    return list(collection.aggregate(pipeline))

def short_comments(collection):
    pipeline = [
        {
            "$match": {
                "$expr": {
                    "$lt": [
                        {"$strLenCP": "$content"},
                        5
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "content": 1,
                "score": 1,
                "at": 1
            }
        }
    ]

    return collection.aggregate(pipeline)

def average_rating_per_day(collection):
    pipeline = [
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$at",
                        "unit": "day",
                        "timezone": "UTC"
                    }
                },
                "average_rating": {
                    "$avg": "$score"
                },
                "reviews_count": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "average_rating": {
                    "$round": ["$average_rating", 2]
                },
                "reviews_count": 1
            }
        },
        {
            "$sort": {
                "date": 1
            }
        }
    ]

    return collection.aggregate(pipeline)


def main():
    password = getpass("MongoDB password: ")

    with MongoClient(
        host="127.0.0.1",
        port=27017,
        username="admin",
        password=password,
        authSource="admin",
    ) as client:

        client.admin.command("ping")

        db = client["tiktok_reviews"]
        collection = db["reviews"]

        print("Successfully connected to MongoDB!")
        results = top_5_comments(collection)

        for result in results:
            print(result)

        print("\nComments shorter than 5 characters:")

        results = short_comments(collection)

        total = 0

        for review in results:
            if total < 10:
                print(review)

            total += 1

        print(f"Total short comments: {total}")

        print("\nAverage rating per day:")

        results = average_rating_per_day(collection)

        total_days = 0

        for review in results:
            if total_days < 10:
                print(review)

            total_days += 1

        print(f"Total days: {total_days}")


if __name__ == "__main__":
    main()