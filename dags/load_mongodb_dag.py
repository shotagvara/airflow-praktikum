from airflow.sdk import Asset, dag, task

CLEANED_REVIEWS_ASSET = Asset("reviews_cleaned_content_csv")


@dag(
    dag_id="load_mongodb_dag",
    schedule=[CLEANED_REVIEWS_ASSET],
    catchup=False,
)
def load_mongodb_dag():

    @task
    def load_to_mongodb():
        import os
        from pathlib import Path

        import pandas as pd
        from airflow.providers.mongo.hooks.mongo import MongoHook

        csv_path = (
            Path(os.environ["REVIEWS_PROJECT_DIR"])
            / "data"
            / "output"
            / "reviews_cleaned_content.csv"
        )

        total_inserted = 0

        with MongoHook(mongo_conn_id="reviews_mongodb") as hook:
            client = hook.get_conn()

            db = client["tiktok_reviews"]
            collection = db["reviews"]

            # Full refresh: remove records from previous runs
            collection.delete_many({})

            for chast in pd.read_csv(
                csv_path,
                chunksize=2000,
                keep_default_na=False,
            ):
                chast["at"] = pd.to_datetime(
                    chast["at"],
                    errors="coerce",
                    utc=True,
                )

                records = chast.to_dict(orient="records")

                for record in records:
                    date = record["at"]
                    record["at"] = (
                        date.to_pydatetime()
                        if pd.notna(date)
                        else None
                    )

                collection.insert_many(records)

                total_inserted += len(records)

            print(f"Inserted {total_inserted} reviews into MongoDB")

    load_to_mongodb()


load_mongodb_dag()