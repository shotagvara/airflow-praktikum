from airflow.sdk import Asset, dag, task, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
import os
import pandas as pd
import re

PROJECT_DIR = os.environ["REVIEWS_PROJECT_DIR"]
INPUT_FILENAME = "tiktok_google_play_reviews.csv"

FILE_PATH = os.path.join(
    PROJECT_DIR,
    "data",
    "input",
    INPUT_FILENAME,
)

OUTPUT_DIR = os.path.join(
    PROJECT_DIR,
    "data",
    "output",
)

CLEANED_REVIEWS_ASSET = Asset("reviews_cleaned_content_csv")

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "\U0001FA00-\U0001FAFF"  # extended symbols
    "\U00002600-\U000026FF"  # miscellaneous symbols
    "\U00002700-\U000027BF"  # dingbats
    "\U0001F1E6-\U0001F1FF"  # flags
    "\u200D"         # Zero-width joiner
    "\u20E3"         # Combining enclosing keycap
    "\uFE0E-\uFE0F"  # Variation selectors
    "]+",
    flags=re.UNICODE,
)


def remove_emojis(text):
    return EMOJI_PATTERN.sub("", text)

def get_output_path(filename):
    return os.path.join(OUTPUT_DIR, filename)

@dag(
    dag_id = "process_reviews",
    schedule  = None,
    catchup = False,
)
def process_reviews_dag():

    wait_for_file = FileSensor(
        task_id = "wait_for_file",
        filepath=INPUT_FILENAME,
        fs_conn_id = "reviews_filesystem",
        poke_interval = 10,
        timeout = 300,
    )

    @task.branch
    def check_file():
        if os.path.getsize(FILE_PATH)  == 0:
            return "log_empty_file"
        else:
            return "process_data.replace_nulls"

    @task.bash
    def log_empty_file():
        return 'echo "The input file is empty"'



    @task_group
    def process_data(input_path):
        @task
        def replace_nulls(input_path):

            data = pd.read_csv(input_path)
            data.fillna("-", inplace=True)

            output_path = get_output_path("reviews_nulls_replaced.csv")

            data.to_csv(output_path, index=False)

            return output_path

        @task
        def sort_by_date(input_path):

            data = pd.read_csv(input_path)
            data.sort_values("at", inplace=True)

            output_path = get_output_path("reviews_sorted_by_date.csv")

            data.to_csv(output_path, index=False)
            return output_path

        @task(outlets=[CLEANED_REVIEWS_ASSET])
        def clean_content(input_path):

            data = pd.read_csv(input_path)

            data["content"] = data["content"].apply(remove_emojis)

            output_path = get_output_path("reviews_cleaned_content.csv")
            data.to_csv(output_path, index=False)

            return output_path

        replaced_nulls_data_path = replace_nulls(input_path)
        sorted_data_by_date_path = sort_by_date(replaced_nulls_data_path)
        cleaned_content_data_path = clean_content(sorted_data_by_date_path)



    file_branch = check_file()
    empty_task = log_empty_file()
    processing_group = process_data(FILE_PATH)

    wait_for_file >> file_branch

    file_branch >> [empty_task, processing_group]


process_reviews_dag()
