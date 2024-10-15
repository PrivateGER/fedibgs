import json
import time
import uuid

from io import StringIO
from html.parser import HTMLParser
from psycopg import ProgrammingError
from celery import signals
import database

from celery import Celery, Task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

app = Celery('tasks', broker=database.get_redis_url())


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def defang_urls(string):
    return string.replace("http://", "").replace("https://", "").replace("data:", "data.")


def strip_query_params(url):
    return url.split("?")[0]


def preprocess_dataset(dataset):
    preprocessed = {
        "content": strip_tags(dataset["content"]),
        "attachments": [strip_query_params(url) for url in [
            attachment["url"]
            for attachment in dataset["attachments"]
            if attachment["url"] and attachment["url"].startswith("http")
               and attachment["url"].endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))
        ]
                        ]
    }
    return preprocessed

author_id_map = {}

@signals.task_retry.connect
@signals.task_failure.connect
@signals.task_revoked.connect
def on_task_failure(**kwargs):
    """Abort transaction on task errors.
    """
    # celery exceptions will not be published to `sys.excepthook`. therefore we have to create another handler here.
    from traceback import format_tb

    logger.error('[task:%s:%s]' % (kwargs.get('task_id'), kwargs['sender'].request.correlation_id,)
                 + '\n'
                 + ''.join(format_tb(kwargs.get('traceback', [])))
                 + '\n'
                 + str(kwargs.get('exception', '')))


@app.task(autoretry_for=(Exception,))
def ingest_batch(datasets):
    with database.get_db_connection() as connection:
        cursor = connection.cursor()

        for dataset in datasets:
            # Check if the post already exists
            cursor.execute("SELECT exists ( SELECT 1 FROM posts WHERE id = %s )", (dataset["id"],))
            exists = cursor.fetchone()[0]
            connection.commit()
            if exists:
                print(f"Post {dataset['id']} already exists")
                continue

            if dataset["author"]["url"] not in author_id_map:
                # Get the author's ID
                try:
                    cursor.execute("SELECT id FROM authors WHERE url = %s", (dataset["author"]["url"],))

                    # Check if the author exists
                    author_id = cursor.fetchone()
                    if author_id:
                        author_id_map[dataset["author"]["url"]] = author_id[0]
                        author_id = author_id[0]
                    else:
                        # Insert the author if they don't exist
                        cursor.execute(
                            "INSERT INTO authors (url, username) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING id",
                            (dataset["author"]["url"], dataset["author"]["username"]))
                        connection.commit()
                        author_id = cursor.fetchone()[0]
                        logger.info(f"Inserted author {dataset['author']['url']} with ID {author_id}")
                        author_id_map[dataset["author"]["url"]] = author_id
                except ProgrammingError as e:
                    connection.rollback()
                    raise "Failed to insert author" from e
            else:
                author_id = author_id_map[dataset["author"]["url"]]

            if not author_id:
                raise Exception(f"Failed to get author ID for {dataset['author']['url']}")

            try:
                # Insert the post
                cursor.execute("INSERT INTO posts (id, content, post_url, tags, author_id) "
                               "VALUES (%s, %s, %s, %s, %s)", (dataset["id"], strip_tags(dataset["content"]),
                                                               dataset["postURL"],
                                                               json.dumps(dataset["tags"]), author_id))
                logger.info(f"Inserted post {dataset['id']}")

                for attachment in dataset["attachments"]:
                    cursor.execute("INSERT INTO attachments (url, description, post_id) "
                                   "VALUES (%s, %s, %s)", (attachment["url"], attachment["description"], dataset["id"]))
                    logger.info(f"Inserted attachment {attachment['url']}")

                connection.commit()
            except:
                connection.rollback()
                raise Exception("Failed to insert post and attachments")
    return True


@app.task(autoretry_for=(Exception,))
def sync_posts_not_in_meilisearch():
    return

