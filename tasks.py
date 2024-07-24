import json
import time
import uuid

from clip_client import Client
from io import StringIO
from html.parser import HTMLParser
from docarray import DocumentArray, Document
import database

from celery import Celery
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
        "content": defang_urls(strip_tags(dataset["content"])),
        "attachments": [strip_query_params(url) for url in [
            attachment["url"]
            for attachment in dataset["attachments"]
            if attachment["url"] and attachment["url"].startswith("http")
               and attachment["url"].endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))
        ]
                        ]
    }
    return preprocessed


c = Client('http://localhost:51000')

db_connection = database.get_db_connection()


class ElementDocument(Document):
    text: str = None
    uri: str = None



@app.task
def ingest_batch(datasets):
    # Preprocess the content and attachments
    clip_documents = DocumentArray()
    dataset_info = []

    cursor = db_connection.cursor()

    logger.info(f"Processing {len(datasets)} posts")

    for dataset in datasets:
        # Verify that the post is not already in the database
        cursor.execute("SELECT id FROM posts WHERE id = %s", (dataset["id"],))
        if cursor.fetchone():
            logger.info(f"Post {dataset['id']} already exists in the database, skipping...")
            continue

        preprocessed = preprocess_dataset(dataset)
        doc = ElementDocument(id=str(uuid.uuid4()),
                              text=preprocessed["content"] if len(preprocessed["content"]) > 0 else "EMPTY")
        clip_documents.append(doc)
        dataset_info.append({"dataset": dataset, "content_index": len(clip_documents) - 1, "attachments_indices": []})
        for attachment in preprocessed["attachments"]:
            attachment_doc = Document(ElementDocument(id=str(uuid.uuid4()), uri=attachment))
            clip_documents.append(attachment_doc)
            dataset_info[-1]["attachments_indices"].append(len(clip_documents) - 1)

    # Generate embeddings for the batch
    try:
        logger.info("Generating embeddings...")
        # print entire clip_documents (docarray)
        clip_documents.summary()

        start_time = time.time()
        r = c.encode(clip_documents, batch_size=min(128, len(clip_documents)))

        r.summary()

        logger.info(f"Embedding generation took {time.time() - start_time} seconds")
    except Exception as e:
        logger.error(f"Error during embedding generation: {e}")
        raise e

    for info in dataset_info:
        dataset = info["dataset"]

        # Add the embedding for the main content
        content_embedding = r[info["content_index"]].embedding if r[info[
            "content_index"]].text != "EMPTY" else None

        dataset["text_embedding"] = content_embedding.tolist() if content_embedding is not None else None

        # Add embeddings for the attachments
        for i, attachment_index in enumerate(info["attachments_indices"]):
            attachment_embedding = r[attachment_index].embedding
            dataset["attachments"][i][
                "embedding"] = attachment_embedding.tolist() if attachment_embedding is not None else None
        print(dataset)

        # Insert the author if they don't exist
        cursor.execute("INSERT INTO authors (url, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                       (dataset["author"]["url"], dataset["author"]["username"]))

        # Get the author's ID
        cursor.execute("SELECT id FROM authors WHERE url = %s", (dataset["author"]["url"],))
        author_id = cursor.fetchone()[0]

        # Insert the post
        cursor.execute("INSERT INTO posts (id, content, text_embedding, post_url, tags, author_id) "
                       "VALUES (%s, %s, %s::real[], %s, %s, %s)", (dataset["id"], dataset["content"],
                                                                   dataset["text_embedding"], dataset["postURL"],
                                                                   json.dumps(dataset["tags"]), author_id))

        for attachment in dataset["attachments"]:
            cursor.execute("INSERT INTO attachments (url, description, vector, post_id) "
                           "VALUES (%s, %s, %s::real[], %s)", (attachment["url"], attachment["description"],
                                                               attachment["embedding"],
                                                               dataset["id"]))

    return True


def ingest_data(dataset):
    # Generate embeddings for the dataset and images

    #attachment_urls = [attachment["url"] for attachment in dataset["attachments"]]
    clip_dataset = [
        defang_urls(strip_tags(dataset["content"]))
    ]
    #clip_dataset.extend(attachment_urls)
    print(clip_dataset)

    r = c.encode(clip_dataset)

    # Convert to lists from numpy arrays
    r = [embed.tolist() for embed in r]  # Convert to lists from numpy arrays

    # Add the embeddings to the dataset
    dataset["text_embedding"] = r[0]
    if len(r) > 1:
        for i, embedding in enumerate(r[1:]):
            dataset["attachments"][i]["embedding"] = embedding

    # Send the dataset to the database
    cursor = db_connection.cursor()

    # Insert the author if they don't exist
    cursor.execute("INSERT INTO authors (url, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                   (dataset["author"]["url"], dataset["author"]["username"]))
    # Get the author's ID
    cursor.execute("SELECT id FROM authors WHERE url = %s", (dataset["author"]["url"],))
    author_id = cursor.fetchone()[0]

    cursor.execute("INSERT INTO posts (id, content, text_embedding, post_url, tags, author_id) "
                   "VALUES (%s, %s, %s, %s, %s, %s)", (dataset["id"], dataset["content"],
                                                       dataset["text_embedding"], dataset["postURL"],
                                                       json.dumps(dataset["tags"]), author_id))

    for attachment in dataset["attachments"]:
        cursor.execute("INSERT INTO attachments (url, description, vector, post_id) "
                       "VALUES (%s, %s, %s, %s)", (attachment["url"], attachment["description"],
                                                   attachment["embedding"] if "embedding" in attachment else None,
                                                   dataset["id"]))

    db_connection.commit()
    cursor.close()

    return True
