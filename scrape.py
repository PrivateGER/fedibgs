import hashlib
import os
import signal
import sys
import threading
import time
import uuid
from asyncio import timeout
from contextlib import closing
from datetime import datetime

import requests
from mastodon import StreamListener
from prometheus_client import Counter, Histogram
from fastapi import FastAPI
from prometheus_client import make_asgi_app
import uvicorn
import logging

logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S')

scraped_posts = Counter('scraped_posts', 'Number of posts scraped from the endpoint during runtime')
scraped_attachments = Counter('scraped_attachments', 'Number of attachments scraped from the endpoint during runtime')
scraped_posts_heatmap = Histogram('post_time_by_hour', 'Post time by hour', buckets=(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                                                                   11, 12, 13, 14, 15, 16, 17, 18, 19,
                                                                                   20, 21, 22, 23))

import database
import tasks

STREAM_BASE = "https://fedi.buzz"
if len(sys.argv) >= 2 and sys.argv[1]:
    STREAM_BASE = sys.argv[1]

# Get endpoint from ARGV
FEDERATED_TIMELINE_STREAM = STREAM_BASE + "/api/v1/streaming/public"

AUTH_HEADER = None
if len(sys.argv) >= 3 and sys.argv[2]:
    logging.info("Using provided auth header")
    AUTH_HEADER = "Bearer " + sys.argv[2].strip()

queue_buffer = []
buffer_size = 32
class BGSListener(StreamListener):
    def on_update(self, status):
        # Verify that the id is set, that content is not empty, and that the content is not a boost
        if not hasattr(status, "id") or not hasattr(status, "content") or (hasattr(status, "reblog") and status["reblog"]):
            return True

        # Do not include replies, we only want top-level posts
        if status["in_reply_to_id"]:
            return True

        # Verify that we have either an attachment or some content
        if not status["content"] and not status["media_attachments"]:
            return True

        id_hash = hashlib.md5()
        id_hash.update(str(status["url"]).encode("utf-8"))
        post_uuid = uuid.UUID(id_hash.hexdigest())

        attachments = []
        for attachment in status["media_attachments"]:
            if attachment["type"] != "image":
                continue

            # priority order: remote_url -> preview_url -> url
            attachment_url = attachment["remote_url"] if attachment["remote_url"] else attachment["url"]

            # HEAD request to check whether a redirect is present
            try:
                attachment_response = requests.head(attachment_url, allow_redirects=True, timeout=2, headers={"User-Agent": "FediBGS/0.0.1"})

                # check that it returns a file (200 OK)
                if attachment_response.status_code != 200:
                    logging.warning("Attachment URL returned non-200 status code: %s - %s" % (attachment_response.status_code, attachment_url))
                    continue

                if attachment_response.url != attachment_url:
                    attachment_url = attachment_response.url
            except Exception as e:
                logging.error("Error while checking attachment URL: %s" % e)
                continue

            attachments.append({
                "description": attachment["description"],
                "url": attachment_url,
            })
            scraped_attachments.inc()

        tags = []
        for tag in status["tags"]:
            tags.append(tag["name"])

        object = {
            "id": str(post_uuid),
            "content": status["content"],
            "attachments": attachments,
            "postURL": status["url"],
            "tags": tags,
            "author": {
                "url": status["account"]["url"],
                "username": status["account"]["username"],
            },
            "indexedAt": datetime.now(),
        }

        queue_buffer.append(object)
        scraped_posts.inc()
        scraped_posts_heatmap.observe(time.localtime().tm_hour)
        #print('\r- Buffered %d/%d posts - ID: %s' % (len(queue_buffer), buffer_size, object["id"]), end='', flush=True)

        if len(queue_buffer) >= buffer_size:
            tasks.ingest_batch.delay(queue_buffer)

            queue_buffer.clear()
            #print("\r- OK - Buffer flushed -", end='', flush=True)
            logging.info("Buffer flushed - %d posts" % buffer_size)

        return True

    def on_abort(self, status):
        logging.error("Stream connection aborted: %s" % status)
        os.kill(os.getpid(), signal.SIGINT)

    def on_error(self, status_code):
        logging.error("Stream connection error: %s" % status_code)
        os.kill(os.getpid(), signal.SIGINT)


def stream_timeline(endpoint, listener, params={}):
    def connect_func():
        headers = {"User-Agent": "FediBGS/0.0.1", "Accept": "text/event-stream"}
        if AUTH_HEADER:
            headers["Authorization"] = AUTH_HEADER

            # Only show first and last bits of the token
            redacted_token = AUTH_HEADER[:10] + "..." + AUTH_HEADER[-10:]
            logging.info("Connecting to %s with token %s" % (endpoint, redacted_token))

        connection = requests.get(endpoint, headers=headers, data=params, stream=True,
                                  timeout=None, allow_redirects=True)

        if connection.status_code != 200:
            logging.error("Could not connect to server. HTTP status: %i" % connection.status_code)
            logging.error(connection.text)
            return None
        return connection

    # Blocking, never returns (can only leave via exception)
    connection = connect_func()
    with closing(connection) as r:
        listener.handle_stream(r)

app = FastAPI(debug=False)
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

def start_metrics_server():
    # Get METRICS_PORT from environment, default to 9999
    port = os.getenv("METRICS_PORT", 9999)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="error")

# Kill program if no posts are scraped for 30 seconds
def post_watchdog():
    logging.info("Starting watchdog...")
    watchdog_counter = 0
    old_post_size = 0
    while True:
        if watchdog_counter >= 30:
            logging.error("No posts scraped for 30 seconds, exiting...")
            os.kill(os.getpid(), signal.SIGINT)
        if len(queue_buffer) != old_post_size:
            watchdog_counter = 0
            old_post_size = len(queue_buffer)
        else:
            watchdog_counter += 1
            if watchdog_counter % 5 == 0:
                logging.warning("No posts scraped for %d seconds" % watchdog_counter)
        time.sleep(1)


if __name__ == "__main__":
    # Start the metrics server in a separate thread
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()

    # Start the watchdog in a separate thread
    watchdog_thread = threading.Thread(target=post_watchdog, daemon=True)
    watchdog_thread.start()

    # Start the main application
    listener = BGSListener()
    stream_timeline(FEDERATED_TIMELINE_STREAM, listener)
