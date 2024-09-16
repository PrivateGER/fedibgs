import hashlib
import sys
import uuid
from contextlib import closing

import requests
from mastodon import StreamListener

import database
import tasks

STREAM_BASE = "https://fedi.buzz"
if len(sys.argv) >= 2 and sys.argv[1]:
    STREAM_BASE = sys.argv[1]

# Get endpoint from ARGV
FEDERATED_TIMELINE_STREAM = STREAM_BASE + "/api/v1/streaming/public"

AUTH_HEADER = None
if len(sys.argv) >= 3 and sys.argv[2]:
    AUTH_HEADER = "Bearer " + sys.argv[2]

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
                attachment_response = requests.head(attachment_url, allow_redirects=True)

                # check that it returns a file (200 OK)
                if attachment_response.status_code != 200:
                    print("Attachment URL returned non-200 status code:",
                          attachment_response.status_code, attachment_url)
                    continue

                if attachment_response.url != attachment_url:
                    attachment_url = attachment_response.url
            except Exception as e:
                print("Error while checking attachment URL:", e)
                continue

            attachments.append({
                "description": attachment["description"],
                "url": attachment_url,
            })

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
            }
        }

        queue_buffer.append(object)
        print('\r- Buffered %d/%d posts - ID: %s' % (len(queue_buffer), buffer_size, object["id"]),
              end='', flush=True)

        if len(queue_buffer) >= buffer_size:
            tasks.ingest_batch.delay(queue_buffer)

            queue_buffer.clear()
            print("\r- OK - Buffer flushed -", end='', flush=True)

        return True

    def on_abort(self, status):
        print(status)
        return True


def stream_timeline(endpoint, listener, params={}):
    def connect_func():
        headers = {"User-Agent": "FediBGS/0.0.1"}
        if AUTH_HEADER:
            headers["Authorization"] = AUTH_HEADER

        connection = requests.get(endpoint, headers=headers, data=params, stream=True,
                                  timeout=None)

        if connection.status_code != 200:
            print("Could not connect to server. HTTP status: %i" % connection.status_code)
            return None
        return connection

    # Blocking, never returns (can only leave via exception)
    connection = connect_func()
    with closing(connection) as r:
        listener.handle_stream(r)


listener = BGSListener()

stream_timeline(FEDERATED_TIMELINE_STREAM, listener)
