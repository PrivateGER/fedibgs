import asyncio
import time

from fastapi import FastAPI, Request, WebSocket
from starlette.exceptions import HTTPException
from starlette.staticfiles import StaticFiles

import database
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

origins = [
    "http://localhost:3000",
]

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cachedStats = {
    "posts": 0,
    "attachments": 0,
    "last_updated": 0
}

# Posts within last 7 days
@app.get("/api/stats")
async def counts():
    # Cache the stats for 5 minutes
    if cachedStats["last_updated"] + 300 < time.time():
        cachedStats["last_updated"] = time.time()
        with database.get_db_connection() as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM posts WHERE indexed_at > now() - interval '7 days'")
            post_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM attachments WHERE indexed_at > now() - interval '7 days'")
            attachment_count = cursor.fetchone()[0]

            cachedStats["posts"] = post_count
            cachedStats["attachments"] = attachment_count

    return {"posts": cachedStats["posts"], "attachments": cachedStats["attachments"]}

@app.get("/api/author/search")
async def search_authors(q: str):
    q = q.strip()
    q = q.replace("%", "") # lmao
    # Add wildcards to query
    q = f"%{q}%"

    with database.get_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT id, username, url FROM authors WHERE username ILIKE %s OR url ILIKE %s LIMIT 50", (q, q))
        authors = cursor.fetchall()
        return [{"id": author[0], "username": author[1], "url": author[2]} for author in authors]

@app.get("/api/author/{author_id}")
async def author(author_id: int, offset: int = 0):
    with database.get_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT username, url FROM authors WHERE id = %s", (author_id,))
        author = cursor.fetchone()
        if not author:
            return {"error": "Author not found"}

        cursor.execute("SELECT COUNT(*) FROM posts WHERE author_id = %s", (author_id,))
        total_posts = cursor.fetchone()[0]

        cursor.execute("SELECT \"posts\".id AS id, content, post_url, date_part('epoch', indexed_at) AS indexed_at FROM posts WHERE author_id = %s ORDER BY posts.indexed_at DESC LIMIT 50 OFFSET %s", (author_id, offset))
        posts = cursor.fetchall()

        postIDs = []
        for post in posts:
            postIDs.append(post[0])

        cursor.execute("SELECT post_id, description, url FROM attachments WHERE post_id = ANY(%s)", (postIDs,))
        attachments = cursor.fetchall()

        formatted_posts = []
        for post in posts:
            attachments_for_post = []
            for attachment in attachments:
                if attachment[0] == post[0]:
                    attachments_for_post.append({
                        "id": attachment[0],
                        "description": attachment[1],
                        "url": attachment[2]
                    })

            formatted_posts.append({
                "id": post[0],
                "content": post[1],
                "post_url": post[2],
                "indexed_at": int(post[3])*1000,
                "attachments": attachments_for_post,
                "author_id": author_id,
                "username": author[0]
            })
        print(formatted_posts)

        return {"id": author_id, "username": author[0], "url": author[1], "posts": formatted_posts, "total_posts": total_posts}

@app.get("/api/search")
async def search(q: str, offset: int = 0):
    with database.get_db_connection() as connection:
        cursor = connection.cursor()

        # Get potential result count
        cursor.execute("SELECT COUNT(*) FROM posts WHERE posts.content_ts @@ websearch_to_tsquery('english', %s::text)", (q,))
        total_result_count = cursor.fetchone()[0]
        if total_result_count == 0:
            return {"posts": []}

        cursor.execute(
            "SELECT \"posts\".id AS id, content, a.username, post_url, date_part('epoch', indexed_at) AS indexed_at, a.id FROM (SELECT * FROM posts WHERE posts.content_ts @@ websearch_to_tsquery('english', %s::text)) AS posts "
            "JOIN authors a on posts.author_id = a.id "
            "ORDER BY indexed_at DESC LIMIT 50 OFFSET %s",
            (q, offset, ))
        posts = cursor.fetchall()

        # Get attachments of the posts
        postIDs = []
        for post in posts:
            postIDs.append(post[0])

        if not postIDs:
            return {"posts": []}

        cursor.execute("SELECT post_id, description, url FROM attachments WHERE post_id = ANY(%s)", (postIDs,))

        attachments = cursor.fetchall()

        formatted_posts = []
        for post in posts:
            attachments_for_post = []
            for attachment in attachments:
                if attachment[0] == post[0]:
                    attachments_for_post.append({
                        "id": attachment[0],
                        "description": attachment[1],
                        "url": attachment[2]
                    })

            formatted_posts.append({
                "id": post[0],
                "content": post[1],
                "username": post[2],
                "post_url": post[3],
                "indexed_at": int(post[4])*1000,
                "attachments": attachments_for_post,
                "author_id": post[5]
            })

        return {"posts": formatted_posts, "total_result_count": total_result_count}


@app.websocket("/stream")
async def stream_posts(websocket: WebSocket):
    await websocket.accept()
    last_unix = int(time.time())
    try:
        while True:
            with database.get_db_connection() as connection:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT posts.id AS id, content, a.username, post_url, date_part('epoch', indexed_at), author_id AS indexed_at FROM posts "
                    "JOIN authors a on posts.author_id = a.id WHERE indexed_at > to_timestamp(%s) "
                    "ORDER BY indexed_at DESC LIMIT 50",
                    (last_unix,))

                posts = cursor.fetchall()
                postIDs = []
                for post in posts:
                    postIDs.append(post[0])

                cursor.execute("SELECT post_id, description, url FROM attachments WHERE post_id = ANY(%s)", (postIDs,))
                attachments = cursor.fetchall()

                formatted_posts = []
                for post in posts:
                    attachments_for_post = []
                    for attachment in attachments:
                        if attachment[0] == post[0]:
                            attachments_for_post.append({
                                "id": str(attachment[0]),
                                "description": attachment[1],
                                "url": attachment[2]
                            })

                    formatted_posts.append({
                        "id": str(post[0]),
                        "content": post[1],
                        "username": post[2],
                        "post_url": post[3],
                        "indexed_at": int(post[4])*1000,
                        "attachments": attachments_for_post,
                        "author_id": str(post[5])
                    })

                await websocket.send_json({"posts": formatted_posts})

            last_unix = int(time.time())
            # Sleep 500ms to avoid hammering the database
            await asyncio.sleep(1)
    except Exception as e:
        print("Stream connection closed")
        print("Error in stream_posts:", e)
        raise e


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except (HTTPException, StarletteHTTPException) as ex:
            if ex.status_code == 404:
                return await super().get_response("index.html", scope)
            else:
                raise ex



app.mount('/', SPAStaticFiles(directory='fedibgs-fe/dist/', html=True))
