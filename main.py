import asyncio
import time

from fastapi import FastAPI, Request, WebSocket
import database
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

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

templates = Jinja2Templates(directory="templates")


@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse(request=request, name="index.jinja")

# Posts within last 7 days
@app.get("/api/stats")
async def counts():
    with database.get_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM posts WHERE indexed_at > now() - interval '7 days'")
        post_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM attachments WHERE indexed_at > now() - interval '7 days'")
        attachment_count = cursor.fetchone()[0]
        return {"posts": post_count, "attachments": attachment_count}

@app.get("/api/search")
async def search(q: str, after: int = 0):
    with database.get_db_connection() as connection:
        cursor = connection.cursor()

        cursor.execute(
            "SELECT \"posts\".id AS id, content, a.username, post_url FROM posts JOIN public.authors a on posts.author_id = a.id "
            "WHERE indexed_at > to_timestamp(%s) AND posts.content_ts @@ websearch_to_tsquery('english', %s::text) ORDER BY indexed_at DESC LIMIT 50",
            (after, q,))

        posts = cursor.fetchall()

        # Get attachments of the posts
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
                "username": post[2],
                "post_url": post[3],
                "attachments": attachments_for_post
            })

        return {"posts": formatted_posts}


@app.websocket("/stream")
async def stream_posts(websocket: WebSocket, query: str = ""):
    await websocket.accept()
    last_unix = int(time.time())
    try:
        while True:
            print("Sending posts after", last_unix)
            # If empty query, just get the latest posts
            if not query:
                with database.get_db_connection() as connection:
                    cursor = connection.cursor()
                    cursor.execute(
                        "SELECT content, a.username, post_url, date_part('epoch', indexed_at) AS indexed_at FROM posts JOIN public.authors a on "
                        "posts.author_id = a.id WHERE indexed_at > to_timestamp(%s)"
                        "ORDER BY indexed_at DESC LIMIT 50", (last_unix - 1,))
                    posts = cursor.fetchall()
                    cursor.close()
                    await websocket.send_json({"posts": posts})
            else:
                with database.get_db_connection() as connection:
                    cursor = connection.cursor()
                    cursor.execute("SELECT content, a.username, post_url FROM posts JOIN public.authors a on "
                                   "posts.author_id = a.id WHERE indexed_at > to_timestamp(%s)"
                                   "AND posts.content_ts @@ websearch_to_tsquery('english', %s::text) ORDER BY indexed_at DESC LIMIT 50",
                                   (last_unix - 1, query,))
                    posts = cursor.fetchall()
                    cursor.close()
                    await websocket.send_json({"posts": posts})

            last_unix = int(time.time())
            # Sleep 500ms to avoid hammering the database
            await asyncio.sleep(1)
    except Exception as e:
        print("Error in stream_posts:", e)
        print("Stream connection closed")
