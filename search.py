import sys

from clip_client import Client

import database

query = ""

for i in range(1, len(sys.argv)):
    query += sys.argv[i] + " "

query = query.strip()

db_connection = database.get_db_connection()

cursor = db_connection.cursor()

print("Encoding \"" + query + "\" into embedding...")
query_embedding = database.embed_or_cache(query)

print("Querying database for posts...")

cursor.execute("SELECT id, content, %s::real[] <=> posts.text_embedding AS score, post_url FROM posts ORDER BY text_embedding <=> %s::real[] LIMIT 5;",
               (query_embedding, query_embedding,))
results = cursor.fetchall()

for result in results:
    id = result[0]
    content = result[1]
    score = result[2]
    url = result[3]

    print("Post ID:", id)
    print("Content:", content)
    print("Score:", score)
    print("URL:", url)
    print("--------------")


print("Querying database for attachments...")
cursor.execute("SELECT attachments.id, attachments.url, %s::real[] <=> attachments.vector AS score, posts.id FROM posts JOIN attachments ON posts.id = attachments.post_id WHERE vector IS NOT NULL ORDER BY attachments.vector <=> %s::real[] LIMIT 5;",
               (query_embedding, query_embedding))
results = cursor.fetchall()

for result in results:
    id = result[0]
    url = result[1]
    score = result[2]
    post_id = result[3]

    print("Attachment ID:", id)
    print("Post ID:", post_id)
    print("URL:", url)
    print("Score:", score)
    print("--------------")
