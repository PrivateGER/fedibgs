from fastapi import FastAPI
from clip_client import Client
import database

app = FastAPI()

c = Client('http://localhost:51000')


@app.get("/")
async def root():
    return {"message": "Hello World"}

class SearchType(str):
    semantic = "semantic"
    full_text = "full_text"
    combined = "combined"

@app.get("/search")
async def search(query: str, mode: SearchType = SearchType.combined, cache: bool = True):
    cursors = database.get_cursor()

    query_embedding = database.embed_or_cache(query, cache)

    if mode == SearchType.combined:
        cursors.execute("""
        -- The query starts with a Common Table Expression (CTE), named "semantic_search".
        WITH semantic_search AS (
        SELECT id, content, text_embedding, 
        RANK () OVER (ORDER BY text_embedding <=> %s::real[]) AS rank
        FROM posts
        ORDER BY text_embedding <=> %s::real[]
        ), 
        -- Another CTE, named "full_text_search", is defined.
        full_text_search AS (
        SELECT id, content, text_embedding, 
        RANK () OVER (ORDER BY 
            ts_rank(to_tsvector('english', content), 
                plainto_tsquery(%s)) DESC) AS rank
        FROM posts
        WHERE to_tsvector('english', content) @@ plainto_tsquery(%s)
        ORDER BY ts_rank(
            to_tsvector('english', content), plainto_tsquery(%s)) DESC 
        )
        
        -- The main query selects columns from both CTEs.
        SELECT 
            COALESCE(semantic_search.id, full_text_search.id) AS id,
            COALESCE(semantic_search.content, full_text_search.content) AS content,
            COALESCE(1.0 / (1 + semantic_search.rank), 0.0) + 
            COALESCE(1.0 / (1 + full_text_search.rank), 0.0) AS rank
        FROM semantic_search FULL OUTER JOIN full_text_search USING (id)
        ORDER BY rank DESC LIMIT 25;
        """, (query_embedding, query_embedding, query, query, query))
    elif mode == SearchType.full_text:
        cursors.execute("""
        SELECT id, content, 
        ts_rank(to_tsvector('english', content), plainto_tsquery(%s)) AS rank
        FROM posts
        WHERE to_tsvector('english', content) @@ plainto_tsquery(%s)
        ORDER BY rank DESC LIMIT 25;
        """, (query, query))
    elif mode == SearchType.semantic:
        print("Semantic search for query:", query)
        cursors.execute("""
        SELECT id, content,
        text_embedding <=> %s::real[] AS distance
        FROM posts
        ORDER BY text_embedding <=> %s::real[] LIMIT 25;
        """, (query_embedding, query_embedding))
    else:
        return {"error": "Invalid search type"}

    results = cursors.fetchall()
    cursors.close()

    cleaned_results = []

    for result in results:
        id = result[0]
        content = result[1]
        score = result[2]
        cleaned_results.append({"id": id, "content": content, "score": score})

    return cleaned_results
