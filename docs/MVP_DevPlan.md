
### Phase 1 — Data + Storage (MVP baseline)
1. **Postgres schema**
  - (done) `posts` table : `uri, text, created_at, tags, langs, embed`
  - Add a `post_embeddings` table:
    - `uri` (PK, FK to posts)
    - `embedding` (vector)
    - `updated_at`
2. **Ingestion → Postgres**
  - Ingest posts into `posts`.
    - ToDo: Main program 
3. **Batch indexers**
  - Periodically pull recent posts (e.g., last 2–3 days) and compute embeddings.
  - Upsert into `post_embeddings`.

### Phase 2 — Retrieval + Ranking
4. **Candidate retrieval**
  - Use PGVector ANN search on `embedding`, filter by:
    - time window (last N days)
    - language(s)
    - tags (optional)
    - embed type (optional)
5. **Rerank**
  - LLM reranker or heuristic weights:
    - recency boost
    - tag overlap boost
    - embed type/domain boost if query asks “news”
6. **Return top‑10**
  - Include `uri`, `text`, short snippet, optional explanation.

### Phase 3 — LangGraph conversational loop
7. **LangGraph state** (per “recommender session”)
  - `query_history`
  - `current_query`
  - `constraints` (langs, tags, embed types, time window)
  - `last_candidates`
8. **Graph nodes**
  - **Interpret**: parse request → structured query + constraints
  - **Retrieve**: run PGVector query → candidates
  - **Rerank**: apply LLM/heuristic scoring
  - **Respond**: top‑10 + rationale
9. **Update state**
  - Store new constraints and update `query_summary`.

### Phase 4 — Multi‑session support
10. **Session store**
- User can have multiple named recommenders:
  - `recommendation_sessions` table
  - store `state_json` (constraints, last query, etc.)
11. **Allow edits**
- “Now add tech news” updates constraints + re‑retrieves.

## Appendix
### Why this stack fits your story
- PGVector handles **fast retrieval** within a small sliding window (couple days).
- LangGraph handles **iterative refinements** cleanly.
- LLM usage stays small: only query interpretation + reranking.

### LangGraph Nodes
Suggestion for the user story flow
A simple 4‑node LangGraph is enough:
1) Interpret user request → query intent + constraints
2) Retrieve candidates (vector + filters)
3) Rerank (LLM or heuristic)
4) Respond top‑K + store session state
   Store conversation state as:
   - query_summary
   - required_topics
   - excluded_topics
   - time_window
   - last_results