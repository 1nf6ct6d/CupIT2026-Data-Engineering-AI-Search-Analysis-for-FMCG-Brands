import psycopg2
from urllib.parse import urlparse
from src.common.db import get_connection


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc


def parse_ai_response(raw_text: str):

    if "AI_ANSWER:" not in raw_text or "SOURCES:" not in raw_text:
        return None, []

    answer_part, sources_part = raw_text.split("SOURCES:", 1)
    answer_text = answer_part.replace("AI_ANSWER:", "").strip()

    source_urls = []
    for line in sources_part.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if ". " in line:
            _, url = line.split(". ", 1)
            source_urls.append(url.strip())

    return answer_text, source_urls

conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT raw_ai_id, query_text, ai_response_body
FROM raw.ai_responses
WHERE is_processed = FALSE
ORDER BY raw_ai_id;
""")
raw_ai_rows = cur.fetchall()

insert_ai_answer_query = """
INSERT INTO stage.ai_answers (
    raw_ai_id,
    query_text,
    ai_answer_text
)
VALUES (%s, %s, %s)
RETURNING ai_answer_id;
"""

insert_ai_source_query = """
INSERT INTO stage.ai_sources (
    ai_answer_id,
    source_rank,
    source_url,
    source_domain
)
VALUES (%s, %s, %s, %s);
"""

mark_raw_processed_query = """
UPDATE raw.ai_responses
SET is_processed = TRUE,
    processed_at = CURRENT_TIMESTAMP
WHERE raw_ai_id = %s;
"""

for raw_ai_id, query_text, ai_response_body in raw_ai_rows:
    ai_answer_text, source_urls = parse_ai_response(ai_response_body)
    if not ai_answer_text:
        continue

    cur.execute(
        insert_ai_answer_query,
        (raw_ai_id, query_text, ai_answer_text)
    )
    ai_answer_id = cur.fetchone()[0]

    rank = 1
    for url in source_urls:
        domain = extract_domain(url)

        cur.execute(
            insert_ai_source_query,
            (ai_answer_id, rank, url, domain)
        )
        rank += 1

    cur.execute(mark_raw_processed_query, (raw_ai_id,))

conn.commit()
cur.close()
conn.close()

print("AI raw-данные успешно распарсены в stage (incremental mode)")