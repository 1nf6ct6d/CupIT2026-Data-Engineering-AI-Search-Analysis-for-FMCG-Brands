from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src.common.db import get_connection


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc


conn = get_connection()
cur = conn.cursor()

select_query = """
SELECT raw_id, query_text, response_body
FROM raw.search_responses
WHERE is_processed = FALSE
ORDER BY raw_id;
"""

cur.execute(select_query)
raw_rows = cur.fetchall()

insert_query = """
INSERT INTO stage.search_results (
    raw_id,
    query_text,
    rank_num,
    result_title,
    result_url,
    result_domain,
    result_snippet,
    is_loaded_to_dwh
)
VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE);
"""

mark_raw_processed_query = """
UPDATE raw.search_responses
SET is_processed = TRUE,
    processed_at = CURRENT_TIMESTAMP
WHERE raw_id = %s;
"""

for raw_id, query_text, response_body in raw_rows:
    soup = BeautifulSoup(response_body, "lxml")
    results = soup.find_all("div", class_="result")

    rank = 1
    for result in results:
        link_tag = result.find("a")
        snippet_tag = result.find("p")

        title = link_tag.get_text(strip=True) if link_tag else None
        url = link_tag.get("href") if link_tag else None
        domain = extract_domain(url) if url else None
        snippet = snippet_tag.get_text(strip=True) if snippet_tag else None

        cur.execute(
            insert_query,
            (
                raw_id,
                query_text,
                rank,
                title,
                url,
                domain,
                snippet
            )
        )
        rank += 1

    cur.execute(mark_raw_processed_query, (raw_id,))

conn.commit()
cur.close()
conn.close()

print("Search-данные успешно распарсены из raw в stage (incremental mode)")