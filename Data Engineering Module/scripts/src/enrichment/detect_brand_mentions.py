from src.common.db import get_connection


def normalize_text(text: str) -> str:
    return text.lower() if text else ""


conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT brand_id, brand_name
FROM dwh.dim_brand;
""")
brands = cur.fetchall()

cur.execute("""
SELECT fact_id, result_title, result_snippet
FROM dwh.fact_search_results;
""")
search_results = cur.fetchall()

insert_query = """
INSERT INTO dwh.fact_brand_mentions (
    fact_id,
    brand_id,
    mention_location,
    matched_text
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (fact_id, brand_id, mention_location) DO NOTHING;
"""

for fact_id, result_title, result_snippet in search_results:
    title_norm = normalize_text(result_title)
    snippet_norm = normalize_text(result_snippet)

    for brand_id, brand_name in brands:
        brand_norm = normalize_text(brand_name)

        if brand_norm in title_norm:
            cur.execute(
                insert_query,
                (fact_id, brand_id, "title", brand_name)
            )

        if brand_norm in snippet_norm:
            cur.execute(
                insert_query,
                (fact_id, brand_id, "snippet", brand_name)
            )

conn.commit()
cur.close()
conn.close()

print("Упоминания брендов успешно определены и загружены")