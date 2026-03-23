import psycopg2
from src.common.db import get_connection

def classify_intent(query: str) -> str:
    q = query.lower().strip()

    brands = [
        "oral-b",
        "pampers",
        "pantene",
        "head & shoulders",
        "philips",
        "clear",
        "huggies",
        "gillette"
    ]

    comparison_markers = [
        " vs ",
        " против ",
        "сравнение",
        "что лучше",
        "разница между"
    ]

    consultation_markers = [
        "какая",
        "какой",
        "какие",
        "лучше для",
        "как выбрать",
        "как подобрать",
        "посоветуй",
        "рейтинг",
        "топ"
    ]

    for marker in comparison_markers:
        if marker in q:
            return "comparison"

    if " или " in q:
        brands_found = sum(1 for brand in brands if brand in q)
        if brands_found >= 2:
            return "comparison"

    for marker in consultation_markers:
        if marker in q:
            return "consultation"

    for brand in brands:
        if brand in q:
            return "brand"

    return "category"


conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT query_id, query_text
FROM dwh.dim_query;
""")

queries = cur.fetchall()

update_query = """
UPDATE dwh.dim_query
SET intent_type = %s
WHERE query_id = %s;
"""

for query_id, query_text in queries:
    intent = classify_intent(query_text)

    cur.execute(
        update_query,
        (intent, query_id)
    )

conn.commit()
cur.close()
conn.close()

print("Intent classification completed")