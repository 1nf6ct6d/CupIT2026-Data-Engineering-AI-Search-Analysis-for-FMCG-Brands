import psycopg2
from src.common.db import get_connection

def classify_category(query: str) -> str:
    q = query.lower().strip()

    oral_keywords = [
        "зубная щетка",
        "зубная паста",
        "oral-b",
        "blend-a-med"
    ]

    hair_keywords = [
        "шампунь",
        "перхоть",
        "волос",
        "pantene",
        "head & shoulders",
        "head and shoulders"
    ]

    baby_keywords = [
        "подгузник",
        "памперс",
        "pampers",
        "детские салфетки"
    ]

    for keyword in oral_keywords:
        if keyword in q:
            return "Oral Care"

    for keyword in hair_keywords:
        if keyword in q:
            return "Hair Care"

    for keyword in baby_keywords:
        if keyword in q:
            return "Baby Care"

    return "Other"


conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT query_id, query_text
FROM dwh.dim_query;
""")
queries = cur.fetchall()

update_query = """
UPDATE dwh.dim_query
SET category_name = %s
WHERE query_id = %s;
"""

for query_id, query_text in queries:
    category = classify_category(query_text)

    cur.execute(
        update_query,
        (category, query_id)
    )

conn.commit()
cur.close()
conn.close()

print("Category classification completed")