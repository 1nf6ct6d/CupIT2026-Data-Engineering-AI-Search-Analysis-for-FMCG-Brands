import psycopg2
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
SELECT ai_fact_id, ai_answer_text
FROM dwh.fact_ai_answers;
""")
ai_answers = cur.fetchall()

insert_query = """
INSERT INTO dwh.fact_ai_brand_mentions (
    ai_fact_id,
    brand_id,
    matched_text
)
VALUES (%s, %s, %s)
ON CONFLICT (ai_fact_id, brand_id) DO NOTHING;
"""

for ai_fact_id, ai_answer_text in ai_answers:
    ai_text_norm = normalize_text(ai_answer_text)

    for brand_id, brand_name in brands:
        brand_norm = normalize_text(brand_name)

        if brand_norm in ai_text_norm:
            cur.execute(
                insert_query,
                (ai_fact_id, brand_id, brand_name)
            )

conn.commit()
cur.close()
conn.close()

print("Упоминания брендов в AI-ответах успешно загружены")