import psycopg2
from src.common.db import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
INSERT INTO dwh.dim_query (query_text)
SELECT DISTINCT query_text
FROM stage.ai_answers
ON CONFLICT (query_text) DO NOTHING;
""")

cur.execute("""
INSERT INTO dwh.dim_domain (domain_name)
SELECT DISTINCT source_domain
FROM stage.ai_sources
WHERE source_domain IS NOT NULL
ON CONFLICT (domain_name) DO NOTHING;
""")

cur.execute("""
SELECT ai_answer_id, query_text, ai_answer_text
FROM stage.ai_answers
WHERE is_loaded_to_dwh = FALSE
ORDER BY ai_answer_id;
""")
stage_answers = cur.fetchall()

cur.execute("""
SELECT source_id
FROM dwh.dim_source
WHERE source_name = 'test_ai_source';
""")
source_row = cur.fetchone()

if not source_row:
    raise ValueError("Источник 'test_ai_source' не найден в dwh.dim_source")

source_id = source_row[0]

insert_fact_ai_answer = """
INSERT INTO dwh.fact_ai_answers (
    query_id,
    source_id,
    ai_answer_text
)
VALUES (%s, %s, %s)
RETURNING ai_fact_id;
"""

insert_fact_ai_source = """
INSERT INTO dwh.fact_ai_sources (
    ai_fact_id,
    domain_id,
    source_rank,
    source_url
)
VALUES (%s, %s, %s, %s);
"""

mark_stage_loaded = """
UPDATE stage.ai_answers
SET is_loaded_to_dwh = TRUE,
    loaded_to_dwh_at = CURRENT_TIMESTAMP
WHERE ai_answer_id = %s;
"""

for ai_answer_id, query_text, ai_answer_text in stage_answers:
    cur.execute("""
    SELECT query_id
    FROM dwh.dim_query
    WHERE query_text = %s;
    """, (query_text,))
    query_id = cur.fetchone()[0]

    cur.execute(
        insert_fact_ai_answer,
        (query_id, source_id, ai_answer_text)
    )
    ai_fact_id = cur.fetchone()[0]

    cur.execute("""
    SELECT source_rank, source_url, source_domain
    FROM stage.ai_sources
    WHERE ai_answer_id = %s
    ORDER BY source_rank;
    """, (ai_answer_id,))
    sources = cur.fetchall()

    for source_rank, source_url, source_domain in sources:
        cur.execute("""
        SELECT domain_id
        FROM dwh.dim_domain
        WHERE domain_name = %s;
        """, (source_domain,))
        domain_id = cur.fetchone()[0]

        cur.execute(
            insert_fact_ai_source,
            (ai_fact_id, domain_id, source_rank, source_url)
        )

    cur.execute(mark_stage_loaded, (ai_answer_id,))

conn.commit()
cur.close()
conn.close()

print("Incremental AI stage -> DWH loading completed")