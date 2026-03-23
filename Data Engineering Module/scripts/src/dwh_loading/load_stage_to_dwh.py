from src.common.db import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
INSERT INTO dwh.dim_query (query_text)
SELECT DISTINCT query_text
FROM stage.search_results
ON CONFLICT (query_text) DO NOTHING;
""")

cur.execute("""
INSERT INTO dwh.dim_domain (domain_name)
SELECT DISTINCT result_domain
FROM stage.search_results
WHERE result_domain IS NOT NULL
ON CONFLICT (domain_name) DO NOTHING;
""")

cur.execute("""
SELECT source_id
FROM dwh.dim_source
WHERE source_name = 'test_source';
""")
source_row = cur.fetchone()

if not source_row:
    raise ValueError("Источник 'test_source' не найден в dwh.dim_source")

source_id = source_row[0]

cur.execute("""
SELECT
    result_id,
    query_text,
    rank_num,
    result_title,
    result_url,
    result_domain,
    result_snippet
FROM stage.search_results
WHERE is_loaded_to_dwh = FALSE
ORDER BY result_id;
""")
stage_rows = cur.fetchall()

insert_fact_query = """
INSERT INTO dwh.fact_search_results (
    query_id,
    domain_id,
    source_id,
    rank_num,
    result_title,
    result_url,
    result_snippet
)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

mark_stage_loaded_query = """
UPDATE stage.search_results
SET is_loaded_to_dwh = TRUE,
    loaded_to_dwh_at = CURRENT_TIMESTAMP
WHERE result_id = %s;
"""

for (
    result_id,
    query_text,
    rank_num,
    result_title,
    result_url,
    result_domain,
    result_snippet
) in stage_rows:

    cur.execute("""
    SELECT query_id
    FROM dwh.dim_query
    WHERE query_text = %s;
    """, (query_text,))
    query_id = cur.fetchone()[0]

    cur.execute("""
    SELECT domain_id
    FROM dwh.dim_domain
    WHERE domain_name = %s;
    """, (result_domain,))
    domain_id = cur.fetchone()[0]

    cur.execute(
        insert_fact_query,
        (
            query_id,
            domain_id,
            source_id,
            rank_num,
            result_title,
            result_url,
            result_snippet
        )
    )

    cur.execute(mark_stage_loaded_query, (result_id,))

conn.commit()
cur.close()
conn.close()

print("Stage search-данные успешно загружены в DWH (incremental mode)")