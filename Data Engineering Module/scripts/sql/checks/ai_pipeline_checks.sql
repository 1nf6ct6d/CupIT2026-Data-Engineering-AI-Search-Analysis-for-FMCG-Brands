-- 1. RAW AI записи, которые не попали в STAGE
SELECT
    r.raw_ai_id,
    r.query_text,
    r.is_processed,
    r.processed_at
FROM raw.ai_responses r
LEFT JOIN stage.ai_answers s
    ON r.raw_ai_id = s.raw_ai_id
WHERE s.raw_ai_id IS NULL;


-- 2. STAGE AI ответы, которые не попали в DWH
SELECT
    s.ai_answer_id,
    s.query_text,
    s.is_loaded_to_dwh,
    s.loaded_to_dwh_at
FROM stage.ai_answers s
LEFT JOIN dwh.fact_ai_answers f
    ON s.ai_answer_id = f.ai_fact_id
WHERE f.ai_fact_id IS NULL;


-- 3. AI ответы в DWH без источников
SELECT
    f.ai_fact_id,
    f.query_id,
    f.ai_answer_text
FROM dwh.fact_ai_answers f
LEFT JOIN dwh.fact_ai_sources s
    ON f.ai_fact_id = s.ai_fact_id
WHERE s.ai_fact_id IS NULL;


-- 4. Источники в STAGE без домена
SELECT
    ai_source_id,
    ai_answer_id,
    source_rank,
    source_url,
    source_domain
FROM stage.ai_sources
WHERE source_domain IS NULL;


-- 5. Запросы без intent
SELECT
    query_id,
    query_text,
    intent_type
FROM dwh.dim_query
WHERE intent_type IS NULL;


-- 6. Запросы без category
SELECT
    query_id,
    query_text,
    category_name
FROM dwh.dim_query
WHERE category_name IS NULL;


-- 7. Дубликаты AI brand mentions
SELECT
    ai_fact_id,
    brand_id,
    COUNT(*) AS duplicate_count
FROM dwh.fact_ai_brand_mentions
GROUP BY
    ai_fact_id,
    brand_id
HAVING COUNT(*) > 1;


-- 8. Дубликаты AI answers в DWH по одному и тому же тексту
SELECT
    query_id,
    ai_answer_text,
    COUNT(*) AS duplicate_count
FROM dwh.fact_ai_answers
GROUP BY
    query_id,
    ai_answer_text
HAVING COUNT(*) > 1;