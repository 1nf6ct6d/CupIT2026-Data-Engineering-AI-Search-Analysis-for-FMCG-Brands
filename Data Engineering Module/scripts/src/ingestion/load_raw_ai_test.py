import psycopg2
from src.common.db import get_connection

conn = get_connection()
cur = conn.cursor()

insert_query = """
INSERT INTO raw.ai_responses (query_text, source_name, ai_response_body)
VALUES (%s, %s, %s);
"""

ai_text_1 = """
AI_ANSWER:
Head & Shoulders is one of the most popular anti-dandruff shampoos. Clear is also often mentioned as an alternative.

SOURCES:
1. https://www.wildberries.ru/catalog/shampun-head-shoulders
2. https://expert.ru/hair-care/best-dandruff-shampoos
3. https://irecommend.ru/content/head-shoulders-review
"""

ai_text_2 = """
AI_ANSWER:
Oral-B is frequently recommended among electric toothbrush brands. Philips is another strong option for comparison.

SOURCES:
1. https://www.ozon.ru/product/oral-b-io-series
2. https://expert.ru/oral-care/best-electric-toothbrushes
3. https://market.yandex.ru/product--philips-sonicare
"""

ai_text_3 = """
AI_ANSWER:
Philips is one of the best electric toothbrush brands for daily use. Many users also compare it with other premium toothbrush brands.

SOURCES:
1. https://expert.ru/oral-care/philips-review
2. https://market.yandex.ru/product--philips-sonicare
3. https://irecommend.ru/content/philips-sonicare-review
"""

test_data = [
    ("шампунь от перхоти", "test_ai_source", ai_text_1),
    ("электрическая зубная щетка", "test_ai_source", ai_text_2),
    ("лучшая электрическая зубная щетка", "test_ai_source", ai_text_3)
]

for row in test_data:
    cur.execute(insert_query, row)

conn.commit()
cur.close()
conn.close()

print("Тестовые AI raw-данные успешно загружены")