from src.common.db import get_connection

conn = get_connection()
cur = conn.cursor()

insert_query = """
INSERT INTO raw.search_responses (
    query_text,
    source_name,
    response_body,
    is_processed
)
VALUES (%s, %s, %s, FALSE);
"""

html_1 = """
<html>
    <body>
        <div class="result">
            <a href="https://www.ozon.ru/product/elektricheskaya-zubnaya-shchetka-123">
                Электрическая зубная щетка Oral-B
            </a>
            <p>Качественная электрическая зубная щетка для ежедневного ухода.</p>
        </div>
        <div class="result">
            <a href="https://irecommend.ru/content/luchshaya-zubnaya-shchetka">
                Лучшая электрическая зубная щетка — обзор
            </a>
            <p>Сравниваем популярные модели и бренды.</p>
        </div>
    </body>
</html>
"""

html_2 = """
<html>
    <body>
        <div class="result">
            <a href="https://www.wildberries.ru/catalog/shampun-ot-perkhoti">
                Шампунь от перхоти Head & Shoulders
            </a>
            <p>Популярный шампунь против перхоти для ежедневного применения.</p>
        </div>
        <div class="result">
            <a href="https://expert.ru/hair-care/kak-vybrat-shampun">
                Как выбрать шампунь от перхоти
            </a>
            <p>Экспертный материал о выборе шампуня по типу волос.</p>
        </div>
    </body>
</html>
"""

test_data = [
    ("электрическая зубная щетка", "test_source", html_1),
    ("шампунь от перхоти", "test_source", html_2)
]

for row in test_data:
    cur.execute(insert_query, row)

conn.commit()
cur.close()
conn.close()

print("Тестовые raw search-данные успешно загружены")