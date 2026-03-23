import json
from pathlib import Path
from src.common.db import get_connection


def build_html(results: list[dict]) -> str:
    html = ["<html><body>"]

    for result in results:
        title = result.get("title") or ""
        url = result.get("url") or ""
        snippet = result.get("snippet") or ""

        html.append(f"""
        <div class="result">
            <a href="{url}">
                {title}
            </a>
            <p>{snippet}</p>
        </div>
        """)

    html.append("</body></html>")
    return "\n".join(html)


def main():
    file_path = Path("data/raw_search_manual.json")
    data = json.loads(file_path.read_text(encoding="utf-8"))

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

    inserted = 0

    for row in data:
        query_text = row["query_text"]
        source_name = row["source_name"]
        results = row.get("results", [])

        if not results:
            continue

        html = build_html(results)

        cur.execute(
            insert_query,
            (query_text, source_name, html)
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {inserted} search raw rows from JSON.")


if __name__ == "__main__":
    main()