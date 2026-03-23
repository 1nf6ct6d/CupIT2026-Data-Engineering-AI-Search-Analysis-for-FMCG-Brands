import json
from src.common.db import get_connection

FILE_PATH = "data/raw_ai_manual.json"


def main():
    conn = get_connection()
    cur = conn.cursor()

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    for row in data:

        answer = row["ai_answer_text"].strip()
        sources = row.get("sources", [])

        raw_body = "AI_ANSWER:\n"
        raw_body += answer
        raw_body += "\n\nSOURCES:\n"

        for i, source in enumerate(sources, start=1):
            raw_body += f"{i}. https://{source}\n"

        cur.execute(
            """
            INSERT INTO raw.ai_responses (
                query_text,
                source_name,
                ai_response_body,
                engine,
                is_processed
            )
            VALUES (%s, %s, %s, %s, FALSE)
            """,
            (
                row["query_text"],
                "yandex_gpt",
                raw_body,
                row["engine"],
            ),
        )

    conn.commit()

    cur.close()
    conn.close()

    print("AI raw data loaded")


if __name__ == "__main__":
    main()