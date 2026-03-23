import json
import time
from pathlib import Path
from urllib.parse import urlparse

from src.common.yandex_gpt_client import ask_yandex_gpt


SEARCH_INPUT = Path("data/raw_search_manual.json")
AI_QUERIES_FILE = Path("data/queries/ai_queries.txt")
AI_OUTPUT = Path("data/raw_ai_manual.json")


def extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").strip()
    except Exception:
        return ""


def load_ai_queries() -> set[str]:
    return {
        line.strip()
        for line in AI_QUERIES_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def build_prompt(query_text: str, results: list[dict]) -> str:
    top_results = results[:8]

    lines = []
    for i, r in enumerate(top_results, start=1):
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        url = r.get("url", "")
        domain = extract_domain(url)

        lines.append(
            f"{i}. TITLE: {title}\n"
            f"   DOMAIN: {domain}\n"
            f"   URL: {url}\n"
            f"   SNIPPET: {snippet}"
        )

    joined_results = "\n\n".join(lines)

    return f"""
Сформируй краткий аналитический ответ на запрос пользователя на основе поисковой выдачи.

Запрос:
{query_text}

Поисковые результаты:
{joined_results}

Правила:
1. Ответ должен быть на русском языке.
2. Ответ должен быть кратким, 4–7 предложений.
3. Не выдумывай факты, опирайся только на поисковые результаты.
4. Если в результатах встречаются бренды, можно упомянуть их.
5. Не перечисляй URL.
6. Не добавляй раздел "Источники".
Верни только основной текст ответа.
""".strip()


def main():
    search_data = json.loads(SEARCH_INPUT.read_text(encoding="utf-8"))
    ai_queries = load_ai_queries()

    filtered_rows = [row for row in search_data if row["query_text"] in ai_queries]

    print(f"Total search queries in JSON: {len(search_data)}")
    print(f"AI target queries from file: {len(ai_queries)}")
    print(f"Rows selected for AI generation: {len(filtered_rows)}")

    output_data = []

    for idx, row in enumerate(filtered_rows, start=1):
        query_text = row["query_text"]
        results = row.get("results", [])

        if not results:
            print(f"Skip empty search results: {query_text}")
            continue

        print(f"Generating AI answer for ({idx}/{len(filtered_rows)}): {query_text}")

        prompt = build_prompt(query_text, results)

        try:
            ai_answer_text = ask_yandex_gpt(
                prompt=prompt,
                temperature=0.2,
                max_tokens=400,
                system_prompt=(
                    "Ты аналитический помощник. "
                    "Пишешь кратко, по делу, без выдуманных фактов."
                ),
                max_retries=5,
                base_delay=2.0,
            )
        except Exception as e:
            print(f"Failed to generate AI answer for '{query_text}': {e}")
            continue

        domains = []
        for result in results[:8]:
            domain = extract_domain(result.get("url", ""))
            if domain and domain not in domains:
                domains.append(domain)

        output_data.append({
            "query_text": query_text,
            "engine": "yandex_gpt_based_on_search_api",
            "ai_answer_text": ai_answer_text,
            "sources": domains
        })

        time.sleep(1.5)

    AI_OUTPUT.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Saved AI dataset to {AI_OUTPUT}")
    print(f"Generated AI answers: {len(output_data)}")


if __name__ == "__main__":
    main()