import base64
import json
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://searchapi.api.cloud.yandex.net/v2/web/search"
QUERIES_FILE = Path("data/queries/search_queries.txt")
OUTPUT_FILE = Path("data/raw_search_manual.json")


def fetch_search_xml(query: str) -> str:
    headers = {
        "Authorization": f"Api-Key {os.getenv('YANDEX_API_KEY')}",
        "Content-Type": "application/json",
    }

    payload = {
        "query": {
            "searchType": "SEARCH_TYPE_RU",
            "queryText": query
        },
        "folderId": os.getenv("YANDEX_FOLDER_ID"),
        "responseFormat": "FORMAT_XML",
        "page": 0,
        "fixTypoMode": "FIX_TYPO_MODE_ON",
        "userAgent": "Mozilla/5.0"
    }

    response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    data = response.json()

    raw_data_b64 = data.get("rawData")
    if not raw_data_b64:
        return ""

    try:
        xml_bytes = base64.b64decode(raw_data_b64)
        return xml_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def parse_search_xml(xml_text: str) -> list[dict]:
    if not xml_text:
        return []

    soup = BeautifulSoup(xml_text, "xml")
    results = []

    groups = soup.find_all("group")
    rank = 1

    for group in groups:
        doc = group.find("doc")
        if not doc:
            continue

        title_tag = doc.find("title")
        url_tag = doc.find("url")

        passage_tag = doc.find("passage")

        title = title_tag.get_text(" ", strip=True) if title_tag else None
        url = url_tag.get_text(" ", strip=True) if url_tag else None
        snippet = passage_tag.get_text(" ", strip=True) if passage_tag else ""

        if not title or not url:
            continue

        results.append({
            "rank": rank,
            "title": title,
            "url": url,
            "snippet": snippet
        })
        rank += 1

    return results


def main():
    queries = [
        line.strip()
        for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    output_data = []

    for query in queries:
        print(f"Collecting: {query}")

        xml_text = fetch_search_xml(query)

        Path("debug_search_response.xml").write_text(xml_text, encoding="utf-8")

        results = parse_search_xml(xml_text)

        output_data.append({
            "query_text": query,
            "source_name": "yandex_search_api",
            "results": results
        })

        print(f"  -> found {len(results)} results")

    OUTPUT_FILE.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()