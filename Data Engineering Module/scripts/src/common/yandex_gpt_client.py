import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"


def ask_yandex_gpt(
    prompt: str,
    temperature: float = 0.3,
    max_tokens: int = 500,
    system_prompt: str | None = None,
    max_retries: int = 5,
    base_delay: float = 2.0,
) -> str:
    api_key = os.getenv("YANDEX_API_KEY")
    folder_id = os.getenv("YANDEX_FOLDER_ID")

    if not api_key:
        raise ValueError("YANDEX_API_KEY не найден")
    if not folder_id:
        raise ValueError("YANDEX_FOLDER_ID не найден")

    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
        "x-folder-id": folder_id,
    }

    messages = []

    if system_prompt:
        messages.append({
            "role": "system",
            "text": system_prompt
        })

    messages.append({
        "role": "user",
        "text": prompt
    })

    payload = {
        "modelUri": f"gpt://{folder_id}/yandexgpt/latest",
        "completionOptions": {
            "stream": False,
            "temperature": temperature,
            "maxTokens": max_tokens,
        },
        "messages": messages,
    }

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                API_URL,
                headers=headers,
                json=payload,
                timeout=90
            )
            response.raise_for_status()

            data = response.json()
            return data["result"]["alternatives"][0]["message"]["text"].strip()

        except requests.exceptions.RequestException as e:
            last_error = e

            if attempt == max_retries:
                break

            delay = base_delay * attempt
            print(
                f"YandexGPT request failed (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

    raise RuntimeError(f"YandexGPT request failed after {max_retries} attempts: {last_error}")


if __name__ == "__main__":
    answer = ask_yandex_gpt(
        "Ответь одной короткой фразой: привет",
        max_tokens=50
    )
    print(answer)