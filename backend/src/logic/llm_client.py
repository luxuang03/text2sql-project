import os
import requests


def ask_ollama_for_sql(prompt: str, model: str | None = None) -> str:

    ollama_url = os.getenv("OLLAMA_URL", "http://ollama:11434/api/chat")
    model_name = model or os.getenv("OLLAMA_MODEL", "gemma3:1b-it-qat")

    response = requests.post(
        ollama_url,
        json={
            "model": model_name,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "stream": False
        },
        timeout=180
    )

    response.raise_for_status()

    data = response.json()

    return data["message"]["content"].strip()