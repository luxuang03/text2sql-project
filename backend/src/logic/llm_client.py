import os
import requests
import time


DEFAULT_OLLAMA_MODEL = "gemma3:1b-it-qat"
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL}/api/chat"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_PULL_URL = f"{OLLAMA_BASE_URL}/api/pull"

# Si assicura che Ollama sia raggiungibile
def wait_for_ollama_ready(timeout_seconds: int = 180) -> None:
    start = time.time()
    last_error = None

    while time.time() - start < timeout_seconds:
        try:
            response = requests.get(OLLAMA_TAGS_URL, timeout=5)
            response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)

    raise RuntimeError(f"Ollama non raggiungibile entro {timeout_seconds}s: {last_error}")

# Controlla che il modello richiesto (quello di default nel nostro caso) si già disponibile
def is_model_available(model_name: str) -> bool:
    response = requests.get(OLLAMA_TAGS_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    for model in data.get("models", []):
        if model.get("name") == model_name:
            return True
    return False

# Scarica il modello di Ollama
def pull_model(model_name: str) -> None:
    response = requests.post(
        OLLAMA_PULL_URL,
        json={"model": model_name, "stream": False},
    )
    response.raise_for_status()

# Verifica che Ollama sia raggiungibile e scarica il modello se manca
def ensure_ollama_model(model_name: str = DEFAULT_OLLAMA_MODEL) -> None:
    wait_for_ollama_ready()

    if not is_model_available(model_name):
        print(f"[startup] Modello {model_name} non presente. Avvio download...")
        pull_model(model_name)
        print(f"[startup] Download modello {model_name} completato.")
    else:
        print(f"[startup] Modello {model_name} già disponibile.")

# Manda ad Ollama la richiesta di generazione della query sql
def ask_ollama_for_sql(prompt: str, model: str | None = None) -> str:

    ollama_url = os.getenv("OLLAMA_URL", "http://ollama:11434/api/chat")
    model_name = model or os.getenv("OLLAMA_MODEL", "gemma3:1b-it-qat")

    response = requests.post(
        ollama_url,
        json={
            "model": model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "Sei un assistente che traduce domande in SQL per MariaDB. Devi rispondere solo con una query SELECT valida."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "stream": False,
            "options": {
                "temperature": 0,
                "top_p": 1,
                "seed": 42
            }
        },
        timeout=180
    )

    response.raise_for_status()

    data = response.json()

    return data["message"]["content"].strip()