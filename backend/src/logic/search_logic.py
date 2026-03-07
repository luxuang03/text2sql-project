import re
from typing import List, Dict
from ..db import get_connection
from .schema_prompt import get_schema_for_llm
from .sql_search_logic import run_sql_search
import requests
import os
from ..db import execute_select



def _normalize_question(q: str) -> str:
    """
    Normalizzazione minima per match esatto:
    - trim
    - collassa spazi multipli
    - lower
    - rimuove punto finale (.)
    """
    q = " ".join(q.strip().split())
    q = q.lower()
    if q.endswith("."):
        q = q[:-1]
    return q


def _extract_year_from_question(question: str) -> int:
    match = re.search(r"\d+", question)
    if not match:
        raise ValueError("Nessun anno trovato nella domanda.")
    return int(match.group(0))


def _extract_min_age_from_question(question: str) -> int:
    match = re.search(r"\d+", question)
    if not match:
        raise ValueError("Nessuna età trovata nella domanda.")
    return int(match.group(0))


def _make_item(item_type: str, name: str) -> Dict:
    """
    Crea un oggetto nel formato richiesto dal test:

    {
      "item_type": "...",
      "properties": [
        {"property_name": "name", "property_value": name}
      ]
    }
    """
    return {
        "item_type": item_type,
        "properties": [
            {
                "property_name": "name",
                "property_value": name,
            }
        ],
    }


def handle_search(question: str) -> List[Dict]:
    """
    Gestisce l'endpoint /search.

    Domande supportate:
    1) 'Elenca i film del <ANNO>.'
    2) 'Quali sono i registi presenti su Netflix?'
    3) 'Elenca tutti i film di fantascienza.'
    4) 'Quali film sono stati fatti da un regista di almeno <ANNI> anni?'
    5) 'Quali registi hanno fatto più di un film?'

    L'output è SEMPRE una lista di item nel formato:

    {
      "item_type": "film" | "director",
      "properties": [
        {"property_name": "name", "property_value": "<titolo_o_nome>"}
      ]
    }
    """

    original_question = question
    q_norm = _normalize_question(question)


    results: List[Dict] = []

    # 1) "Elenca i film del <ANNO>."
    if q_norm.startswith("elenca i film del"):
        year = _extract_year_from_question(question)

        sql = """
            SELECT m.titolo, m.anno
            FROM movies AS m
            WHERE m.anno = ?
            ORDER BY m.titolo;
        """
        rows = execute_select(sql, (year,))

        for title, year_value in rows:
            # Il test si aspetta item_type = "film" e name = titolo
            results.append(_make_item("film", title))

        return results

    # 2) "Quali sono i registi presenti su Netflix?"
    if q_norm == "quali sono i registi presenti su netflix?":
        sql = """
            SELECT DISTINCT d.nome
            FROM directors d
            JOIN movies m           ON d.id = m.regista_id
            JOIN movie_platforms mp ON m.id = mp.movie_id
            JOIN platforms p        ON p.id = mp.platform_id
            WHERE LOWER(TRIM(p.nome)) = 'netflix'
            ORDER BY d.nome;
        """
        rows = execute_select(sql)

        for (director_name,) in rows:
            results.append(_make_item("director", director_name))

        return results

    # 3) "Elenca tutti i film di fantascienza."
    if q_norm == "elenca tutti i film di fantascienza":
        sql = """
            SELECT m.titolo, m.anno, m.genere
            FROM movies AS m
            WHERE LOWER(TRIM(m.genere)) = 'fantascienza'
            ORDER BY m.anno, m.titolo;
        """
        rows = execute_select(sql)

        for title, year_value, genre in rows:
            results.append(_make_item("film", title))

        return results

    # 4) "Quali film sono stati fatti da un regista di almeno <ANNI> anni?"
    if q_norm.startswith("quali film sono stati fatti da un regista di almeno"):
        min_age = _extract_min_age_from_question(question)

        sql = """
            SELECT 
                m.titolo,
                m.anno,
                d.nome AS director_name,
                d.eta  AS director_age
            FROM movies AS m
            JOIN directors AS d ON m.regista_id = d.id
            WHERE d.eta >= ?
            ORDER BY m.anno, m.titolo;
        """
        rows = execute_select(sql, (min_age,))

        for title, year_value, director_name, director_age in rows:
            results.append(_make_item("film", title))

        return results

    # 5) "Quali registi hanno fatto più di un film?"
    if q_norm == "quali registi hanno fatto più di un film?":
        sql = """
            SELECT 
                d.nome,
                COUNT(m.id) AS movies_count
            FROM directors d
            JOIN movies m ON d.id = m.regista_id
            GROUP BY d.id, d.nome
            HAVING COUNT(m.id) > 1
            ORDER BY d.nome;
        """
        rows = execute_select(sql)

        for director_name, movies_count in rows:
            results.append(_make_item("director", director_name))

        return results

    # Se la domanda non è riconosciuta -> lista vuota
    raise ValueError("Domanda non supportata")





def ask_ollama(prompt: str, model: str | None = None) -> str:
    ollama_url = os.getenv("OLLAMA_URL", "http://ollama:11434/api/generate")
    model_name = model or os.getenv("OLLAMA_MODEL", "gemma3:1b-it-qat")

    response = requests.post(
        ollama_url,
        json={
            "model": model_name,
            "prompt": prompt,
            "stream": False
        },
        timeout=180
    )
    response.raise_for_status()
    data = response.json()

    return data.get("response", "").strip()


def search_with_llm(question: str, model: str | None = None):
    conn = get_connection()
    try:
        schema_str = get_schema_for_llm(conn)

        prompt = f"""
Sei un assistente che genera query SQL per MariaDB.

Schema del database:
{schema_str}

Domanda utente:
{question}

Regole:
- genera una sola query SQL
- usa solo SELECT
- usa solo tabelle e colonne presenti nello schema
- non scrivere spiegazioni
- non usare markdown
- non usare ```sql

Rispondi solo con la query SQL.
""".strip()

        sql_query = ask_ollama(prompt, model=model)

        sql_result = run_sql_search(sql_query)

        return {
            "sql": sql_query,
            "sql_validation": sql_result["sql_validation"],
            "results": sql_result["results"]
        }

    finally:
        conn.close()
