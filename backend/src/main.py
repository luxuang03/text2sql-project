from fastapi import FastAPI, HTTPException, Body
from contextlib import asynccontextmanager
from pathlib import Path
import time
from .db import get_connection
from .logic.add_logic import handle_add, AddLineFormatError, handle_add_from_tsv
from .logic.schema_logic import get_schema
from .logic.search_logic import handle_search
from .logic.sql_search_logic import run_sql_search
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Literal


def _is_movies_table_empty() -> bool:
    """
    True se la tabella movies è vuota, False se contiene almeno un record.
    Solleva eccezioni se il DB non è raggiungibile o la tabella non esiste ancora.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies;")
        (n,) = cur.fetchone()
        cur.close()
        return n == 0
    finally:
        conn.close()


def populate_db_on_startup():
    """
    Popola il database leggendo backend/src/data.tsv, ma SOLO se movies è vuota.
    Include un retry perché MariaDB/tabelle potrebbero non essere pronte al primo colpo.
    """
    # Retry DB/tabelle pronte
    ready = False
    last_err = None
    for _ in range(30):  # ~30 secondi
        try:
            empty = _is_movies_table_empty()
            # se la query è riuscita, DB/tabelle sono pronti
            ready = True
            if not empty:
                print("[startup] DB già popolato (movies non vuota). Skip popolamento.")
                return
            break  # tabella pronta e vuota -> procedo a popolare
        except Exception as e:
            last_err = e
            print(f"[startup] DB non pronto: {e}")
            time.sleep(1)

    if not ready:
        print(f"[startup] DB non pronto dopo retry. Ultimo errore: {last_err}")
        return

    # Percorso data.tsv (stessa cartella di main.py)
    tsv_path = Path(__file__).parent / "data.tsv"
    if not tsv_path.exists():
        print(f"[startup] data.tsv non trovato in {tsv_path}")
        return

    print(f"[startup] Popolamento DB da {tsv_path}...")

    inserted = 0
    skipped = 0

    with tsv_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                handle_add_from_tsv(line)
                inserted += 1
            except AddLineFormatError as e:
                skipped += 1
                print(f"[startup] Riga TSV non valida: {e} -- riga: {line}")
            except Exception as e:
                skipped += 1
                print(f"[startup] Errore inserendo riga: {e} -- riga: {line}")

    print(f"[startup] Popolamento DB completato. Inserite: {inserted}, Skippate: {skipped}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    populate_db_on_startup()
    yield


app = FastAPI(
    title="Text2SQL Backend",
    version="0.1.0",
    description="Backend FastAPI per esonero/progetto",
    lifespan=lifespan,
)

class SqlSearchRequest(BaseModel):
    sql_query: str


class SqlSearchResponse(BaseModel):
    sql: str
    sql_validation: Literal["valid", "unsafe", "invalid"]
    results: Optional[List[Dict[str, Any]]]




@app.get("/")
def root():
    return {"status": "ok", "message": "Backend FastAPI attivo"}


@app.get("/schema_summary")
def schema_summary():
    return get_schema()


@app.post("/sql_search", response_model=SqlSearchResponse)
def sql_search(payload: SqlSearchRequest):
    return run_sql_search(payload.sql_query)

@app.get("/search/{question}")
def search_path(question: str):
    try:
        return handle_search(question)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))



@app.post("/add")
def add(payload: dict = Body(...)):
    """
    Input JSON: {"data_line": "..."} con virgole come delimitatore.
    Output: {"status":"ok"} oppure 422 se formato non valido.
    """
    data_line = payload.get("data_line")
    if data_line is None:
        raise HTTPException(status_code=422, detail="Campo 'data_line' mancante nel JSON.")

    try:
        return handle_add(data_line)
    except AddLineFormatError as e:
        raise HTTPException(status_code=422, detail=str(e))
