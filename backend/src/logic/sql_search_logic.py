import re
from typing import Any, Dict, List, Optional, Literal

from ..db import get_connection


_UNSAFE_PATTERNS = [
    r"\b(drop|truncate|alter|create|rename)\b",
    r"\b(insert|update|delete|replace)\b",
    r"\b(grant|revoke)\b",
    r"\b(call|execute|prepare)\b",
    r"\b(load_file|outfile|dumpfile)\b",
    r"--", r"/\*", r"\*/",
]


def validate_sql(sql_query: str) -> Literal["valid", "unsafe", "invalid"]:
    """
    Regole richieste dal prof/tester:
    - 'unsafe' se NON è SELECT o contiene comandi di modifica DB
    - 'invalid' se vuota
    - 'valid' se SELECT “sicura” (poi la query deve anche eseguire senza errori)
    """
    if sql_query is None:
        return "invalid"
    q = sql_query.strip()
    if not q:
        return "invalid"

    # multi-statement: consentiamo al massimo un ';' finale
    if q.count(";") > 1 or (q.count(";") == 1 and not q.endswith(";")):
        return "unsafe"

    q = q[:-1].strip() if q.endswith(";") else q

    # deve iniziare con SELECT
    if not re.match(r"(?is)^\s*select\b", q):
        return "unsafe"

    # blocco keyword/commenti pericolosi
    for pat in _UNSAFE_PATTERNS:
        if re.search(pat, q, flags=re.IGNORECASE):
            return "unsafe"

    return "valid"



def _fetch_rows_as_dicts(cur) -> List[Dict[str, Any]]:
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]


def _row_to_item(row: Dict[str, Any], item_type: str) -> Dict[str, Any]:
    # Il tester estrae i risultati cercando property_name == "name" :contentReference[oaicite:1]{index=1}
    keys = list(row.keys())
    name_key = None
    for cand in ["titolo", "title", "nome", "name"]:
        if cand in row:
            name_key = cand
            break
    if name_key is None and keys:
        name_key = keys[0]

    properties: List[Dict[str, Any]] = []
    if name_key is not None:
        properties.append({"property_name": "name", "property_value": row.get(name_key)})

    for k, v in row.items():
        if k == name_key:
            continue
        properties.append({"property_name": k, "property_value": v})

    return {"item_type": "film", "properties": properties}


def run_sql_search(sql_query: str) -> Dict[str, Any]:
    """
    Ritorna SEMPRE:
    {"sql": str, "sql_validation": "valid|unsafe|invalid", "results": list|None}
    come da specifiche progetto finale :contentReference[oaicite:2]{index=2}
    """
    status = validate_sql(sql_query)

    if status != "valid":
        return {"sql": sql_query, "sql_validation": status, "results": None}

    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql_query)
            rows = _fetch_rows_as_dicts(cur)
            cur.close()
        finally:
            conn.close()
    except Exception:
        # SELECT che non esegue (es. tabella inesistente) => invalid, results None :contentReference[oaicite:3]{index=3}
        return {"sql": sql_query, "sql_validation": "invalid", "results": None}

    results = [_row_to_item(r, "film") for r in rows]
    return {"sql": sql_query, "sql_validation": "valid", "results": results}