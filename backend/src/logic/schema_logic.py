from ..db import execute_select


def get_schema() -> list[dict]:
    """
    Restituisce tutte le tabelle e colonne del database movies_db
    come lista di dizionari con chiavi:
    - table_name
    - table_column   <-- NOME RICHIESTO DAL TEST
    """
    query = """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """

    rows = execute_select(query, ("movies_db",))

    result: list[dict] = []
    for table_name, column_name in rows:
        result.append(
            {
                "table_name": table_name,
                "table_column": column_name,  # <-- QUI IL NOME CORRETTO
            }
        )

    return result
