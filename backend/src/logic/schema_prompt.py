import os


def get_schema_for_llm(conn):
    db_name = os.getenv("DB_NAME")

    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, column_name, column_type
        FROM information_schema.columns
        WHERE table_schema = ?
        ORDER BY table_name, ordinal_position
        """,
        (db_name,)
    )

    rows = cur.fetchall()
    cur.close()

    tables = {}

    for table_name, column_name, column_type in rows:
        if table_name not in tables:
            tables[table_name] = []

        tables[table_name].append(f"{column_name}:{column_type}")

    lines = []

    for table_name in tables:
        cols = ", ".join(tables[table_name])
        lines.append(f"{table_name}({cols})")

    return "\n".join(lines)