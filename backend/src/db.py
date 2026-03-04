import mariadb

# Parametri del DB 
DB_HOST = "mariadb"   
DB_PORT = 3306
DB_USER = "user"      
DB_PASSWORD = "password"
DB_NAME = "movies_db"



# Crea connessione con mariadb
def get_connection() -> mariadb.Connection:
    try:
        conn = mariadb.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        conn.autocommit = True
        return conn
    except mariadb.Error as e:
        print(f"[db] Errore connessione MariaDB: {e}")
        raise


# Esegue select
def execute_select(query: str, params: tuple | None = None) -> list[tuple]:
    if params is None:
        params = ()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


# Esegue insert/update/delete
def execute_modify(query: str, params: tuple | None = None) -> None:
    if params is None:
        params = ()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
    finally:
        conn.close()
