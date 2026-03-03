import mariadb

# Parametri del DB -- da sostituire quando avrete il container MariaDB sul Mac
DB_HOST = "mariadb"   # deve essere il nome del servizio MariaDB nel docker-compose
DB_PORT = 3306
DB_USER = "user"      # come in docker-compose
DB_PASSWORD = "password"
DB_NAME = "movies_db"



def get_connection() -> mariadb.Connection:
    """
    Crea una connessione a MariaDB e la restituisce.
    """
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


def execute_select(query: str, params: tuple | None = None) -> list[tuple]:
    """
    Esegue una SELECT e restituisce una lista di tuple.
    """
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


def execute_modify(query: str, params: tuple | None = None) -> None:
    """
    Esegue INSERT/UPDATE/DELETE e fa commit.
    """
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
