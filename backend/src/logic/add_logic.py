from typing import Optional, Tuple, List

from ..db import get_connection


class AddLineFormatError(ValueError):
    """
    Eccezione per indicare che la riga passata a /add non rispetta il formato atteso.
    (Verrà trasformata in HTTP 422 da main.py.)
    """
    pass


def _parse_add_line_csv(line: str) -> Tuple[str, str, int, int, str, str, str]:
    """
    Parsea una riga CSV nel formato:
    Titolo,Regista,Età_Autore,Anno,Genere,Piattaforma_1,Piattaforma_2

    Ritorna:
    (title, director_name, director_age, year, genre, platform1, platform2)
    """
    line = line.strip()
    parts = [p.strip() for p in line.split(",")]

    if len(parts) != 7:
        raise AddLineFormatError(f"Numero di campi non valido: attesi 7, trovati {len(parts)}")

    title, director_name, director_age_str, year_str, genre, platform1, platform2 = parts

    if not title or not director_name or not genre:
        raise AddLineFormatError("Titolo, regista e genere non possono essere vuoti.")

    try:
        director_age = int(director_age_str)
        year = int(year_str)
    except ValueError:
        raise AddLineFormatError("Età autore e anno devono essere interi.")

    return title, director_name, director_age, year, genre, platform1, platform2




def _upsert_director(cur, director_name: str, director_age: int) -> int:
    """
    Inserisce o aggiorna un regista basandosi solo sul nome.
    Ritorna director_id.
    """
    cur.execute("SELECT id FROM directors WHERE name = ?;", (director_name,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            "INSERT INTO directors (name, age) VALUES (?, ?);",
            (director_name, director_age),
        )
        return cur.lastrowid

    director_id = row[0]
    cur.execute(
        "UPDATE directors SET age = ? WHERE id = ?;",
        (director_age, director_id),
    )
    return director_id


def _upsert_platforms(cur, platforms: List[str]) -> List[int]:
    """
    Inserisce (se mancano) le piattaforme e ritorna la lista degli id.
    """
    ids: List[int] = []
    for pname in platforms:
        pname = pname.strip()
        if pname:
            pname = pname[0].upper() + pname[1:].lower()
        if not pname:
            continue

        cur.execute("SELECT id FROM platforms WHERE name = ?;", (pname,))
        row = cur.fetchone()

        if row is None:
            cur.execute("INSERT INTO platforms (name) VALUES (?);", (pname,))
            ids.append(cur.lastrowid)
        else:
            ids.append(row[0])

    return ids


def _upsert_movie(cur, title: str, year: int, genre: str, director_id: int) -> int:
    """
    Inserisce o aggiorna un film basandosi solo sul titolo.
    Ritorna movie_id.
    """
    cur.execute("SELECT id FROM movies WHERE title = ?;", (title,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            "INSERT INTO movies (title, year, genre, director_id) VALUES (?, ?, ?, ?);",
            (title, year, genre, director_id),
        )
        return cur.lastrowid

    movie_id = row[0]
    cur.execute(
        "UPDATE movies SET year = ?, genre = ?, director_id = ? WHERE id = ?;",
        (year, genre, director_id, movie_id),
    )
    return movie_id


def _replace_movie_platforms(cur, movie_id: int, platform_ids: List[int]) -> None:
    """
    Sostituisce tutte le piattaforme associate al film (delete + insert),
    come richiesto dalle specifiche (se cambiano piattaforme, rimpiazza).
    """
    cur.execute("DELETE FROM movie_platforms WHERE movie_id = ?;", (movie_id,))
    for pid in platform_ids:
        cur.execute(
            "INSERT INTO movie_platforms (movie_id, platform_id) VALUES (?, ?);",
            (movie_id, pid),
        )


def _handle_add_parts(parts: list[str]) -> dict:
    if len(parts) != 7:
        raise AddLineFormatError(f"Numero di campi non valido: attesi 7, trovati {len(parts)}")

    title = parts[0].strip()
    director_name = parts[1].strip()
    director_age_str = parts[2].strip()
    year_str = parts[3].strip()
    genre = parts[4].strip()
    platform1 = parts[5].strip()
    platform2 = parts[6].strip()

    if not title or not director_name or not genre:
        raise AddLineFormatError("Titolo, regista e genere non possono essere vuoti.")

    try:
        director_age = int(director_age_str)
        year = int(year_str)
    except ValueError:
        raise AddLineFormatError("Età autore e anno devono essere interi.")

    platforms = [platform1, platform2]

    conn = get_connection()
    try:
        cur = conn.cursor()

        director_id = _upsert_director(cur, director_name, director_age)
        movie_id = _upsert_movie(cur, title, year, genre, director_id)

        platform_ids = _upsert_platforms(cur, platforms)
        _replace_movie_platforms(cur, movie_id, platform_ids)

        conn.commit()
        return {"status": "ok"}
    finally:
        conn.close()


def handle_add(line: str) -> dict:
    # CSV input per /add (virgole)
    parts = [p.strip() for p in line.strip().split(",")]
    return _handle_add_parts(parts)


def handle_add_from_tsv(tsv_line: str) -> dict:
    tsv_line = tsv_line.rstrip("\n")
    if not tsv_line.strip():
        return {"status": "ok"}

    parts = tsv_line.split("\t")

    # Padding fino a 7 campi per gestire piattaforme finali vuote (1 o 2 vuote)
    while len(parts) < 7:
        parts.append("")

    if len(parts) != 7:
        raise AddLineFormatError(
            f"Numero di campi TSV non valido: attesi 7, trovati {len(parts)}"
        )

    return _handle_add_parts(parts)

