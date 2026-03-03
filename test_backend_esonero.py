"""
NON CAMBIARE QUESTO SCRIPT DI TEST.
USEREMO ESATTAMENTE QUESTO SCRIPT PER VALIDARE IL VOSTRO CODICE
(+ un breve test manuale del frontend web).
DOVETE ESEGUIRE QUESTO TEST DA FUORI DAL DOCKER.
"""
from typing import Set
from urllib.parse import quote, urljoin
import requests
import copy

class BackendTester():
    def __init__(self, url: str):
        self.url = url

        self.questions_text = """Elenca i film del <ANNO>.
Quali sono i registi presenti su Netflix?
Elenca tutti i film di fantascienza.
Quali film sono stati fatti da un regista di almeno <ANNI> anni?
Quali registi hanno fatto più di un film?"""

        self.questions = self.questions_text.splitlines(keepends=False)

        # Store original expected results (matching 's initial state)
        self.original_expected_directors_netflix = {
            "Bong Joon-ho", "Christopher Nolan", "David Fincher", "Ridley Scott",
            "Martin Scorsese", "Robert Zemeckis", "Francis Ford Coppola",
            "Lana Wachowski", "Hayao Miyazaki", "Quentin Tarantino",
            "Damien Chazelle", "Todd Phillips", "George Miller", "Denis Villeneuve"
        }
        self.original_expected_multi_film_directors = {
            "Christopher Nolan", "David Fincher", "Martin Scorsese",
            "Quentin Tarantino", "Robert Zemeckis", "Steven Spielberg",
            "Peter Jackson", "Denis Villeneuve"
        }
        self.expected_scifi_films = { # This one doesn't change in the test after adding
            "Inception", "Interstellar", "Star Wars: A New Hope", "The Matrix",
            "Back to the Future", "Blade Runner 2049", "Arrival"
        }

    def test_database_schema_format(self) -> None:
        urlpath = urljoin(self.url, "schema_summary")
        response = requests.get(urlpath)
        assert response.status_code == 200, f"Expected status code 200 but got {response.status_code} for URL {urlpath}"
        result = response.json()

        assert isinstance(result, list), f"Expected result type list but got {type(result)}"
        for index, column_dict in enumerate(result):
            assert isinstance(column_dict, dict), f"Expected each element to be a dict but element at index {index} is {type(column_dict)}"
            assert column_dict.get("table_name") is not None, f"Each dictionary of the schema summary must have a 'table_name', got: {column_dict}"
            assert column_dict.get("table_column") is not None, f"Each dictionary of the schema summary must have a 'table_column', got: {column_dict}"

        table_names = [x["table_name"] for x in result]
        assert "movies" in table_names, (
            f"Expected one of the tables to be 'movies' but got table names: {table_names}. "
            "You can use other tables if you want."
        )
        print(f"PASS: the database schema summary is correctly formatted and contains the 'movies' table.\n")

    def test_invalid_question(self, question: str) -> None:
        urlpath = urljoin(self.url, f"search/{quote(question)}")
        print(f"  GET {urlpath}")
        response = requests.get(urlpath)
        assert response.status_code == 422, f"After asking the invalid question '{question}', expected 422 error code but got: {response.status_code} {response.text}."
        print(f"PASS: after asking invalid question '{question}', correctly got 422 error code.\n")

    def _run_search_and_extract(self, question: str, expected_item_type: str) -> set:
        """
        Executes the search request for a given question and verifies that each item has the expected item_type.
        It extracts the values for the property "name" from each returned item.
        """
        urlpath = urljoin(self.url, f"search/{quote(question)}")
        print(f"  GET {urlpath}")
        response = requests.get(urlpath)
        if response.status_code == 422:
             raise AssertionError(
                 f"Request failed for question: '{question}'. Got status code 422 (Unprocessable Entity). "
                 f"This likely means the backend doesn't recognize this exact question string. Response: {response.text}"
             )
        assert response.status_code == 200, (
            f"Request failed for question: '{question}'. Expected status code 200 but got {response.status_code}. Response: {response.text}"
        )
        result = response.json()
        assert isinstance(result, list), (
            f"Expected result to be of type list but got {type(result)} for question: '{question}'"
        )

        names = set()
        for idx, item in enumerate(result):
            assert isinstance(item, dict), (
                f"Expected each item to be a dict but got {type(item)} at index {idx} for question: '{question}'"
            )
            actual_item_type = item.get("item_type")
            assert actual_item_type == expected_item_type, (
                f"Expected item_type to be '{expected_item_type}' but got '{actual_item_type}' "
                f"at index {idx} for question: '{question}'"
            )
            properties = item.get("properties", [])
            item_name = None
            for prop in properties:
                if prop.get("property_name") == "name":
                    item_name = prop.get("property_value")
                    names.add(item_name)
                    break
            assert item_name is not None, f"Item at index {idx} missing 'name' property for question: '{question}'"

        print(f"  -> Extracted names: {names}")
        return names

    def test_question_1_movies_of_year(self, year: str, expected_titles: Set[str]) -> None:
        print(f"Testing: Question 1 - Movies of year {year}...")
        question = self.questions[0].replace("<ANNO>", year)
        returned_titles = self._run_search_and_extract(question, "film")
        assert returned_titles == expected_titles, (
            f"FAIL: For year {year}, expected movie titles {expected_titles} but got {returned_titles}"
        )
        print(f"PASS: Question 1 - Movies of year {year}.\n")

    def test_question_2_directors_on_netflix(self, expected_directors: Set[str]) -> None:
        """ Test for the question "Quali sono i registi presenti su Netflix?". """
        print("Testing: Question 2 - Directors on Netflix...")
        question = self.questions[1]
        returned_directors = self._run_search_and_extract(question, "director")
        assert returned_directors == expected_directors, (
            f"FAIL: For question: '{question}', expected directors {expected_directors} but got {returned_directors}"
        )
        print("PASS: Question 2 - Directors on Netflix.\n")

    def test_question_3_science_fiction(self) -> None:
        """ Test for the question "Elenca tutti i film di fantascienza." """
        print("Testing: Question 3 - Science Fiction Movies...")
        question = self.questions[2]
        returned_films = self._run_search_and_extract(question, "film")
        assert returned_films == self.expected_scifi_films, f"FAIL: For question: '{question}', expected films {self.expected_scifi_films} but got {returned_films}"
        print("PASS: Question 3 - Science Fiction Movies.\n")

    def test_question_4_director_age(self, threshold: str, expected_titles: Set[str]) -> None:
        """ Test per la domanda "Quali film sono stati fatti da un regista di almeno <ANNI> anni?" """
        print(f"Testing: Question 4 - Director Age >= {threshold}...")
        question = self.questions[3].replace("<ANNI>", threshold)
        returned_titles = self._run_search_and_extract(question, "film")
        assert returned_titles == expected_titles, (
            f"FAIL: For threshold {threshold}, expected movie titles {expected_titles} but got {returned_titles}"
        )
        print(f"PASS: Question 4 - Director Age >= {threshold}.\n")

    def test_question_5_multiple_films_directors(self, expected_directors: Set[str]) -> None:
        """ Test per la domanda "Quali registi hanno fatto più di un film?" """
        print("Testing: Question 5 - Directors with Multiple Films...")
        question = self.questions[4]
        returned_directors = self._run_search_and_extract(question, "director")
        assert returned_directors == expected_directors, (
            f"FAIL: For question: '{question}', expected directors {expected_directors} but got {returned_directors}"
        )
        print("PASS: Question 5 - Directors with Multiple Films.\n")

    def add_movie_line(self, data_line_string: str) -> None:
        """
        Sends the pre-formatted comma-separated data_line string to the POST /add endpoint.
        The expected format is: Titolo,Regista,Età_Autore,Anno,Genere,Piattaforma_1,Piattaforma_2
        Con Piattaforma_1 e Piattaforma_2 che possono essere 
        """
        urlpath = urljoin(self.url, "add") # Use the /add endpoint

        # Prepare the JSON payload: {"data_line": "..."}
        # The input argument 'data_line_string' is used directly
        payload = {"data_line": data_line_string}

        print(f"  POST {urlpath} with JSON body: {payload}")
        response = requests.post(urlpath, json=payload) # Send payload as JSON

        # Check for success (200 OK according to spec example, or 422 error)
        if response.status_code == 422:
             raise AssertionError(
                 f"Failed to add movie line '{data_line_string}'. Expected status code 200 but got 422. "
                 f"This might indicate an invalid format according to the backend. Response: {response.text}"
             )
        assert response.status_code == 200, (
            f"Failed to add movie line '{data_line_string}'. Expected status code 200 but got {response.status_code}. "
            f"Response: {response.text}"
        )
        # Check the response body matches {"status": "ok"}
        try:
            response_json = response.json()
            assert response_json == {"status": "ok"}, \
                f"Expected response body {{'status': 'ok'}} but got {response_json}"
        except requests.exceptions.JSONDecodeError:
            raise AssertionError(f"Response for adding movie line '{data_line_string}' was not valid JSON. Response text: {response.text}")

        print(f"  -> backend acknowledged add request for line: '{data_line_string}' (Status: {response.status_code}).\n")

    def test_add_movie_of_frank_darabont(self) -> None:
        """Adds 'The Green Mile' (another Frank Darabont film) via the /add endpoint."""
        print("Testing: Add Movie - The Shawshank Redemption (/add)...")
        # Format: Titolo,Regista,Età_Autore,Anno,Genere,Piattaforma_1,Piattaforma_2
        data_string = "The Green Mile,Frank Darabont,65,1999,Drama,Netflix,"
        self.add_movie_line(data_string)
        print("PASS: Add Movie - The Shawshank Redemption (/add).\n")

    def test_add_movie_of_2010_year(self) -> None:
        """Adds 'The Social Network' from 2010 via the /add endpoint."""
        print("Testing: Add Movie - The Social Network (/add)...")
        # Format: Titolo,Regista,Età_Autore,Anno,Genere,Piattaforma_1,Piattaforma_2
        data_string = "The Social Network,David Fincher,62,2010,Drama,Netflix,Amazon Prime Video"
        self.add_movie_line(data_string)
        print("PASS: Add Movie - The Social Network (/add).\n")

    def test_invalid_add(self, wrong_add_line: str):
        """Tests sending an invalidly formatted line to the /add endpoint."""
        print("Testing: Invalid Add Movie Line (/add)...")
        invalid_data_string = wrong_add_line # Example of an invalid format
        urlpath = urljoin(self.url, "add")
        payload = {"data_line": invalid_data_string}
        print(f"  POST {urlpath} with JSON body: {payload}")
        response = requests.post(urlpath, json=payload)
        assert response.status_code == 422, f"After sending invalid data line '{invalid_data_string}', expected 422 error code but got: {response.status_code} {response.text}."
        print(f"PASS: after sending invalid data line '{invalid_data_string}', correctly got 422 error code.\n")


def main():
    URL = "http://localhost:8003"
    assert URL == "http://localhost:8003", "Do not change the URL, you must expose the backend on port 8003"
    tester = BackendTester(URL)

    print("--- Phase 1: Testing Initial State ---")
    tester.test_database_schema_format()

    # Test Q1: Movies of Year (Initial)
    original_2010_movies = {"Inception", "Shutter Island"}
    tester.test_question_1_movies_of_year("2010", original_2010_movies)
    tester.test_question_1_movies_of_year("1998", {"Saving Private Ryan"})

    # Test Q2: Directors on Netflix (Initial)
    tester.test_question_2_directors_on_netflix(tester.original_expected_directors_netflix)

    # Test Q3: Sci-Fi Movies (Initial - should not change)
    tester.test_question_3_science_fiction()

    # Test Q4: Director Age (Initial - should not change)
    expected_films_threshold_80 = {
        "Gladiator", "Shutter Island", "Star Wars: A New Hope", "The Godfather",
        "Goodfellas", "Spirited Away", "Mad Max: Fury Road", "The Wolf of Wall Street"
    }
    tester.test_question_4_director_age("80", expected_films_threshold_80)
    expected_films_threshold_85 = {"Gladiator", "The Godfather"}
    tester.test_question_4_director_age("85", expected_films_threshold_85)

    # Test Q5: Multiple Films Directors (Initial)
    tester.test_question_5_multiple_films_directors(tester.original_expected_multi_film_directors)
    
    tester.test_invalid_question("Domanda strana")
    
    print("\n--- Phase 2: Adding New Movies (via /add endpoint) ---")
    tester.test_add_movie_of_frank_darabont() 
    tester.test_add_movie_of_2010_year()
    tester.test_invalid_add("hello, world") # too short
    tester.test_invalid_add("a,b,c,d,e,f,g,h,i,l,m,n,o,p,q") # too long

    print("\n--- Phase 3: Testing State After Additions ---")

    # Re-test Q1: Movies of Year 2010 (should now include The Social Network)
    updated_expected_titles_2010 = copy.deepcopy(original_2010_movies)
    updated_expected_titles_2010.add("The Social Network")
    tester.test_question_1_movies_of_year("2010", updated_expected_titles_2010)

    # Re-test Q2: Directors on Netflix (should now include Frank Darabont)
    updated_netflix_directors = copy.deepcopy(tester.original_expected_directors_netflix)
    updated_netflix_directors.add("Frank Darabont")
    tester.test_question_2_directors_on_netflix(updated_netflix_directors)

    # Re-test Q5: Multiple Films Directors (should now include Frank Darabont)
    updated_multi_film_directors = copy.deepcopy(tester.original_expected_multi_film_directors)
    updated_multi_film_directors.add("Frank Darabont")
    tester.test_question_5_multiple_films_directors(updated_multi_film_directors)

    # Optional: Re-test Q3 and Q4 to ensure they haven't changed (they shouldn't have given the additions made before)
    print("\n--- Re-testing Unchanged Queries (Sanity Check) ---")
    tester.test_question_3_science_fiction()
    tester.test_question_4_director_age("80", expected_films_threshold_80)
    tester.test_question_4_director_age("85", expected_films_threshold_85)


    print("\n-----------------------------------------------------")
    print("ALL TESTS PASS!")
    print("(Check manually the frontend webpage UI before handing in the midterm)")
    print("-----------------------------------------------------")

if __name__ == "__main__":
    main()