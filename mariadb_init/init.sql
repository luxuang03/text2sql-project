CREATE DATABASE IF NOT EXISTS movies_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE movies_db;

DROP TABLE IF EXISTS movie_platforms;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS platforms;
DROP TABLE IF EXISTS directors;

CREATE TABLE directors (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL UNIQUE,
    eta  INT NOT NULL
);

CREATE TABLE movies (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    titolo      VARCHAR(255) NOT NULL,
    anno        INT NOT NULL,
    genere      VARCHAR(100) NOT NULL,
    regista_id  INT NOT NULL,
    CONSTRAINT fk_movies_director
        FOREIGN KEY (regista_id) REFERENCES directors(id)
);

CREATE TABLE platforms (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE movie_platforms (
    movie_id    INT NOT NULL,
    platform_id INT NOT NULL,
    PRIMARY KEY (movie_id, platform_id),
    CONSTRAINT fk_mp_movie
        FOREIGN KEY (movie_id) REFERENCES movies(id),
    CONSTRAINT fk_mp_platform
        FOREIGN KEY (platform_id) REFERENCES platforms(id)
);