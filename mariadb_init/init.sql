-- init.sql
-- Script di inizializzazione del database

-- (facoltativo) crea il database, il collega su macOS potrà usare lo stesso nome
CREATE DATABASE IF NOT EXISTS movies_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE movies_db;

-- Elimino le tabelle se esistono già 
DROP TABLE IF EXISTS movie_platforms;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS platforms;
DROP TABLE IF EXISTS directors;

-- Tabella dei registi
CREATE TABLE directors (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    age  INT NOT NULL
);

-- Tabella dei film 
CREATE TABLE movies (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    year        INT NOT NULL,
    genre       VARCHAR(100) NOT NULL,
    director_id INT NOT NULL,
    CONSTRAINT fk_movies_director
        FOREIGN KEY (director_id) REFERENCES directors(id)
);

-- Tabella delle piattaforme (Netflix, Amazon Prime Video, NOW, ...)
CREATE TABLE platforms (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Tabella di relazione molti-a-molti fra film e piattaforme
CREATE TABLE movie_platforms (
    movie_id    INT NOT NULL,
    platform_id INT NOT NULL,
    PRIMARY KEY (movie_id, platform_id),
    CONSTRAINT fk_mp_movie
        FOREIGN KEY (movie_id) REFERENCES movies(id),
    CONSTRAINT fk_mp_platform
        FOREIGN KEY (platform_id) REFERENCES platforms(id)
);
