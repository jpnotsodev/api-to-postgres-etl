CREATE DATABASE testdb;

\c testdb;

CREATE TABLE public.users (
id INTEGER PRIMARY KEY,
name TEXT,
username TEXT,
email TEXT,
phone TEXT,
website TEXT
);