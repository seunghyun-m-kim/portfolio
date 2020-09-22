# Data Modeling and ETL Pipeline with Postgres
### Project: Data Modeling with Postgres

This project was completed as part of Udacity's Data Engineering Nanodegree. Data set and prompt provided by [2011–2020 Udacity, Inc.](https://www.udacity.com), used under [CC BY](https://creativecommons.org/licenses/by-nc-nd/3.0/).

---

In this project, we aim to assist a startup called Sparkify in their data analytics of songs and user activity on their new music streaming app. They are particularly interested in understanding what songs users are listening to. Their data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Our goal is to create a Postgres database with tables designed to optimize queries on song play analysis. After planning, the company has decided on the following star schema for their data:

**Fact Table**

1. **songplays**

    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

2. **users**
    - user_id, first_name, last_name, gender, level

3. **songs**
    - song_id, title, artist_id, year, duration

4. **artists**
    - artist_id, name, location, latitude, longitude

5. **time**
    - start_time, hour, day, week, month, year, weekday

To construct the tables, our tasks include creating empty tables, building a Postgres ETL pipeline, and running the pipeline for data extraction and insertion.

---
### Install

- This project requires Python 3 (project was built on version 3.6.3) with `os`, `glob`, `psycopg2`, and `pandas` libraries installed. Install via [Anaconda Distribution platform](https://www.anaconda.com/).
- Jupyter Notebook is optional. Install via [Anaconda Distribution platform](https://www.anaconda.com/).   

---
### Run

- Clone this repo to your computer.
- Unzip `data.zip` and place the `data` folder in the repo.
- Launch a Postgres server with local API access enabled.
- Open `conf.cfg` and enter the credentials.
- In a terminal or command window, navigate to the top-level repo directory.
- Run `python create_tables.py`.
- Run `python etl.py`.
- In Jupyter Notebook, open `test.ipynb`, enter the credentials, and run all cells to check results.

---
### Files

- `data.zip` : Compression containing JSON data files
- `conf.cfg` : Configuration script containing the Postgres server credentials.
- `create_tables.py` : Command script creating Sparkify database and empty tables inside.
- `etl.py` : Command script extracting data from JSON files and inserting them into the created tables.
- `sql_queries.py` : Support script containing quries to drop tables, create tables, and insert data.
- `test.ipynb` : SQL script to check that all tables are constructed properly.
- `etl.ipynb` : Preview of `etl.py`. Notebook containing ETL pipeline instructions.
