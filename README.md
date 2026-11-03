# API to Postgres ETL

This project demonstrates how to build a simple ETL pipeline using Apache Airflow, an open-source orchestration tool, written in Python, used to programmatically author, schedule, and monitor workflows.

## Project Goals

1. **Data Ingestion** - fetch users data from an API (*[JSONplacehdoler](https://www.google.com)*).
2. **Data Transformation** - perform simple transformation to clean and standardized the data we've just collected.
3. **Data Storage** - use PostgresDB to store the data.
4. **Data Pipeline** - build the ETL pipeline using Apache Airflow.
5. **Pipeline Automation** - automate the execution of your ETL pipeline by incorporating `start_date` and `schedule` into your DAG file.

## ETL Architecture

![api-to-postgres-etl.png](images\api-to-postgres-etl.png)

## Dataset Used in this Project 

In this project, we are building a data pipeline that extracts data through an API call to [JSONplacehdoler](https://www.google.com). API calls are made using the `requests` module:

```python
>>> r = requests.get(https://jsonplaceholder.typicode.com/<resource>)
```

The resulting data is in JSON format, as shown below:

```json
{
    "id": 1,
    "name": "Leanne Graham",
    "username": "Bret",
    "email": "Sincere@april.biz",
    "address": {
      "street": "Kulas Light",
      "suite": "Apt. 556",
      "city": "Gwenborough",
      "zipcode": "92998-3874",
      "geo": {
        "lat": "-37.3159",
        "lng": "81.1496"
      }
    },
    "phone": "1-770-736-8031 x56442",
    "website": "hildegard.org",
    "company": {
      "name": "Romaguera-Crona",
      "catchPhrase": "Multi-layered client-server neural-net",
      "bs": "harness real-time e-markets"
    }
  },
  ...
```

## Tools used in this project

* **Apache Airflow** - an open-source workflow management platform used to programmatically author, schedule, and monitor data engineering and batch processing using Python.
* **Docker** - an open-sourcs virtualization software that packages applications and their dependencies into lightweight, portable units called containers.
* **Python** - a high-level, general-purpose programming language known for its versatility, readability, simplicity and extensive ecosystem.