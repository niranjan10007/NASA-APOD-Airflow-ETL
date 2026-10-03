# NASA APOD API → Airflow → PostgreSQL ETL Pipeline

An end-to-end data engineering project that extracts astronomy data from NASA's Astronomy Picture of the Day (APOD) API, orchestrates the ETL workflow using Apache Airflow, and stores the processed data in PostgreSQL for querying and analysis.

## 🚀 Project Overview

This project demonstrates a practical ETL pipeline using:

**NASA APOD API → Apache Airflow → Python Transformation → PostgreSQL**

The pipeline automatically:

1. Extracts APOD data from NASA's public API.
2. Passes the API response through an Airflow workflow.
3. Transforms and prepares the required fields using Python.
4. Loads the processed data into PostgreSQL.
5. Stores the data in a structured relational table for further analysis.

The project was developed using **Astro CLI** to run Apache Airflow locally with Docker.

---

## 🏗️ Architecture

```text
                 NASA APOD API
                       │
                       │ HTTP Request
                       ▼
              ┌──────────────────┐
              │  Apache Airflow  │
              │   HTTP Operator  │
              └────────┬─────────┘
                       │
                       │ API Response
                       ▼
              ┌──────────────────┐
              │ Python Transform │
              │   & Validation   │
              └────────┬─────────┘
                       │
                       │ Processed Data
                       ▼
              ┌──────────────────┐
              │    PostgreSQL    │
              │    apod_data     │
              └────────┬─────────┘
                       │
                       ▼
                  DBeaver / SQL
```

---

## 🔄 ETL Workflow

### 1. Extract

The pipeline sends an HTTP request to the NASA APOD API and retrieves the daily astronomy content.

The API response contains information such as:

* Date
* Title
* Explanation
* Media type
* Media URL
* Additional APOD metadata

The NASA API key is stored securely in an **Airflow Connection** rather than hardcoded in the DAG.

---

### 2. Transform

The extracted API response is processed using Python.

The transformation step prepares the required fields before loading them into PostgreSQL.

Example fields include:

```text
date
title
explanation
url
media_type
```

---

### 3. Load

The transformed data is inserted into a PostgreSQL table:

```text
public.apod_data
```

PostgreSQL provides a structured destination where the APOD data can be queried using SQL.

---

## 🛠️ Technologies Used

| Technology     | Purpose                                      |
| -------------- | -------------------------------------------- |
| Python         | Data processing and transformation           |
| Apache Airflow | Workflow orchestration                       |
| Astro CLI      | Local Airflow development environment        |
| Docker         | Containerized Airflow/PostgreSQL environment |
| NASA APOD API  | Data source                                  |
| PostgreSQL     | Data storage                                 |
| DBeaver        | Database inspection and SQL querying         |
| Git            | Version control                              |
| GitHub         | Source-code hosting                          |

---

## 📁 Project Structure

```text
NASA-APOD-Airflow-ETL/
│
├── dags/
│   ├── .airflowignore
│   └── etl.py
│
├── .dockerignore
├── .gitignore
├── Dockerfile
├── packages.txt
├── requirements.txt
└── README.md
```

### Important files

**`dags/etl.py`**

Contains the Airflow DAG and ETL workflow.

**`requirements.txt`**

Contains the Python dependencies required by the project.

**`Dockerfile`**

Defines the Docker environment used by the Airflow project.

**`.gitignore`**

Prevents local configuration files, secrets, and generated files from being committed to Git.

---

## ⚙️ Local Setup

### Prerequisites

Install the following:

* Docker Desktop
* Astro CLI
* Git
* Python
* DBeaver (optional, for database inspection)

---

### 1. Clone the repository

```bash
git clone https://github.com/niranjan10007/NASA-APOD-Airflow-ETL.git
cd NASA-APOD-Airflow-ETL
```

---

### 2. Start the Airflow environment

Run:

```bash
astro dev start
```

This starts the local Airflow environment using Docker.

Open the Airflow UI from the URL provided by Astro CLI.

---

## 🔐 Airflow Connection Configuration

The NASA API key is **not stored directly in the source code**.

Create an Airflow connection named:

```text
nasa_api
```

Configure the connection so the API key is stored in the connection's Extra field.

Example:

```json
{
  "api_key": "YOUR_NASA_API_KEY"
}
```

Replace `YOUR_NASA_API_KEY` with your actual NASA API key.

### Security

Do **not** commit the actual API key to GitHub.

The project accesses the key through the Airflow connection instead of hardcoding it in the DAG.

---

## 🗄️ PostgreSQL

The pipeline loads the processed APOD records into:

```text
Database: postgres
Schema: public
Table: apod_data
```

You can inspect the data using SQL:

```sql
SELECT *
FROM public.apod_data;
```

To view the latest records:

```sql
SELECT *
FROM public.apod_data
ORDER BY date DESC;
```

---

## ▶️ Running the Pipeline

1. Start the Astro environment:

```bash
astro dev start
```

2. Open the Airflow UI.

3. Locate the NASA APOD ETL DAG.

4. Enable/unpause the DAG if required.

5. Trigger the DAG manually.

6. Monitor the task execution in Airflow.

7. Verify the loaded records in PostgreSQL.

---

## 📊 Example Data Flow

```text
NASA APOD API
      ↓
HTTP Request
      ↓
JSON Response
      ↓
Python Processing
      ↓
Structured APOD Record
      ↓
PostgreSQL
      ↓
SQL Analysis
```

---

## 🧠 Data Engineering Concepts Demonstrated

This project demonstrates practical understanding of:

* ETL pipeline development
* REST API integration
* HTTP-based data extraction
* Airflow DAG development
* Airflow operators
* Workflow orchestration
* Python-based transformation
* PostgreSQL data loading
* SQL querying
* Docker-based development
* Environment and secret management
* Git version control
* GitHub repository management

---

## 🔒 Security & Configuration

Sensitive and local configuration files are excluded from version control.

The project does not commit:

```text
.env
airflow_settings.yaml
.astro/
```

API credentials should always be stored using Airflow Connections or another secure secrets-management solution.

---

## 🔮 Future Improvements

Potential extensions for this project include:

* Add incremental loading and duplicate handling.
* Add data-quality validation.
* Add retry and failure-handling mechanisms.
* Add logging and monitoring.
* Add automated tests.
* Add a reporting/dashboard layer.
* Deploy the Airflow pipeline to a cloud environment.
* Store historical APOD data for long-term analysis.

---

## 👨‍💻 Author

**Niranjan Barhate**

Data Engineer | Python | SQL | PySpark | Apache Spark | Kafka | Airflow | ETL/ELT

GitHub:
https://github.com/niranjan10007

LinkedIn:
https://www.linkedin.com/in/niranjan-barhate-756797308/

---

## ⭐ Project

If you find this project useful for learning data engineering, feel free to explore the repository and follow the development of the pipeline.
