# Finance · Sales · Ops Analytics Platform  
**End-to-End Data Engineering & Analytics Platform with Airflow, dbt, PostgreSQL & Power BI**

---

## 🚀 Project Overview

This project is a **realistic, end-to-end analytics engineering platform** that simulates how modern companies build **Finance, Sales, and Operations analytics** from raw data to business-ready metrics.

It is designed to mirror **real production workflows**, not toy examples.

**Core goals:**
- Orchestrate daily ETL pipelines using **Apache Airflow**
- Transform analytics models using **dbt**
- Store data in **PostgreSQL**
- Serve analytics-ready datasets for **BI & decision-making**

---

## 🧱 Architecture Overview
# Finance · Sales · Ops Analytics Platform  
**End-to-End Data Engineering & Analytics Platform with Airflow, dbt, PostgreSQL & Power BI**

---

## 🚀 Project Overview

This project is a **realistic, end-to-end analytics engineering platform** that simulates how modern companies build **Finance, Sales, and Operations analytics** from raw data to business-ready metrics.

It is designed to mirror **real production workflows**, not toy examples.

**Core goals:**
- Orchestrate daily ETL pipelines using **Apache Airflow**
- Transform analytics models using **dbt**
- Store data in **PostgreSQL**
- Serve analytics-ready datasets for **BI & decision-making**

---

## 🧱 Architecture Overview
Mock Data (CSV, Daily)
↓
Airflow DAG (Daily Schedule)
↓
Python ETL (Sales / Ops / Finance)
↓
PostgreSQL (Staging + Fact Tables)
↓
dbt (Analytics Models & Tests)
↓
BI Layer (Power BI / SQL / Analytics)

This architecture closely reflects how **Finance Business Partners, Analytics Engineers, and Data Engineers** collaborate in real companies.

---

## 🛠️ Tech Stack

| Layer | Technology |
|-----|-----------|
| Orchestration | Apache Airflow (Dockerized) |
| ETL | Python |
| Data Warehouse | PostgreSQL |
| Transformations | dbt |
| Containerization | Docker & Docker Compose |
| Analytics / BI | Power BI / SQL |
| Environment | GitHub Codespaces / Local Docker |

---

## 📂 Project Structure
Finance-Sales-Ops-Analytics-Platform
├── dags/ # Airflow DAG definitions
│ └── etl_full_pipeline_daily.py
├── database/
│ ├── mock_data/ # Daily generated CSV mock data
│ └── etl/ # Python ETL scripts
├── analytics_platform/ # dbt project
│ ├── models/
│ ├── packages.yml
│ └── dbt_project.yml
├── docker-compose.airflow.yml # Airflow Docker Compose
├── Dockerfile.airflow # Custom Airflow image (dbt, git, deps)
├── README.md # Project documentation
└── .env # Environment variables (not committed)

---

## 🔄 Data Pipeline Flow

### 1️⃣ Mock Data Generation
- Daily mock CSV data is generated to simulate real transactional systems.
- Date-partitioned folders (`YYYYMMDD`) are created automatically.

### 2️⃣ ETL Processing (Python)
Each domain has its own ETL logic:
- **Sales ETL**
- **Operations ETL**
- **Finance ETL**

These scripts:
- Validate input data
- Load data into PostgreSQL
- Maintain consistent schemas

### 3️⃣ Analytics Modeling (dbt)
- dbt models transform raw facts into analytics-ready tables
- Includes:
  - Fact & dimension models
  - Tests & dependencies
  - Reusable macros via `dbt_utils`

### 4️⃣ BI Consumption
- Final models are ready for:
  - Power BI
  - SQL analytics
  - Finance & Ops reporting

---

## ⏱️ Airflow DAG

- **DAG Name:** `etl_full_pipeline_daily`
- **Schedule:** `@daily`
- **Execution Order:**

generate_mock_data
→ etl_sales
→ etl_operations
→ etl_finance
→ dbt_build

All tasks must succeed before downstream tasks execute, ensuring data consistency.

---

## 🐳 Running the Project (Docker)

### Prerequisites
- Docker & Docker Compose
- GitHub Codespaces or local Docker environment

### Start Airflow
```bash
docker compose -f docker-compose.airflow.yml up --build

Access Airflow UI
http://localhost:8080

Default credentials
Username: admin
Password: admin

✅ Project Status

✔ End-to-end pipeline implemented
✔ Dockerized Airflow environment
✔ dbt integration with Airflow
✔ Daily scheduled ETL
✔ Analytics-ready output

This project is production-inspired, intentionally designed to demonstrate:

Data orchestration

Analytics modeling

Cross-domain data design

Real-world failure handling & retries

🎯 Why This Project Matters

Unlike simple demos, this platform demonstrates:

How analytics pipelines behave over time

How Finance, Sales, and Ops data intersect

How orchestration + transformation work together

How analytics engineers think in systems, not scripts

📌 Next Enhancements (Planned)

CI/CD for dbt tests

Cloud deployment (Azure / AWS)

Data quality monitoring

Role-based access control

Production-grade secrets management

👤 Author

Built by Keeon

Background: Software Engineering → Analytics Engineering / Finance BI

Focus: Real-world data platforms, not toy projects

📖 Detailed architecture notes and design decisions are documented in the GitHub Wiki.