# Phase 5 – ML Forecasting Pipeline (Sales / Inventory / Profit)

## 1. Objective

The goal of Phase 5 is to build an **end-to-end, production-style ML forecasting pipeline** that connects:

- Finance / Sales / Ops data modeling
- Machine learning training & inference
- Workflow orchestration
- Experiment tracking
- Business-facing consumption (Power BI)

This phase focuses on **monthly forecasting** for:
- Sales
- Inventory
- Profit

The emphasis is not model complexity, but **data correctness, reproducibility, observability, and business usability**.

---

## 2. High-Level Architecture
PostgreSQL (Raw Facts)
↓
dbt (ML Training Datasets)
↓
Python ML Training Scripts
↓
MLflow (Experiments & Artifacts)
↓
Batch Prediction Scripts
↓
PostgreSQL (Prediction Tables)
↓
Power BI (Actual vs Forecast)

---

## 3. Data Modeling (dbt)

### 3.1 Training Datasets

All ML training datasets are **dbt-managed and versioned**.

Location: analytics_platform/models/ml/

Models:
- `ml_train_sales_monthly.sql`
- `ml_train_inventory_monthly.sql`
- `ml_train_profit_monthly.sql`

These models:
- Aggregate raw fact tables into **monthly grain**
- Apply consistent business logic
- Serve as the **single source of truth** for ML training

This design ensures:
- Reproducibility
- Clear lineage
- Separation of data modeling and ML logic

---

### 3.2 Why dbt for ML Data?

Using dbt for ML feature datasets allows:
- SQL-based transparency for business users
- Git versioning of training data logic
- Easy backfills and audits
- Alignment with existing analytics modeling practices

---

## 4. Machine Learning Training

### 4.1 Model Type

For Phase 5, **baseline linear regression models** are used:
- Simple
- Interpretable
- Fast to train
- Suitable for demonstrating pipeline design

Model scripts are located in: analytics_platform/ml/

Each domain (Sales / Inventory / Profit) has:
- Feature preparation
- Model training
- Evaluation
- Artifact logging

---

### 4.2 MLflow Tracking

MLflow is used to track:
- Model parameters
- Metrics (e.g. RMSE)
- Artifacts (models, predictions)

Artifacts include:
- Serialized models
- Batch prediction CSV outputs

All experiments are stored locally under: analytics_platform/mlruns/

---

## 5. Batch Prediction

After training, batch inference is executed to generate **1-month-ahead forecasts**.

Outputs:
- `ml_pred_sales_1m`
- `ml_pred_inventory_1m`
- `ml_pred_profit_1m`

Predictions are written back into PostgreSQL tables, enabling:
- SQL access
- BI consumption
- Historical comparison

---

## 6. Orchestration (Airflow)

### 6.1 DAG Design

An Airflow DAG orchestrates the full pipeline:

1. Train models (Sales / Inventory / Profit)
2. Log experiments to MLflow
3. Run batch predictions
4. Persist results

Key characteristics:
- Clear task grouping
- Deterministic execution order
- Manual or scheduled runs

This mirrors real-world MLOps orchestration patterns.

---

## 7. Business Consumption (Power BI)

### 7.1 Forecast Tables

Power BI imports prediction tables:
- `PredSales1m`
- `PredInventory1m`
- `PredProfit1m`

Each table contains:
- Date
- Region / Country identifiers
- Predicted values
- Model metadata (model_name, run_id, prediction_time)

---

### 7.2 Forecast vs Actual Analysis

The Power BI report includes:
- KPI cards (Actual vs Forecast)
- Variance %
- Monthly trend comparisons

This allows business users to:
- Evaluate forecast accuracy
- Detect trend divergence
- Support planning & decision-making

---

## 8. Key Design Decisions

- **dbt for training data** instead of ad-hoc SQL
- **Simple models first**, pipeline correctness over model complexity
- **MLflow for observability**, not just model storage
- **Database-first predictions**, not file-based BI integration

---

## 9. Assumptions

- Monthly aggregation is sufficient for planning use cases
- Historical patterns are predictive of short-term future
- Baseline models provide enough signal for demonstration
- Data quality from upstream pipelines is reliable

---

## 10. Limitations

- No hyperparameter tuning
- No automated model selection
- No model monitoring or drift detection
- Single-horizon forecasting only (1 month)

These are **intentional trade-offs** to keep Phase 5 focused on architecture.

---

## 11. What’s Next (Phase 6 Ideas)

Potential extensions:
- Multi-horizon forecasting (3M / 6M)
- Model comparison and promotion logic
- Automated retraining triggers
- Forecast accuracy tracking
- Deployment to cloud ML services

---

## 12. Summary

Phase 5 demonstrates how Finance, Sales, and Operations data can be transformed into **actionable forecasts** using modern analytics and MLOps practices.

The result is not just a model, but a **reproducible, auditable, and business-ready forecasting system**.