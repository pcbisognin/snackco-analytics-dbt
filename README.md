# SnackCo Trade Analytics

## Objective

This project implements an end-to-end analytics pipeline for a fictional snack brand, focusing on sales performance, stock availability, and operational insights across stores and products.
The goal is to transform raw operational data into a tested, well-modeled analytical layer ready for BI and decision-making.

---

## Data Architecture

The pipeline follows a modern analytics stack:

**Cloud Storage → BigQuery (raw) → dbt (staging → marts)**

* **Raw**: source data loaded into BigQuery without transformations
* **Staging**: lightweight transformations and data type standardization (views)
* **Marts**: dimensional models (tables) optimized for analytics and reporting

---

## Modeling Approach

### Staging Layer

* Implemented as **views**
* Responsible for:

  * data type casting
  * column selection and naming
  * basic data validation
* No business logic or aggregations

### Marts Layer

* Implemented as **tables**
* Dimensional (star schema) modeling
* Clear separation between:

  * **Dimensions** (descriptive attributes)
  * **Facts** (events and measurable values)

---

## Data Models

### Dimensions

* **dim_date** – calendar dimension generated from sales and stock date ranges
* **dim_product** – product catalog with stable surrogate keys
* **dim_store** – store attributes and classification

### Facts

* **fact_sales** – transactional sales events (one row per sale)
* **fact_stock_daily** – daily stock state per product and store

---

## Key Design Decisions

### Surrogate Keys

Natural identifiers from source systems are converted into **deterministic surrogate keys** using hashing.
This ensures:

* stable joins across reprocessings
* independence from source-system IDs
* readiness for future Slowly Changing Dimensions (SCD) without refactoring facts

### Materialization Strategy

* **Views** in staging for always-up-to-date transformations
* **Tables** in marts for performance, consistency, and controlled refreshes

---

## Data Quality & Testing

Data quality is enforced using dbt tests:

* **Uniqueness and non-null constraints** on dimension keys
* **Referential integrity tests** between facts and dimensions
* Validation of mandatory fields and basic domain constraints

All staging and marts models are covered by automated tests.

---

## Example Analytical Use Cases

* Sales performance by product, store, and time
* Identification of out-of-stock frequency and duration
* Analysis of promotion impact on sales
* Correlation between stock availability and sales volume

---

## Tech Stack

* **BigQuery**
* **dbt**
* **SQL**
* **Power BI**

---

## Project Status

The pipeline is fully implemented, tested, and ready for analytical consumption.
Future extensions may include incremental models, SCD implementation, and BI dashboards.
