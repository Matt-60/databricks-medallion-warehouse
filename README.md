# Databricks Medallion Data Warehouse

## Overview

This project demonstrates the design and implementation of an **end-to-end data warehouse pipeline in Databricks** using **Medallion Architecture**, Delta Lake, PySpark, and SQL.

The solution processes transactional e-commerce data through Bronze, Silver, and Gold layers, applying incremental data loading, deduplication, `MERGE` operations, dimensional modeling, surrogate keys, and incremental fact processing.

The project is designed to simulate a production-oriented data engineering workflow, including orchestration with **Databricks Jobs**.

---

## Architecture

```text
                         SOURCE
                            │
                            ▼
                 ┌────────────────────┐
                 │      BRONZE        │
                 │ Incremental Append │
                 └─────────┬──────────┘
                           │
                           ▼
                 ┌────────────────────┐
                 │      SILVER        │
                 │ Incremental MERGE  │
                 │ Deduplication      │
                 │ Standardization    │
                 └─────────┬──────────┘
                           │
                           ▼
                 ┌────────────────────┐
                 │       GOLD         │
                 │ Dimensional Model  │
                 └─────────┬──────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
        DIMENSIONS                   FACT
       MERGE / Lookup          Incremental MERGE
              │                         │
              └────────────┬────────────┘
                           ▼
                      DATA MART
```

### Medallion Layers

**Bronze**

* Raw source data stored in Delta format
* Incremental ingestion
* Append-based loading
* Preserves incoming source records and changes
* Uses `last_updated` as an incremental watermark
* Stores `ingestion_ts` for pipeline auditing

**Silver**

* Cleaned and standardized data
* Incremental processing
* `MERGE` used to maintain the latest version of each order
* Deduplication using `ROW_NUMBER()`
* Data transformations such as customer name standardization
* Stores `process_ts` to track processing time

**Gold**

* Dimensional data warehouse model
* Surrogate keys generated using Delta identity columns
* Customer and product dimensions maintained using incremental `MERGE`
* Small reference dimensions such as payment types and countries maintained using `DISTINCT` and `MERGE`
* Date dimension generated from the transactional date range
* Sales fact table processed incrementally

---

## Data Model

The Gold layer follows a **star schema**:

```text
                    ┌─────────────────┐
                    │  DimCustomers   │
                    │-----------------│
                    │ customer_sk (PK)│
                    │ customer_id     │
                    │ customer_name   │
                    │ customer_email  │
                    └────────┬────────┘
                             │
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  DimProducts    │          │          │   DimPayments   │
│-----------------│          │          │-----------------│
│ product_sk (PK) │          │          │ payment_sk (PK)│
│ product_id      │          │          │ payment_type   │
│ product_name    │          │          └────────┬────────┘
│ category        │          │                   │
└────────┬────────┘          │                   │
         │                   ▼                   │
         │          ┌─────────────────┐          │
         └─────────►│    FactSales    │◄─────────┘
                    │-----------------│
                    │ order_id        │
                    │ customer_sk     │
                    │ product_sk      │
                    │ payment_sk      │
                    │ region_sk       │
                    │ date_key        │
                    │ quantity        │
                    │ unit_price      │
                    │ sales_amount    │
                    └────────┬────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
          ┌─────────────────┐   ┌─────────────────┐
          │    DimRegion    │   │     DimDate     │
          │-----------------│   │-----------------│
          │ region_sk (PK)  │   │ date_key (PK)   │
          │ country         │   │ full_date       │
          └─────────────────┘   │ year/month/...  │
                                └─────────────────┘
```

### Dimensions

* **DimCustomers** — customer attributes and surrogate key
* **DimProducts** — product attributes and surrogate key
* **DimPayments** — unique payment methods
* **DimRegion** — unique countries
* **DimDate** — calendar dimension

### Fact

**FactSales** contains transactional sales data and foreign keys to the dimensions.

The main measure is:

```text
sales_amount = quantity × unit_price
```

---

## Incremental Processing

Incremental processing is implemented throughout the pipeline where it provides the most value.

### Bronze

New source records are identified using the maximum `last_updated` value already present in Bronze:

```sql
WHERE last_updated > last_load_date
```

The new records are appended to the Bronze Delta table.

### Silver

Silver processes only records that are newer than the latest processed timestamp/date and uses `MERGE` to maintain the current state:

```sql
MERGE INTO silver_table t
USING silver_source s
ON t.order_id = s.order_id
```

The latest record for each order is selected using:

```sql
ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY last_updated DESC
)
```

This prevents multiple versions of the same order from being loaded into the current-state Silver table.

### Gold Dimensions

Customer and product dimensions use incremental `MERGE` operations.

For example:

```sql
WHEN MATCHED AND s.last_updated > t.last_updated
THEN UPDATE SET ...

WHEN NOT MATCHED
THEN INSERT ...
```

This implements **SCD Type 1 behavior**, where updated attributes overwrite the previous values.

Surrogate keys remain stable because they are generated in the dimension tables and are not updated during a `MERGE`.

### Gold Fact

The `FactSales` table is also processed incrementally.

New or changed transactions are identified using `last_updated` and then merged into the fact table based on `order_id`.

This avoids rebuilding the entire fact table for every pipeline execution.

---

## Surrogate Keys

The dimensional model uses surrogate keys generated by Delta identity columns:

```sql
customer_sk BIGINT GENERATED ALWAYS AS IDENTITY
```

The source business keys remain available:

```text
customer_id
product_id
```

while the surrogate keys are used as foreign keys in the fact table:

```text
FactSales
├── customer_sk
├── product_sk
├── payment_sk
├── region_sk
└── date_key
```

This separates source-system identifiers from warehouse keys and provides stable dimension references for the fact table.

---

## Change Data Simulation

To test the incremental pipeline, source-system changes are simulated through separate batches.

### Initial Load

```text
2024-07-01

1001
1002
1003
```

### Batch 1

The first incremental batch contains:

```text
2024-07-02

INSERT → order_id 1004
UPDATE → order_id 1001
```

This allows the pipeline to demonstrate both major incremental scenarios:

* inserting a new record
* updating an existing record

The changes propagate through:

```text
Source
  ↓
Bronze
  ↓
Silver
  ↓
Gold Dimensions
  ↓
FactSales
```

This provides a simple way to validate the complete end-to-end pipeline.

---

## Orchestration

The notebooks are orchestrated using **Databricks Jobs**.

The execution flow is:

```text
01_Bronze
    │
    ▼
02_Silver
    │
    ▼
03_Gold
```

The Gold notebook builds the dimensional model and fact table after Silver has been successfully processed.

The job can be scheduled to run automatically, allowing the pipeline to process new source changes without manually executing each notebook.

---

## Technology Stack

| Technology          | Purpose                                     |
| ------------------- | ------------------------------------------- |
| **Databricks**      | Data processing and orchestration           |
| **PySpark**         | Incremental processing and Spark operations |
| **Spark SQL**       | Data transformation and modeling            |
| **Delta Lake**      | ACID tables and reliable data storage       |
| **SQL**             | Data manipulation and dimensional modeling  |
| **Databricks Jobs** | Pipeline orchestration and scheduling       |

---

## Key Concepts Demonstrated

* Medallion Architecture
* Delta Lake
* Incremental data loading
* Append-only ingestion
* Incremental `MERGE`
* Deduplication with `ROW_NUMBER()`
* Watermark-based processing
* SCD Type 1
* Surrogate keys
* Star schema
* Fact and dimension tables
* Data standardization
* Audit timestamps
* Incremental fact processing
* Databricks Jobs and orchestration

---

## Project Structure

```text
databricks-medallion-dwh/
│
├── notebooks/
│   ├── 01_bronze
│   ├── 02_silver
│   └── 03_gold
│
├── README.md
└── ...
```

---

## Future Improvements

Potential extensions to make the pipeline more production-oriented include:

* handling late-arriving records
* replacing simple watermarks with more robust ingestion metadata
* implementing data quality checks
* adding SCD Type 2 for selected dimensions
* adding pipeline monitoring and logging
* parameterizing notebooks for different environments
* implementing automated testing
* integrating the Gold layer with a BI reporting solution

---

## Summary

This project demonstrates an end-to-end **Databricks data warehouse pipeline** that combines Medallion Architecture with dimensional modeling and incremental data processing.

The main design principle is to use incremental processing where it provides meaningful performance benefits while keeping smaller reference dimensions simple and maintainable.

The result is a scalable and production-oriented architecture suitable for analytical workloads and downstream BI consumption.
