# Databricks Medallion Architecture Project

## Overview

This project demonstrates a complete implementation of a modern Data Warehouse using the Medallion Architecture pattern in Databricks.

The solution includes:

- Initial data load
- Incremental data processing
- Bronze, Silver and Gold layers
- Delta Lake MERGE operations
- Incremental dimension loading
- Incremental fact loading
- Kimball Star Schema modeling
- Surrogate Keys (Identity Columns)

The project simulates a sales system where new orders are inserted and existing orders are updated over time.

---

## Architecture

```text
Source
   │
   ▼
Bronze
(Raw Layer)
   │
   ▼
Silver
(Cleansed & Deduplicated Layer)
   │
   ▼
Gold
(Kimball Star Schema)
```

---

## Technology Stack

- Databricks
- Apache Spark SQL
- Delta Lake
- Medallion Architecture
- Kimball Dimensional Modeling

---

## Project Structure

| Notebook | Description |
|-----------|-------------|
| 01_Drop_Tables | Reset environment |
| 02_Source | Create source table and initial dataset |
| 03_Incremental_Load | Simulate insert and update operations |
| 04_Bronze | Raw ingestion layer |
| 05_Silver | Cleansing, standardization and deduplication |
| 06_Gold | Dimensions and Fact table |

---

# Source Layer

## Table

```sql
source.source_data
```

The source table contains sales transactions.

### Example Attributes

- order_id
- order_date
- customer_id
- customer_name
- product_id
- product_name
- payment_type
- country
- quantity
- unit_price
- last_updated

### Incremental Processing

The entire pipeline uses:

```sql
last_updated
```

as the watermark column.

---

# Bronze Layer

## Purpose

Store raw source data without business transformations.

## Logic

```sql
SELECT *
FROM source.source_data
WHERE last_updated > last_processed_date
```

## Characteristics

- Raw data copy
- Append-only ingestion
- Delta Lake table

---

# Silver Layer

## Purpose

Store cleansed and standardized business data.

## Transformations

### Customer Name Standardization

```sql
UPPER(customer_name)
```

### Deduplication

Latest version of each order is retained.

```sql
ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY last_updated DESC
)
```

### Incremental Processing

```sql
WHERE last_updated >
(
    SELECT MAX(last_updated)
    FROM silver_table
)
```

### Load Strategy

```sql
MERGE INTO silver_table
```

using:

```sql
order_id
```

as the business key.

---

# Gold Layer

The Gold layer follows the Kimball dimensional modeling approach.

## Star Schema

```text
                DimCustomers
                       │
                       │
DimProducts ─── FactSales ─── DimPayments
                       │
                       │
                  DimRegion
                       │
                       │
                    DimDate
```

---

# Dimensions

## DimCustomers

Contains customer-related information.

### Surrogate Key

```sql
customer_sk
```

### Business Key

```sql
customer_id
```

### Loading Strategy

Incremental MERGE.

---

## DimProducts

Contains product information.

### Surrogate Key

```sql
product_sk
```

### Business Key

```sql
product_id
```

### Loading Strategy

Incremental MERGE.

---

## DimPayments

Contains payment methods.

### Surrogate Key

```sql
payment_sk
```

### Business Key

```sql
payment_type
```

### Loading Strategy

Incremental MERGE.

---

## DimRegion

Contains geographical information.

### Surrogate Key

```sql
region_sk
```

### Business Key

```sql
country
```

### Loading Strategy

Incremental MERGE.

---

## DimDate

Calendar dimension generated dynamically from available transaction dates.

### Attributes

- date_key
- full_date
- year
- month
- day
- week_of_year
- day_name
- month_name
- quarter
- is_weekend

---

# Fact Table

## FactSales

Stores transactional sales data.

### Grain

One row per order.

### Foreign Keys

| Column | Dimension |
|----------|------------|
| customer_sk | DimCustomers |
| product_sk | DimProducts |
| payment_sk | DimPayments |
| region_sk | DimRegion |
| date_key | DimDate |

### Measures

- quantity
- unit_price
- sales_amount

### Calculated Measure

```sql
sales_amount = quantity * unit_price
```

---

# Incremental Load Simulation

## Initial Load

Inserted:

```text
3 records
```

Result:

```text
Source → Bronze → Silver → Gold
```

All layers populated successfully.

---

## Incremental Batch

Operations:

```sql
INSERT INTO source_data
```

New Order:

```text
order_id = 1004
```

and

```sql
UPDATE source_data
```

Updated Order:

```text
order_id = 1001
```

---

## Processing Results

### Bronze

New records appended.

### Silver

- 1 INSERT
- 1 UPDATE

using Delta MERGE.

### Gold Dimensions

Incrementally updated using:

```sql
ROW_NUMBER()
```

and

```sql
MERGE
```

### FactSales

Incrementally updated using:

```sql
MERGE
```

on:

```sql
order_id
```

---

# Data Warehouse Design

## Modeling Approach

Kimball Star Schema

### Surrogate Keys

Dimensions use:

- customer_sk
- product_sk
- payment_sk
- region_sk

implemented as:

```sql
GENERATED ALWAYS AS IDENTITY
```

### Why Surrogate Keys?

Benefits:

- Faster joins
- Stable relationships
- Industry standard Kimball design
- Better BI performance

---

# Slowly Changing Dimensions

Current implementation:

```text
SCD Type 1
```

Dimension changes overwrite existing records.

Future enhancement:

```text
SCD Type 2
```

for historical tracking.

---

# Future Improvements

Potential production enhancements:

- Delta Live Tables
- Auto Loader
- Unity Catalog
- SCD Type 2 Dimensions
- Data Quality Checks
- Workflow Orchestration
- OPTIMIZE and ZORDER
- Monitoring and Alerting
- Metadata-driven ETL

---

# Key Concepts Demonstrated

- Medallion Architecture
- Delta Lake MERGE
- Incremental ETL
- Watermark Processing
- Deduplication
- Kimball Modeling
- Star Schema Design
- Surrogate Keys
- Databricks SQL Development
