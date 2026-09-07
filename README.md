# Databricks Medallion Data Warehouse

An end-to-end data warehouse pipeline in Databricks — Bronze → Silver → Gold — built on Delta Lake with incremental loading, deduplication, and dimensional modeling.

`Databricks` · `Delta Lake` · `Spark SQL` · `Star Schema` · `SCD Type 1`

---

## 🎯 Business Goal

A transactional e-commerce source needs to feed a reliable, incrementally-updated star schema for sales reporting — without reprocessing the full history on every run, and without losing track of order updates (not just new orders). This project builds that pipeline end to end: raw orders land in Bronze, get cleaned and deduplicated in Silver, and are modeled into a Gold-layer star schema ready for BI consumption.

## 🏗️ Architecture

```
SOURCE
  │
  ▼
BRONZE  — incremental append (watermark on last_updated)
  │
  ▼
SILVER  — incremental MERGE, dedup, standardization
  │
  ▼
GOLD    — dimensional model (star schema)
  │
  ├── Dimensions (MERGE / lookup) ──┐
  └── Fact (incremental MERGE) ─────┴──► Data Mart
```

| Layer | What happens |
|---|---|
| **Bronze** | Raw records appended incrementally, filtered by a `last_updated` watermark against the max value already in Bronze. Stores `ingestion_ts` for auditing. |
| **Silver** | Deduplicates via `ROW_NUMBER()` (latest version per `order_id`), standardizes fields (e.g. uppercased customer name), and `MERGE`s into a current-state table. Stores `process_ts`. |
| **Gold** | Star schema: `DimCustomers`, `DimProducts` (incremental `MERGE`, SCD Type 1), `DimPayments`/`DimRegion` (small reference dimensions via `DISTINCT` + `MERGE`), `DimDate` (generated from the transactional date range), and `FactSales` (incremental `MERGE`, joined to all dimensions for surrogate keys). |

## ⭐ Data Model

`FactSales` at the center — `customer_sk`, `product_sk`, `payment_sk`, `region_sk`, `date_key` as foreign keys, `quantity` / `unit_price` / `sales_amount` (`= quantity × unit_price`) as measures — surrounded by `DimCustomers`, `DimProducts`, `DimPayments`, `DimRegion`, `DimDate`.

<details>
<summary><b>📐 Incremental logic per layer (click to expand)</b></summary>

**Bronze — watermark append**
```sql
WHERE last_updated > last_load_date   -- last_load_date = MAX(last_updated) already in Bronze
```

**Silver — dedup + current-state MERGE**
```sql
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_updated DESC)
-- keep rn = 1, then:
MERGE INTO silver_table t USING silver_source s ON t.order_id = s.order_id
WHEN MATCHED AND s.last_updated > t.last_updated THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Gold dimensions — SCD Type 1**
```sql
WHEN MATCHED AND s.last_updated > t.last_updated THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```
Surrogate keys (`customer_sk`, `product_sk`, …) are generated once via Delta identity columns and stay stable across updates — the `MERGE` never touches them.

**Gold fact — incremental, joined to dimensions**
```sql
FROM silver_table f
LEFT JOIN dimcustomers c ON f.customer_id = c.customer_id
LEFT JOIN dimproducts p ON f.product_id = p.product_id
...
WHERE f.last_updated > (SELECT COALESCE(MAX(last_updated), '1000-01-01') FROM fact_sales)
```

</details>

## 🧪 Change Simulation

The pipeline is validated against a simple two-batch scenario: an **initial load** (3 orders), then a **batch** that both **inserts** a new order and **updates** an existing one — exercising both incremental paths (new record vs. changed record) end to end through Bronze → Silver → Gold.

## 🧠 Key Design Decisions

- **Incremental everywhere it earns its place** — Bronze, Silver, and the fact table are all watermark-driven; small reference dimensions (`DimPayments`, `DimRegion`) are kept simple via plain `DISTINCT` + `MERGE` rather than over-engineering incremental logic where the data volume doesn't justify it.
- **Delta identity columns for surrogate keys** — decouples source-system IDs (`customer_id`, `product_id`) from warehouse keys, keeping fact-table foreign keys stable even if source identifiers change.
- **SCD Type 1 for dimensions** — attributes are overwritten on update; historical versions aren't preserved, a deliberate simplification for this dataset (see Future Improvements for SCD2).

<details>
<summary><b>⚠️ Known limitation (click to expand)</b></summary>

The watermark column (`last_updated`) is `DATE`, not `TIMESTAMP`. Because the incremental filter is a strict `>`, a record updated on the *same day* as the last successful load could be missed on the next run. Fine for daily-batch demo data; a production version should use a `TIMESTAMP` watermark (or `>=` with a de-dup-safe re-read window).

</details>

<details>
<summary><b>🛠️ Tech stack, repo structure & future improvements (click to expand)</b></summary>

**Technology Stack**

| Technology | Purpose |
|---|---|
| Databricks | Notebook environment, compute |
| Delta Lake | ACID tables, `MERGE`, identity columns |
| Spark SQL | All transformation and modeling logic |
| Databricks Jobs | Intended orchestration (Bronze → Silver → Gold) |

**Repository structure**
```
Databricks-Medallion-Architecture-Project/
├── Source (intial load).ipynb   # creates + seeds the source table
├── Batch load.ipynb             # simulates an insert + an update
├── Bronze.ipynb
├── Silver.ipynb
├── Gold.ipynb
└── Drop tables.ipynb            # teardown for a clean re-run
```

**Future Improvements**
- Handle late-arriving records; move the watermark to `TIMESTAMP` granularity
- Data quality checks
- SCD Type 2 for selected dimensions
- Pipeline monitoring/logging, parameterized notebooks per environment
- Automated testing
- Connect the Gold layer to a BI tool (Power BI)

</details>

---

*Demonstrates Medallion architecture, Delta Lake `MERGE` patterns, watermark-based incremental loading, and dimensional modeling on Databricks.*
