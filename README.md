# End-to-End ELT Pipeline with dbt, Snowflake, and Airflow

## Project overview

This tutorial-inspired learning project demonstrates an end-to-end ELT pipeline using Snowflake as the data warehouse, dbt for SQL transformation and testing, and Apache Airflow with Astronomer Cosmos for scheduling and orchestration.

The pipeline uses Snowflake's TPC-H sample data and transforms raw `ORDERS` and `LINEITEM` tables into a tested, order-level mart named `fct_orders`.

## Architecture

```text
Snowflake TPC-H raw data
        ↓
dbt source declarations
        ↓
Staging views: orders and line items
        ↓
Intermediate models: joins and summaries
        ↓
Mart table: fct_orders
        ↓
dbt tests
        ↓
Airflow/Cosmos daily orchestration
```

## Model layers

| Model | Grain | Purpose |
|---|---|---|
| `stg_tpch_orders` | One row per order | Renames raw order columns |
| `stg_tpch_line_items` | One row per order line | Renames columns and creates `order_item_key` |
| `int_order_items` | One row per order line | Joins line items to orders and calculates discounts |
| `int_order_items_summary` | One row per order | Aggregates line-level sales and discounts |
| `fct_orders` | One row per order | Final business-facing mart |

## Key design

The source order key `o_orderkey` becomes `order_key`. A line item is identified by the composite natural key `l_orderkey + l_linenumber`. The project creates the technical key `order_item_key` with `dbt_utils.generate_surrogate_key`. The line item retains `order_key` so it can join back to its parent order.

## dbt features demonstrated

- Source declarations with `source()`
- Model dependencies with `ref()`
- Staging, intermediate, and mart layers
- Jinja macros for reusable SQL logic
- `dbt_utils.generate_surrogate_key`
- View and table materializations
- Generic tests: `unique`, `not_null`, `relationships`, and `accepted_values`
- Singular SQL tests for discount and date rules

## Local dbt setup

1. Install dbt with the Snowflake adapter.

   ```bash
   python -m pip install dbt-snowflake
   ```

2. Copy `dbt/profiles.yml.example` to `~/.dbt/profiles.yml` and replace the placeholders locally.

3. Install the package and validate the connection.

   ```bash
   cd dbt
   dbt deps
   dbt debug
   ```

4. Compile, build, and test the models.

   ```bash
   dbt compile
   dbt run
   dbt test
   ```

## Airflow setup

The `airflow/` directory contains the Cosmos DAG and required dependencies. Create an Airflow Snowflake connection with ID `snowflake_conn`, then configure the local Airflow environment to mount the `dbt/` directory at `/usr/local/airflow/dags/dbt`.

The DAG is configured to run daily and build the dbt dependency graph from staging through the final mart and tests.

## Security note

Credentials are intentionally excluded. Use `dbt/profiles.yml.example` as a template, and never commit passwords, tokens, private keys, `.env` files, or production connection details.

## Learning outcome

This project helped me understand how raw data becomes reliable analytical data through modular SQL models, reusable macros, key design, data tests, and orchestration.

## Project note

This is a tutorial-inspired learning project, not a claim of production readiness. A production deployment would additionally need stronger secret management, CI/CD, environment separation, monitoring, incremental processing, and cost controls.
