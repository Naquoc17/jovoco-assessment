# Retail Trade ETL Assessment

This project loads retail CSV files into PostgreSQL and builds three layers:

- `stg`: raw CSV data
- `slv`: cleaned business data
- `gold`: analytical star schema

## 1. Semantic Model

The analytical layer uses a simple star schema with one central fact table:

- `gold.fact_sales`
- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_store`
- `gold.dim_date`

### Grain

`fact_sales` has one row per order item.

This grain was chosen because all interview questions can be answered from line-level sales:

- revenue by product and region
- customer segmentation by sales amount
- product pairs bought together in the same order
- customers ordering in multiple quarters

### Modeling Decisions

- `dim_customer` stores customer master data from `slv.customers`
- `dim_product` stores product attributes from `slv.products`
- `dim_store` stores store and region attributes from `slv.stores`
- `dim_date` is generated from the minimum and maximum order dates in `slv.orders`
- `fact_sales` keeps the measures `quantity`, `unit_price`, `line_total`
- `order_id` and `order_status` stay in the fact table as simple transactional fields

This model is intentionally simple and easy to explain in an interview.

## 2. ETL Layers

### Bronze

The Bronze step loads each CSV file into the `stg` schema without business transformation. All columns are loaded as `TEXT` so the raw values stay unchanged.

The load is idempotent because each staging table is dropped and recreated before loading.

### Silver

The Silver step cleans and standardizes the raw values:

- parse inconsistent date formats
- trim empty strings
- normalize customer name matching
- cast IDs and numeric fields to usable types

The transformation is also idempotent because each `slv` table is rebuilt from `stg`.

### Gold

The Gold step builds the star schema from the Silver layer. It is idempotent because the dimension and fact tables are recreated on each run.

## 3. Analysis Queries

The folder `sql/analysis/` contains the SQL answers for:

1. Top-5 products by revenue in the last quarter by region
2. Customer groups High / Mid / Low Value using window functions
3. Top-10 product pairs bought together
4. Share of customers ordering in more than one quarter

## 4. Run

Start PostgreSQL:

```bash
docker-compose up -d
```

Run the pipeline:

```bash
python main.py
```

Run the simple tests:

```bash
python -m unittest discover -s test
```
