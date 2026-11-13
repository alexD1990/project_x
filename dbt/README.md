# project_x dbt

dbt project for **Northpeak**, a fictional Nordic outdoor-gear webshop, on Databricks.

```
source shop (workspace.project_x_raw)      seeds
  customers, products, orders,               country_regions, product_categories,
  order_items, payments                      currency_rates, campaigns
        │                                          │
        ▼                                          │
staging        stg_shop__*  (views)                │
        │                                          │
        ▼                                          ▼
intermediate   int_orders__converted, int_order_items__enriched, int_payments__per_order
        │
        ▼
marts          fct_orders, dim_customers, dim_products, mart_monthly_revenue  (tables)
```

## Run

```bash
cd dbt
source ~/.venvs/dbt-databricks/bin/activate
set -a; source ~/.databricks.env; set +a
dbt deps
dbt build
```

Schemas: `workspace.project_x_seeds`, `project_x_staging`, `project_x_intermediate`, `project_x_marts`.
The raw tables come from the notebook `analyze/generate_source_data`.

One test warns on purpose: a few order lines point at SKUs that were deleted from the product catalogue.
