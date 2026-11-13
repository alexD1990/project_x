# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # project_x: generate source data
# MAGIC
# MAGIC Creates the raw tables for **Northpeak**, a fictional Nordic outdoor-gear webshop, in
# MAGIC `workspace.project_x_raw`. These tables play the role of data that an ingestion tool
# MAGIC (Fivetran, Lakeflow Connect, ...) has already landed. dbt reads them as the source `shop`.
# MAGIC
# MAGIC | Table | Rows (approx.) | Grain |
# MAGIC | --- | --- | --- |
# MAGIC | `customers` | 1 540 | one row per customer *version* (some customers appear twice) |
# MAGIC | `products` | 60 | one row per SKU |
# MAGIC | `orders` | 9 000 | one row per order |
# MAGIC | `order_items` | 17 000 | one row per order line |
# MAGIC | `payments` | 9 600 | one row per payment attempt or refund |
# MAGIC
# MAGIC Prices in `products` are in EUR (the reporting currency); orders and payments are in the customer's local currency.
# MAGIC
# MAGIC The data is deterministic (fixed random seed), so every run gives the same rows.
# MAGIC Run the whole notebook once on serverless compute. Re-running it replaces the tables.
# MAGIC
# MAGIC **Planted on purpose** (things a real raw layer has, and the dbt project deals with):
# MAGIC - ~40 customers have an older duplicate row with an outdated e-mail (deduplicated in staging)
# MAGIC - some e-mails are missing or in upper case
# MAGIC - ~3 % of order statuses have odd casing or trailing spaces (`'Completed'`, `'COMPLETED '`)
# MAGIC - a handful of order lines point at two SKUs that were deleted from `products` (a dbt test warns)

# COMMAND ----------

import datetime as dt
import random
from decimal import Decimal, ROUND_HALF_UP

from pyspark.sql import types as T

CATALOG = "workspace"
SCHEMA = "project_x_raw"

START_DATE = dt.date(2025, 1, 1)
END_DATE = dt.date(2026, 11, 12)  # "today" for the dataset; the last days have open orders

N_CUSTOMERS = 1500
N_ORDERS = 9000

rng = random.Random(42)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Raw landing tables for project_x'")

# COMMAND ----------

# MAGIC %md ## Reference values

# COMMAND ----------

COUNTRIES = {
    # code: (weight, currency, cities)
    "NO": (0.45, "NOK", ["Oslo", "Bergen", "Trondheim", "Stavanger", "Tromso", "Kristiansand", "Bodo", "Lillehammer"]),
    "SE": (0.25, "SEK", ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Umea", "Ostersund"]),
    "DK": (0.15, "DKK", ["Copenhagen", "Aarhus", "Odense", "Aalborg"]),
    "FI": (0.08, "EUR", ["Helsinki", "Tampere", "Oulu", "Rovaniemi"]),
    "DE": (0.07, "EUR", ["Munich", "Berlin", "Hamburg", "Freiburg"]),
}

# Rough price-list rates (EUR per unit of local currency). The exact monthly rates live in the dbt seed currency_rates.
PRICE_LIST_RATE = {"EUR": 1.0, "NOK": 0.0855, "SEK": 0.0870, "DKK": 0.1341}

FIRST_NAMES = [
    "Emma", "Oliver", "Sophie", "James", "Olivia", "Lucas", "Emily", "Noah", "Grace", "Liam",
    "Chloe", "Ethan", "Ava", "Jack", "Mia", "Leo", "Ella", "Henry", "Lily", "Oscar",
    "Freya", "Theo", "Isla", "Max", "Nora", "Felix", "Alice", "Hugo", "Ruby", "Adam",
]
LAST_NAMES = [
    "Smith", "Johnson", "Brown", "Taylor", "Wilson", "Davies", "Evans", "Walker", "Hall", "Wright",
    "Green", "Baker", "Hill", "Clarke", "Turner", "Parker", "Cooper", "Ward", "Morgan", "Bennett",
]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "hotmail.com", "proton.me", "icloud.com", "fastmail.com", "yahoo.com"]

# category_code: (n_products, min_price_eur, max_price_eur, nouns)
CATEGORIES = {
    "TENT":     (6, 150, 640, ["2P Tent", "3P Tent", "Tunnel Tent", "Ultralight Shelter"]),
    "SLEEP":    (6, 50, 360, ["Down Sleeping Bag", "Synthetic Sleeping Bag", "Sleeping Pad", "Liner"]),
    "PACK":     (8, 40, 275, ["Daypack 25L", "Trekking Pack 45L", "Expedition Pack 70L", "Hip Pack"]),
    "JACKET":   (9, 75, 470, ["Shell Jacket", "Down Jacket", "Fleece", "Insulated Vest"]),
    "FOOTWEAR": (8, 70, 290, ["Hiking Boot", "Trail Runner", "Approach Shoe", "Winter Boot"]),
    "SKI":      (7, 100, 760, ["Touring Ski", "Ski Skins", "Touring Binding", "Ski Poles"]),
    "CLIMB":    (6, 20, 240, ["Harness", "Helmet", "Quickdraw Set", "Chalk Bag"]),
    "KITCHEN":  (5, 12, 140, ["Gas Stove", "Pot Set", "Spork", "Insulated Mug"]),
    "ACCESS":   (5, 9, 79, ["Headlamp", "Buff", "Wool Socks", "Map Case"]),
}
BRANDS = ["Summitcraft", "Trailborn", "Polarline", "Fjordfield", "Highmark", "Wildmere"]
ADJECTIVES = ["Ridge", "Summit", "Fjord", "Glacier", "Arctic", "Trail", "Storm", "Midnight", "Birch", "Granite"]

CAMPAIGNS = [
    # code, start, end, discount range  (same codes as the dbt seed campaigns.csv)
    ("WINTER25", dt.date(2025, 1, 6), dt.date(2025, 2, 16), (0.10, 0.20)),
    ("EASTER25", dt.date(2025, 4, 7), dt.date(2025, 4, 21), (0.15, 0.15)),
    ("SUMMER25", dt.date(2025, 6, 2), dt.date(2025, 7, 13), (0.10, 0.20)),
    ("BF25", dt.date(2025, 11, 24), dt.date(2025, 12, 1), (0.20, 0.30)),
    ("XMAS25", dt.date(2025, 12, 2), dt.date(2025, 12, 20), (0.10, 0.10)),
    ("WINTER26", dt.date(2026, 1, 5), dt.date(2026, 2, 15), (0.10, 0.20)),
    ("EASTER26", dt.date(2026, 3, 23), dt.date(2026, 4, 6), (0.15, 0.15)),
    ("SUMMER26", dt.date(2026, 6, 1), dt.date(2026, 7, 12), (0.10, 0.20)),
    ("AUTUMN26", dt.date(2026, 9, 14), dt.date(2026, 10, 4), (0.10, 0.15)),
]

# Orders per month: outdoor gear peaks before Christmas, in early summer and at the start of ski season.
MONTH_WEIGHT = {1: 1.0, 2: 0.9, 3: 1.0, 4: 1.1, 5: 1.2, 6: 1.4, 7: 1.2, 8: 0.9, 9: 0.9, 10: 1.0, 11: 1.6, 12: 1.8}

PAYMENT_METHODS = {
    "NO": [("vipps", 0.45), ("card", 0.35), ("klarna", 0.20)],
    "SE": [("swish", 0.40), ("klarna", 0.35), ("card", 0.25)],
    "DK": [("mobilepay", 0.45), ("card", 0.45), ("paypal", 0.10)],
    "FI": [("card", 0.60), ("paypal", 0.25), ("klarna", 0.15)],
    "DE": [("paypal", 0.45), ("card", 0.35), ("klarna", 0.20)],
}


def pick(weighted):
    """Pick a key from [(value, weight), ...]."""
    values, weights = zip(*weighted)
    return rng.choices(values, weights=weights, k=1)[0]


def money(x):
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def rand_date(start, end):
    return start + dt.timedelta(days=rng.randint(0, (end - start).days))


def rand_ts(day):
    # Most orders in the evening
    hour = pick([(h, 3 if 18 <= h <= 22 else 2 if 9 <= h <= 17 else 1) for h in range(24)])
    return dt.datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))

# COMMAND ----------

# MAGIC %md ## products

# COMMAND ----------

products = []
used_names = set()
sku_no = 1001
for category, (n, lo, hi, nouns) in CATEGORIES.items():
    for _ in range(n):
        brand = rng.choice(BRANDS)
        name = f"{brand} {rng.choice(ADJECTIVES)} {rng.choice(nouns)}"
        while name in used_names:
            name = f"{brand} {rng.choice(ADJECTIVES)} {rng.choice(nouns)}"
        used_names.add(name)
        list_price = money(round(rng.uniform(lo, hi)) - 0.05)  # 249.95-style prices
        cost_price = money(float(list_price) * rng.uniform(0.42, 0.60))
        launched_at = rand_date(dt.date(2022, 1, 1), dt.date(2025, 9, 1))
        products.append((f"NP-{sku_no}", name, category, brand, list_price, cost_price, True, launched_at))
        sku_no += 1

# Two products have been taken out of the range
for i in (4, 37):
    p = list(products[i]); p[6] = False; products[i] = tuple(p)

# Two SKUs that were sold but later deleted from the product table (planted: dbt relationships test warns)
DELETED_SKUS = {"NP-0998": money(109.95), "NP-0999": money(29.95)}

products_schema = T.StructType([
    T.StructField("product_sku", T.StringType(), False),
    T.StructField("product_name", T.StringType(), False),
    T.StructField("category_code", T.StringType(), False),
    T.StructField("brand", T.StringType(), False),
    T.StructField("list_price_eur", T.DecimalType(10, 2), False),
    T.StructField("cost_price_eur", T.DecimalType(10, 2), False),
    T.StructField("is_active", T.BooleanType(), False),
    T.StructField("launched_at", T.DateType(), True),
])

# COMMAND ----------

# MAGIC %md ## customers

# COMMAND ----------

customers = []         # current rows
customer_country = {}
customer_signup = {}
country_weights = [(c, w) for c, (w, _, _) in COUNTRIES.items()]

for customer_id in range(1, N_CUSTOMERS + 1):
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    country = pick(country_weights)
    city = rng.choice(COUNTRIES[country][2])
    signup = rand_date(dt.date(2024, 6, 1), END_DATE - dt.timedelta(days=20))
    local = f"{first}.{last}{rng.randint(1, 99)}".lower()
    email = f"{local}@{rng.choice(EMAIL_DOMAINS)}"
    if rng.random() < 0.05:
        email = email.upper()                       # planted: inconsistent casing
    if rng.random() < 0.017:
        email = None                                # planted: missing e-mail
    opt_in = rng.random() < 0.55
    updated_at = rand_ts(rand_date(signup, END_DATE))
    customers.append((customer_id, first, last, email, country, city, signup, opt_in, updated_at))
    customer_country[customer_id] = country
    customer_signup[customer_id] = signup

# Planted: ~40 customers also have an OLDER row (outdated e-mail, earlier updated_at).
duplicates = []
for row in rng.sample(customers, 40):
    (cid, first, last, email, country, city, signup, opt_in, updated_at) = row
    old_email = f"{first}.{last}@{rng.choice(EMAIL_DOMAINS)}".lower()
    old_updated = dt.datetime.combine(signup, dt.time(12, 0))
    duplicates.append((cid, first, last, old_email, country, city, signup, not opt_in, old_updated))

customers_all = customers + duplicates
rng.shuffle(customers_all)

customers_schema = T.StructType([
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("country_code", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("signup_date", T.DateType(), True),
    T.StructField("marketing_opt_in", T.BooleanType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md ## orders, order_items, payments

# COMMAND ----------

months = []
d = START_DATE
while d <= END_DATE:
    months.append(d)
    d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
def days_in_scope(m):
    nxt = dt.date(m.year + (m.month == 12), m.month % 12 + 1, 1)
    return ((min(nxt - dt.timedelta(days=1), END_DATE) - m).days + 1) / (nxt - m).days

# Seasonality, some growth year over year, and the current month only counts the days so far
month_weights = [(m, MONTH_WEIGHT[m.month] * (1.0 + 0.25 * (m.year - 2025)) * days_in_scope(m)) for m in months]

# Some customers buy far more often than others
customer_weights = [(cid, rng.lognormvariate(0, 0.6)) for cid in range(1, N_CUSTOMERS + 1)]

active_skus = [p for p in products if p[6]]
product_weights = [(p, 1.0 / (1 + i % 7)) for i, p in enumerate(active_skus)]

orders, order_items, payments = [], [], []
order_item_id = 1
payment_id = 1
order_id = 100001

while len(orders) < N_ORDERS:
    cid = pick(customer_weights)
    month = pick(month_weights)
    month_end = min(dt.date(month.year + (month.month == 12), month.month % 12 + 1, 1) - dt.timedelta(days=1), END_DATE)
    day = rand_date(month, month_end)
    if day < customer_signup[cid]:
        continue
    ordered_at = rand_ts(day)
    country = customer_country[cid]
    currency = COUNTRIES[country][1]
    rate = PRICE_LIST_RATE[currency]

    campaign_code, discount_range = None, None
    active = [c for c in CAMPAIGNS if c[1] <= day <= c[2]]
    if active and rng.random() < 0.45:
        campaign_code, _, _, discount_range = active[0]

    days_old = (END_DATE - day).days
    if days_old <= 2:
        status = pick([("pending", 0.6), ("shipped", 0.3), ("cancelled", 0.1)])
    elif days_old <= 10:
        status = pick([("shipped", 0.55), ("completed", 0.35), ("cancelled", 0.07), ("returned", 0.03)])
    else:
        status = pick([("completed", 0.86), ("cancelled", 0.07), ("returned", 0.07)])
    raw_status = status
    if rng.random() < 0.03:
        raw_status = rng.choice([status.capitalize(), status.upper() + " ", " " + status])  # planted

    channel = pick([("web", 0.55), ("app", 0.35), ("marketplace", 0.10)])

    # Order lines
    items_total = Decimal("0")
    for _ in range(pick([(1, 0.45), (2, 0.30), (3, 0.17), (4, 0.08)])):
        if rng.random() < 0.0009:
            sku = rng.choice(list(DELETED_SKUS))           # planted: SKU no longer in products
            list_price = DELETED_SKUS[sku]
        else:
            p = pick(product_weights)
            sku, list_price = p[0], p[4]
        qty = pick([(1, 0.85), (2, 0.12), (3, 0.03)])
        unit_price = money(float(list_price) / rate)
        discount = money(rng.uniform(*discount_range)) if discount_range else Decimal("0.00")
        order_items.append((order_item_id, order_id, sku, qty, unit_price, discount))
        items_total += money(unit_price * qty * (1 - discount))
        order_item_id += 1

    shipping = Decimal("0.00") if float(items_total) * rate >= 89 else money(6.95 / rate)  # free shipping from EUR 89
    order_total = items_total + shipping

    orders.append((order_id, cid, ordered_at, raw_status, channel, currency, shipping, campaign_code,
                   ordered_at + dt.timedelta(hours=rng.randint(1, 30))))

    # Payments
    method = pick(PAYMENT_METHODS[country])
    paid_at = ordered_at + dt.timedelta(minutes=rng.randint(0, 15))
    if status in ("completed", "shipped", "returned"):
        if rng.random() < 0.04:  # first attempt failed, then success with another method
            payments.append((payment_id, order_id, method, order_total, "failed", paid_at)); payment_id += 1
            method = pick(PAYMENT_METHODS[country]); paid_at += dt.timedelta(minutes=rng.randint(1, 20))
        payments.append((payment_id, order_id, method, order_total, "success", paid_at)); payment_id += 1
        if status == "returned":
            refunded_at = paid_at + dt.timedelta(days=rng.randint(7, 30))
            payments.append((payment_id, order_id, method, order_total, "refunded", refunded_at)); payment_id += 1
    elif status == "cancelled" and rng.random() < 0.5:
        payments.append((payment_id, order_id, method, order_total, "failed", paid_at)); payment_id += 1
    elif status == "pending" and rng.random() < 0.3:
        payments.append((payment_id, order_id, method, order_total, "failed", paid_at)); payment_id += 1

    order_id += 1

orders_schema = T.StructType([
    T.StructField("order_id", T.LongType(), False),
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("ordered_at", T.TimestampType(), False),
    T.StructField("status", T.StringType(), True),
    T.StructField("channel", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("shipping_amount", T.DecimalType(10, 2), True),
    T.StructField("campaign_code", T.StringType(), True),
    T.StructField("_loaded_at", T.TimestampType(), True),
])
order_items_schema = T.StructType([
    T.StructField("order_item_id", T.LongType(), False),
    T.StructField("order_id", T.LongType(), False),
    T.StructField("product_sku", T.StringType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("unit_price", T.DecimalType(10, 2), False),
    T.StructField("discount_pct", T.DecimalType(4, 2), False),
])
payments_schema = T.StructType([
    T.StructField("payment_id", T.LongType(), False),
    T.StructField("order_id", T.LongType(), False),
    T.StructField("payment_method", T.StringType(), True),
    T.StructField("amount", T.DecimalType(12, 2), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md ## Write the tables

# COMMAND ----------

tables = {
    "customers": (customers_all, customers_schema, "Customer master data from the webshop backend. One row per customer version."),
    "products": (products, products_schema, "Product catalogue (PIM export). One row per SKU."),
    "orders": (orders, orders_schema, "Webshop orders. One row per order."),
    "order_items": (order_items, order_items_schema, "Order lines. Prices in the order currency."),
    "payments": (payments, payments_schema, "Payment attempts and refunds from the payment provider."),
}

for name, (rows, schema, comment) in tables.items():
    full_name = f"{CATALOG}.{SCHEMA}.{name}"
    (spark.createDataFrame(rows, schema)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(full_name))
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    print(f"{full_name:45} {len(rows):>6} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick look at the raw data
# MAGIC A few checks an analyst would run before modelling. Each one shows something dbt has to handle.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders per month (seasonality and growth)
# MAGIC select date_trunc('MONTH', ordered_at) as order_month, count(*) as orders
# MAGIC from workspace.project_x_raw.orders
# MAGIC group by all
# MAGIC order by order_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Status values are not clean: casing and whitespace differ
# MAGIC select status, count(*) as orders
# MAGIC from workspace.project_x_raw.orders
# MAGIC group by all
# MAGIC order by orders desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Some customers appear more than once
# MAGIC select customer_id, count(*) as versions
# MAGIC from workspace.project_x_raw.customers
# MAGIC group by all
# MAGIC having count(*) > 1
# MAGIC order by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Order lines that point at products that no longer exist
# MAGIC select oi.product_sku, count(*) as order_lines
# MAGIC from workspace.project_x_raw.order_items oi
# MAGIC left join workspace.project_x_raw.products p using (product_sku)
# MAGIC where p.product_sku is null
# MAGIC group by all