# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚀 Step 1: Start Spark Session

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
print("✅ Spark session created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📂 Step 2: Detect Catalog (works for Community & Premium editions)

# COMMAND ----------
def get_catalog():
    try:
        spark.sql("SHOW TABLES IN hive_metastore.default")
        return "hive_metastore"
    except Exception:
        return "workspace"

CATALOG = get_catalog()
print(f"📂 Using catalog: {CATALOG}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Step 3: Load Raw Data

# COMMAND ----------
df = spark.sql(f"SELECT * FROM {CATALOG}.default.data_ecomm")
display(df)

# COMMAND ----------
df.printSchema()
df.describe().show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🧾 Step 4: Create Invoices & Items Tables

# COMMAND ----------
# Invoices Table
customer_columns = ["CustomerID", "Country", "InvoiceDate", "InvoiceNo"]
df_invoice = df.select(customer_columns).drop_duplicates(["InvoiceNo"])
display(df_invoice)

print(f"n_row={df_invoice.count()}, n_col={len(df_invoice.columns)}")

df_invoice.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.invoices")

# COMMAND ----------
# Items Table
item_columns = ["StockCode", "Description", "UnitPrice", "Quantity", "InvoiceNo"]
df_items = df.select(item_columns).drop_duplicates()
display(df_items)

df_items.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.items")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 Step 5: Verify Tables

# COMMAND ----------
spark.sql(f"SHOW TABLES IN {CATALOG}.default").show(truncate=False)
print("Invoices count:", spark.table(f"{CATALOG}.default.invoices").count())
print("Items count:", spark.table(f"{CATALOG}.default.items").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🤖 Step 6: Train / Test Split (Customer-level to prevent leakage)

# COMMAND ----------
customers = df.select("CustomerID").distinct()
train_customers, test_customers = customers.randomSplit([0.8, 0.2], seed=42)

train_df = df.join(train_customers, "CustomerID")
test_df = df.join(test_customers, "CustomerID")

print(f"Train rows: {train_df.count()}, Test rows: {test_df.count()}")

train_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.train_data")
test_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.test_data")

print("✅ Train/Test tables created successfully")

# COMMAND ----------
# MAGIC %md
# MAGIC # 📈 Exploratory Data Analysis

# COMMAND ----------
invoices = spark.table(f"{CATALOG}.default.invoices")
items = spark.table(f"{CATALOG}.default.items")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 🌍 Number of Countries

# COMMAND ----------
result_df = spark.sql(f"""
SELECT COUNT(DISTINCT Country) AS Number_countries
FROM {CATALOG}.default.invoices
""")
display(result_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 🏆 Top 10 Countries with Most Customers

# COMMAND ----------
resultDF = spark.sql(f"""
SELECT Country,
       COUNT(DISTINCT CustomerID) as TotalClientNumber
FROM {CATALOG}.default.invoices
GROUP BY Country
ORDER BY TotalClientNumber DESC
LIMIT 10
""")
display(resultDF)

# COMMAND ----------
# Using PySpark + Matplotlib
data = invoices.groupBy("Country").agg(countDistinct("CustomerID").alias("UniqueCustomerNumber"))
data_pd = data.limit(100000).toPandas().sort_values(by="UniqueCustomerNumber", ascending=False).head(20)

plt.rcParams["figure.figsize"] = (20, 3)
data_plot = pd.DataFrame(data_pd, columns=["Country", "UniqueCustomerNumber"])
data_plot.plot(kind='bar', x='Country', y='UniqueCustomerNumber')
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 👥 Top Customers by Orders

# COMMAND ----------
result_df = spark.sql(f"""
SELECT CustomerID,
       COUNT(DISTINCT InvoiceNo) as TotalOrderNumber
FROM {CATALOG}.default.invoices
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID
ORDER BY TotalOrderNumber DESC
LIMIT 10
""")
display(result_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 📦 Distribution of Items Per Order

# COMMAND ----------
result_df = spark.sql(f"""
SELECT StockCode,
       COUNT(DISTINCT InvoiceNo) AS OrderCount
FROM {CATALOG}.default.items
GROUP BY StockCode
""")
display(result_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 🛒 Most Ordered Items

# COMMAND ----------
most_ordered_items_df = spark.sql(f"""
SELECT StockCode,
       Description,
       SUM(Quantity) AS TotalQuantity
FROM {CATALOG}.default.items
GROUP BY StockCode, Description
ORDER BY TotalQuantity DESC
LIMIT 10
""")
display(most_ordered_items_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 💲 Price Distribution & Negative Price Check

# COMMAND ----------
price_df = spark.sql(f"""
SELECT UnitPrice
FROM {CATALOG}.default.items
WHERE UnitPrice IS NOT NULL
""")

print("Negative prices:")
display(price_df.filter("UnitPrice < 0"))

price_df.describe().show()

price_df_pd = price_df.limit(100000).toPandas()
plt.rcParams["figure.figsize"] = (10, 8)
plt.hist(price_df_pd['UnitPrice'], bins=100)
plt.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 🔦 Customers Who Bought WHITE METAL LANTERN

# COMMAND ----------
white_metal_lantern_df = spark.sql(f"""
SELECT DISTINCT invoices.CustomerID
FROM {CATALOG}.default.items AS items
JOIN {CATALOG}.default.invoices AS invoices
  ON items.InvoiceNo = invoices.InvoiceNo
WHERE items.Description = 'WHITE METAL LANTERN'
  AND invoices.CustomerID IS NOT NULL
""")
display(white_metal_lantern_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 💰 Top Revenue Generating Items (Outside UK)

# COMMAND ----------
result = spark.sql(f"""
SELECT items.Description,
       SUM(items.UnitPrice * items.Quantity) AS total_revenue,
       invoices.Country
FROM {CATALOG}.default.items AS items
JOIN {CATALOG}.default.invoices AS invoices
  ON items.InvoiceNo = invoices.InvoiceNo
WHERE invoices.Country != "United Kingdom"
GROUP BY items.Description, invoices.Country
ORDER BY total_revenue DESC, invoices.Country, items.Description
""")
display(result)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 💰 Top Revenue Generating Items (UK)

# COMMAND ----------
result = spark.sql(f"""
SELECT items.Description,
       SUM(items.UnitPrice * items.Quantity) AS total_revenue,
       invoices.Country
FROM {CATALOG}.default.items AS items
JOIN {CATALOG}.default.invoices AS invoices
  ON items.InvoiceNo = invoices.InvoiceNo
WHERE invoices.Country = "United Kingdom"
GROUP BY items.Description, invoices.Country
ORDER BY total_revenue DESC, invoices.Country, items.Description
""")
display(result)

# COMMAND ----------
# MAGIC %md
# MAGIC # 📝 Key Insights Summary
# MAGIC - Dataset covers multiple countries with varying customer distributions.
# MAGIC - UK is the largest market with the highest revenue contribution.
# MAGIC - Negative prices exist (likely returns or data errors).
# MAGIC - A few items (e.g., lanterns, decorative goods) dominate order volume and revenue.
# MAGIC - Splitting train/test at **customer level** avoids data leakage for ML tasks.
# MAGIC - Delta tables created for reproducibility and efficiency.
