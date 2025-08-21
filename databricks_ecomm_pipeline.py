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

print(f"n_row={df_invoice.count()}, n_col={len(df_invoice.columns)})")

df_invoice.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.invoices")

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

# --- EDA code omitted for brevity (as in original) ---

# COMMAND ----------
# MAGIC %md
# MAGIC # 📝 Key Insights Summary
# MAGIC - Dataset covers multiple countries with varying customer distributions.
# MAGIC - UK is the largest market with the highest revenue contribution.
# MAGIC - Negative prices exist (likely returns or data errors).
# MAGIC - A few items (e.g., lanterns, decorative goods) dominate order volume and revenue.
# MAGIC - Splitting train/test at **customer level** avoids data leakage for ML tasks.
# MAGIC - Delta tables created for reproducibility and efficiency.

# COMMAND ----------
# MAGIC %md
# MAGIC # 🔮 Future Work Extensions

# COMMAND ----------
# MAGIC %md
# MAGIC ## 👥 Customer Segmentation (RFM Analysis + Clustering)

# COMMAND ----------
from pyspark.sql.functions import datediff, lit, max as spark_max, sum as spark_sum, count as spark_count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

rfm_df = spark.sql(f"""
SELECT CustomerID,
       MAX(InvoiceDate) AS last_purchase,
       COUNT(DISTINCT InvoiceNo) AS frequency,
       SUM(UnitPrice * Quantity) AS monetary
FROM {CATALOG}.default.items i
JOIN {CATALOG}.default.invoices inv
  ON i.InvoiceNo = inv.InvoiceNo
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID
""")

max_date = spark.sql(f"SELECT MAX(InvoiceDate) as max_date FROM {CATALOG}.default.invoices").collect()[0]['max_date']
rfm_df = rfm_df.withColumn("recency", datediff(lit(max_date), col("last_purchase")))

assembler = VectorAssembler(inputCols=["recency", "frequency", "monetary"], outputCol="features")
rfm_features = assembler.transform(rfm_df.na.fill(0))

kmeans = KMeans(k=4, seed=42)
model = kmeans.fit(rfm_features)
rfm_result = model.transform(rfm_features)

display(rfm_result.select("CustomerID", "recency", "frequency", "monetary", "prediction"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Time-Series Forecasting (Monthly Sales Trend)

# COMMAND ----------
from pyspark.sql.functions import date_trunc

monthly_sales = spark.sql(f"""
SELECT date_trunc('month', inv.InvoiceDate) as month,
       SUM(i.UnitPrice * i.Quantity) as revenue
FROM {CATALOG}.default.items i
JOIN {CATALOG}.default.invoices inv
  ON i.InvoiceNo = inv.InvoiceNo
GROUP BY date_trunc('month', inv.InvoiceDate)
ORDER BY month
""")

display(monthly_sales)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚨 Anomaly Detection (Unusual Transactions)

# COMMAND ----------
from pyspark.sql.functions import mean as _mean, stddev as _stddev

invoice_revenue = spark.sql(f"""
SELECT inv.InvoiceNo,
       SUM(i.UnitPrice * i.Quantity) as total_invoice_revenue
FROM {CATALOG}.default.items i
JOIN {CATALOG}.default.invoices inv
  ON i.InvoiceNo = inv.InvoiceNo
GROUP BY inv.InvoiceNo
""")

stats_values = invoice_revenue.agg(
    _mean("total_invoice_revenue").alias("mean"),
    _stddev("total_invoice_revenue").alias("std")
).collect()[0]

mean_val, std_val = stats_values["mean"], stats_values["std"]
threshold_low = mean_val - 3 * std_val
threshold_high = mean_val + 3 * std_val

anomalies = invoice_revenue.filter(
    (col("total_invoice_revenue") < threshold_low) | 
    (col("total_invoice_revenue") > threshold_high)
)

display(anomalies)
