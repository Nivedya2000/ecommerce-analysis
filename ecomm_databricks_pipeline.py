# Databricks notebook source
# MAGIC %md
# MAGIC ## 🚀 Step 1: Start Spark Session

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("✅ Spark session created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Step 2: Detect Catalog (works for Community & Premium editions)

def get_catalog():
    try:
        spark.sql("SHOW TABLES IN hive_metastore.default")
        return "hive_metastore"
    except Exception:
        try:
            spark.sql("SHOW TABLES IN main.default")
            return "main"
        except Exception:
            return "workspace"  # Fallback; replace with your actual catalog if needed

CATALOG = get_catalog()
print(f"📂 Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 3: Load Existing Delta Table

try:
    # Load the existing Delta table
    df = spark.sql(f"SELECT * FROM {CATALOG}.default.data_ecomm")
    
    # Verify schema and sample data
    df.printSchema()
    display(df.limit(10))
    
    # Summary statistics
    df.describe().show()
    
    print("✅ Loaded existing Delta table 'data_ecomm'.")
except Exception as e:
    print(f"Error loading table: {str(e)}")
    print("Debugging steps:")
    print(f"1. Check catalog with `print(spark.catalog.currentCatalog())`")
    print(f"2. Verify table exists with `spark.sql('SHOW TABLES IN {CATALOG}.default').show()`")
    print("3. Ensure you have access to the table.")

# COMMAND ----------


# Aggregate top products
top_products = spark.sql(f"""
SELECT Description, SUM(UnitPrice * Quantity) as revenue
FROM {CATALOG}.default.items
GROUP BY Description
ORDER BY revenue DESC
LIMIT 10
""").toPandas()

plt.figure(figsize=(12, 6))
plt.barh(top_products["Description"], top_products["revenue"], color="#FFCC99")
plt.gca().invert_yaxis()  # Highest on top
plt.title("Top 10 Products by Revenue")
plt.xlabel("Revenue")
plt.ylabel("Product")
plt.tight_layout()

display(plt.gcf())
plt.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧾 Step 4: Create Invoices & Items Tables

try:
    # Invoices Table
    customer_columns = ["CustomerID", "Country", "InvoiceDate", "InvoiceNo"]
    df_invoice = df.select(customer_columns).dropDuplicates(["InvoiceNo"])
    display(df_invoice)

    # Print row and column count
    print(f"n_row={df_invoice.count()}, n_col={len(df_invoice.columns)}")

    # Drop existing table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.default.invoices")
    df_invoice.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.invoices")

    # Items Table
    item_columns = ["StockCode", "Description", "UnitPrice", "Quantity", "InvoiceNo"]
    df_items = df.select(item_columns).dropDuplicates()
    display(df_items)

    # Drop existing table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.default.items")
    df_items.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.default.items")

    print("✅ Invoices and Items tables created successfully.")
except Exception as e:
    print(f"Error occurred: {str(e)}")
    print("Debugging steps:")
    print("1. Ensure Cell 3 ran successfully to define 'df'.")
    print(f"2. Check catalog with `print(spark.catalog.currentCatalog())`")
    print(f"3. Verify tables with `spark.sql('SHOW TABLES IN {CATALOG}.default').show()`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Step 5: Verify Tables

spark.sql(f"SHOW TABLES IN {CATALOG}.default").show(truncate=False)
print("Invoices count:", spark.table(f"{CATALOG}.default.invoices").count())
print("Items count:", spark.table(f"{CATALOG}.default.items").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Step 6: Train / Test Split (Customer-level to prevent leakage)

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

from pyspark.sql.functions import col

invoices = spark.table(f"{CATALOG}.default.invoices")
items = spark.table(f"{CATALOG}.default.items")

# Placeholder for EDA queries (add your own as needed)
# Example: Number of unique countries
unique_countries = invoices.select("Country").distinct().count()
print(f"Number of unique countries: {unique_countries}")

# Example: Top 5 countries by invoice count
top_countries = invoices.groupBy("Country").count().orderBy(col("count").desc()).limit(5)
display(top_countries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Bar Chart: Invoice Count by Top 5 Countries

import matplotlib.pyplot as plt

# Close all existing figures to prevent duplication
plt.close('all')

# Debug: Check number of open figures before creating new one
print(f"Number of open figures before: {len(plt.get_fignums())}")

# Calculate top 5 countries by invoice count
top_countries = invoices.groupBy("Country").count().orderBy(col("count").desc()).limit(5)

# Convert to Pandas for plotting
pandas_df = top_countries.toPandas()

# Create a new figure
plt.figure(figsize=(10, 6))

# Create bar chart
plt.bar(pandas_df['Country'], pandas_df['count'], color=['#FF9999', '#66B2FF', '#99FF99', '#FFCC99', '#FF99CC'])
plt.xlabel('Country')
plt.ylabel('Invoice Count')
plt.title('Invoice Count by Top 5 Countries')
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)  # Add grid for readability
plt.tight_layout()

# Debug: Check number of open figures after creation
print(f"Number of open figures after: {len(plt.get_fignums())}")

# Display the single figure
display(plt.gcf())

# Close the figure after display to prevent caching
plt.close()

# Optional: Print confirmation
print("Bar chart generated for top 5 countries.")

# COMMAND ----------

from pyspark.sql.functions import datediff, lit, to_timestamp, col, max as spark_max

# Step 1: Create RFM dataframe
rfm_df = spark.sql(f"""
SELECT inv.CustomerID,
       MAX(inv.InvoiceDate) AS last_purchase,
       COUNT(DISTINCT itm.InvoiceNo) AS frequency,
       SUM(itm.UnitPrice * itm.Quantity) AS monetary
FROM {CATALOG}.default.items itm
JOIN {CATALOG}.default.invoices inv
  ON itm.InvoiceNo = inv.InvoiceNo
WHERE inv.CustomerID IS NOT NULL
GROUP BY inv.CustomerID
""")

print(f"rfm_df row count: {rfm_df.count()}")
rfm_df.printSchema()
display(rfm_df.limit(10))

# Step 2: Get max invoice date (most recent purchase overall)
max_date = spark.table(f"{CATALOG}.default.invoices") \
                .agg(spark_max("InvoiceDate").alias("max_date")) \
                .collect()[0]['max_date']
print(f"🗓️ Max invoice date: {max_date}")

# Step 3: Convert string dates into timestamp using the correct format
rfm_df = rfm_df.withColumn("last_purchase_date", to_timestamp(col("last_purchase"), "M/d/yyyy H:mm"))
rfm_df = rfm_df.withColumn("max_date_converted", to_timestamp(lit(max_date), "M/d/yyyy H:mm"))

# Step 4: Calculate recency (difference in days)
rfm_df = rfm_df.withColumn("recency", datediff(col("max_date_converted"), col("last_purchase_date")))

print("✅ Recency calculation completed successfully")
rfm_df.printSchema()
display(rfm_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 RFM Clustering

import os
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import threadpoolctl

# Limit threading to reduce warnings
os.environ["OMP_NUM_THREADS"] = "1"
threadpoolctl.threadpool_limits(1, "blas")

# Configure Matplotlib for Databricks
%matplotlib inline

try:
    # Convert Spark DataFrame to Pandas
    rfm_pandas = rfm_df.select("CustomerID", "recency", "frequency", "monetary").toPandas()

    # Clean and prepare data
    rfm_pandas = rfm_pandas.dropna(subset=["recency", "frequency", "monetary"])
    rfm_pandas["CustomerID"] = pd.to_numeric(rfm_pandas["CustomerID"], errors="coerce").fillna(0).astype(int)
    rfm_pandas["recency"] = rfm_pandas["recency"].clip(lower=0).astype(float)
    rfm_pandas["frequency"] = rfm_pandas["frequency"].clip(lower=0, upper=rfm_pandas["frequency"].quantile(0.99)).astype(float)
    rfm_pandas["monetary"] = rfm_pandas["monetary"].clip(lower=0, upper=rfm_pandas["monetary"].quantile(0.99)).astype(float)

    # Scale features
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(rfm_pandas[["recency", "frequency", "monetary"]])

    # Run KMeans clustering
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    rfm_pandas["cluster"] = kmeans.fit_predict(rfm_scaled)

    # Display sample results
    print("Sample clustered data:")
    display(rfm_pandas.head(10))

    # Scatter plot: Recency vs Monetary
    plt.figure(figsize=(8, 6))
    scatter = plt.scatter(
        rfm_pandas["recency"],
        rfm_pandas["monetary"],
        c=rfm_pandas["cluster"],
        cmap="viridis",
        alpha=0.7,
        edgecolors="k"
    )
    plt.xlabel("Recency (days)")
    plt.ylabel("Monetary Value")
    plt.title("Customer Segmentation")
    plt.colorbar(scatter, label="Cluster")
    plt.grid(True, linestyle="--", alpha=0.5)
    display(plt.gcf())
    plt.close()

    # Cluster summary
    cluster_summary = rfm_pandas.groupby("cluster").agg({
        "recency": "mean",
        "frequency": "mean",
        "monetary": "mean",
        "CustomerID": "count"
    }).rename(columns={"CustomerID": "num_customers"})

    print("\nCluster Summary:")
    display(cluster_summary.round(2))

except Exception as e:
    print(f"Error: {str(e)}")
    print("Debugging tips:")
    print("- Ensure Cell 10 ran to create rfm_df: rfm_df.show(5)")
    print("- Install dependencies: %pip install scikit-learn pandas matplotlib threadpoolctl")
    print("- Clear notebook state or restart cluster if outputs don't display.")

# COMMAND ----------

# Convert RFM Spark DataFrame to Pandas
rfm_pandas = rfm_df.select("recency", "frequency", "monetary").toPandas()

plt.figure(figsize=(16, 5))

# Recency
plt.subplot(1, 3, 1)
plt.hist(rfm_pandas["recency"], bins=30, color="#66B2FF", edgecolor="black")
plt.title("Recency Distribution")
plt.xlabel("Days Since Last Purchase")
plt.ylabel("Customers")

# Frequency
plt.subplot(1, 3, 2)
plt.hist(rfm_pandas["frequency"], bins=30, color="#99FF99", edgecolor="black")
plt.title("Frequency Distribution")
plt.xlabel("Number of Purchases")
plt.ylabel("Customers")

# Monetary
plt.subplot(1, 3, 3)
plt.hist(rfm_pandas["monetary"], bins=30, color="#FF9999", edgecolor="black")
plt.title("Monetary Distribution")
plt.xlabel("Revenue")
plt.ylabel("Customers")

plt.tight_layout()
display(plt.gcf())
plt.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Time-Series Forecasting (Monthly Sales Trend)

from pyspark.sql.functions import date_trunc, to_timestamp

monthly_sales = spark.sql(f"""
SELECT date_trunc('month', to_timestamp(inv.InvoiceDate, 'M/d/yyyy H:mm')) as month,
       SUM(i.UnitPrice * i.Quantity) as revenue
FROM {CATALOG}.default.items i
JOIN {CATALOG}.default.invoices inv
  ON i.InvoiceNo = inv.InvoiceNo
GROUP BY date_trunc('month', to_timestamp(inv.InvoiceDate, 'M/d/yyyy H:mm'))
ORDER BY month
""")

display(monthly_sales)

# COMMAND ----------

# Convert monthly sales to Pandas
monthly_sales_pd = monthly_sales.toPandas()

plt.figure(figsize=(12, 6))
plt.plot(monthly_sales_pd["month"], monthly_sales_pd["revenue"], marker="o", linestyle="-", color="#66B2FF")
plt.title("Monthly Sales Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.grid(True, linestyle="--", alpha=0.7)
plt.xticks(rotation=45)
plt.tight_layout()

display(plt.gcf())
plt.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 Anomaly Detection (Unusual Transactions)

from pyspark.sql.functions import mean as _mean, stddev as _stddev, col

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Bar Chart: Normal vs. Anomalous Transactions

import matplotlib.pyplot as plt

# Close all existing figures to prevent duplication
plt.close('all')

# Debug: Check number of open figures before creating new one
print(f"Number of open figures before: {len(plt.get_fignums())}")

# Calculate counts for normal and anomalous transactions
normal_count = invoice_revenue.count() - anomalies.count()
anomaly_count = anomalies.count()

# Convert to Pandas for plotting
import pandas as pd
data = [normal_count, anomaly_count]
pandas_df = pd.DataFrame({"Transaction Type": ["Normal Transactions", "Anomalous Transactions"], "Count": data})

# Create a new figure
plt.figure(figsize=(10, 6))

# Create bar chart
plt.bar(pandas_df['Transaction Type'], pandas_df['Count'], color=['#FF9999', '#66B2FF'])
plt.xlabel('Transaction Type')
plt.ylabel('Count')
plt.title('Normal vs. Anomalous Transactions')
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)  # Add grid for readability
plt.tight_layout()

# Debug: Check number of open figures after creation
print(f"Number of open figures after: {len(plt.get_fignums())}")

# Display the single figure
display(plt.gcf())

# Close the figure after display to prevent caching
plt.close()

# Optional: Print confirmation
print("Bar chart generated for normal vs. anomalous transactions.")