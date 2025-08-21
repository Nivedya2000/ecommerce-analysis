
# 🛒 E-Commerce Data Analysis with Databricks & PySpark

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-orange?logo=apachespark)
![Databricks](https://img.shields.io/badge/Databricks-Platform-red?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Enabled-green?logo=databricks)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📌 Project Overview

This project demonstrates how to process, analyze, and visualize **e-commerce transaction data** using **Databricks**, **PySpark**, and **SQL**.
It covers the full pipeline: data preparation, train/test split, exploratory data analysis (EDA), and **advanced analytics (clustering, time-series, anomaly detection)**.

The dataset is stored in Databricks Delta format, and the analysis includes country-level insights, customer behavior, product trends, revenue breakdowns, and predictive extensions.

---

## 🚀 Features

* ✅ **Dynamic catalog detection** – works across Community & Premium Databricks editions
* ✅ **Data preparation** – clean invoice & item tables from raw data
* ✅ **Train/Test split** – customer-level splitting to prevent data leakage
* ✅ **Exploratory Data Analysis (EDA):**

  * Number of countries
  * Top countries by customers
  * Top customers by order volume
  * Most frequently ordered items
  * Price distribution (+ check for negative prices)
  * Revenue-generating items (inside vs. outside UK)
  * Customers who purchased specific products (e.g., *WHITE METAL LANTERN*)
* ✅ **Advanced Analytics:**

  * Customer Segmentation (RFM + Clustering)
  * Time-Series Forecasting (Monthly Sales)
  * Anomaly Detection (Invoice-level revenue outliers)
* ✅ **Visualizations** using Databricks `display()` and Matplotlib

---

## 📂 Project Structure

```
ecommerce-analysis/
│── databricks_ecomm_pipeline.py   # Main Databricks pipeline script
│── README.md                      # Project documentation
│── LICENSE                        # License (MIT)
│── .gitignore                     # Git ignore rules
```

---

## 🛠️ Requirements

* **Databricks Runtime** (with Spark 3.x + Delta support)
* **Python 3.x**
* **Libraries:**

  * PySpark (pre-installed on Databricks)
  * Pandas
  * Matplotlib

---

## ▶️ How to Run

1. Upload `databricks_ecomm_pipeline.py` into your Databricks workspace.
2. Ensure you have a Delta table named `data_ecomm` in your catalog (`hive_metastore.default` or `workspace.default`).
3. Run the notebook cells step by step:

   * Load and prepare data
   * Create invoice and item tables
   * Verify data
   * Train/test split
   * Perform exploratory queries and visualizations
   * Run advanced analytics (clustering, forecasting, anomaly detection)
4. Review insights in Databricks dashboards or plots.

---

## 📊 Key Insights

* The UK contributes the highest number of customers and revenue.
* Some items (e.g., decorative lanterns) dominate sales volume.
* Negative prices exist in the dataset (likely returns/refunds).
* Customer segmentation reveals distinct purchasing clusters.
* Monthly sales trends show seasonal purchase behavior.
* Anomalies (unusually high/low invoices) can indicate fraud or data quality issues.

---

## 🔮 Future Work

* Build ML models for **customer churn prediction**.
* Create **recommendation systems** based on purchase history.
* Deploy dashboards in **Tableau / Power BI** for stakeholders.
* Automate pipeline with **scheduled jobs** in Databricks.

---

## 👩‍💻 Author

Developed by **Nivedya K**
📧 [Email](nivedyak1112@gmail.com) | 🔗 [LinkedIn](https://linkedin.com/in/nivedya-k) | 🐙 [GitHub](https://github.com/Nivedya2000)

---

## 📜 License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.
