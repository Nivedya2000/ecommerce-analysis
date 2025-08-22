# 🛒 E-Commerce Data Analysis with Databricks & PySpark

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-orange?logo=apachespark)
![Databricks](https://img.shields.io/badge/Databricks-Platform-red?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Enabled-green?logo=databricks)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

---

## 📌 Project Overview
This project demonstrates how to process, analyze, and visualize **e-commerce transaction data** using **Databricks**, **PySpark**, and **SQL**.  

It covers the full pipeline:
- Data ingestion & preparation  
- Train/Test split (customer-level)  
- Exploratory Data Analysis (EDA)  
- Advanced analytics (Customer Segmentation, Time-Series Forecasting, Anomaly Detection)  

The dataset is stored in **Delta format**. The pipeline extracts insights about **customer behavior, country-level trends, product performance, and sales anomalies**.

---

## 🚀 Features
- ✅ **Dynamic catalog detection** (works across Community & Premium Databricks editions)  
- ✅ **Clean data preparation** – build invoice & item tables  
- ✅ **Train/Test split** at customer-level to avoid leakage  
- ✅ **Exploratory Data Analysis (EDA)**:
  - Countries & customers  
  - Top products & revenue drivers  
  - Order volume trends  
  - Refund/negative price checks  
- ✅ **Advanced Analytics**:
  - RFM Segmentation + KMeans Clustering  
  - Monthly Sales Trend Forecasting  
  - Anomaly Detection in Invoice Revenue  
- ✅ **Interactive Visualizations** with Databricks `display()` and Matplotlib  

---

## 📂 Project Structure
```
ecommerce-analysis/
│── ecomm_databricks_pipeline.py   # Main Databricks pipeline script
│── README.md                      # Project documentation
│── LICENSE                        # License (MIT)
│── .gitignore                     # Git ignore rules
```

---

## 🛠️ Requirements
- **Databricks Runtime** (Spark 3.x + Delta support)  
- **Python 3.x**  
- Libraries:
  - PySpark (pre-installed in Databricks)  
  - Pandas  
  - Matplotlib  
  - scikit-learn (for clustering)  

---

## ▶️ How to Run
1. Upload `ecomm_databricks_pipeline.py` into your **Databricks workspace**.  
2. Ensure you have a Delta table named **`data_ecomm`** in your catalog (`hive_metastore.default` or `workspace.default`).  
3. Run cells in order:  
   - Load & prepare data  
   - Create `invoices` & `items` tables  
   - Verify tables  
   - Perform EDA & visualizations  
   - Run advanced analytics (clustering, forecasting, anomaly detection)  
4. Review insights in Databricks dashboards/plots.  

---

## 📊 Key Insights
- 🇬🇧 **UK dominates** in customer count & revenue.  
- 📦 Certain products (e.g., decorative lanterns) dominate sales.  
- ⚠️ Negative prices = refunds/returns.  
- 👥 **Customer segmentation** reveals distinct groups of buyers.  
- 📈 Monthly sales trends highlight seasonal demand.  
- 🚨 Outliers in invoices may indicate fraud or data issues.  

---

## 🔮 Future Enhancements
- Customer churn prediction  
- Product recommendation system  
- Dashboard integration (Tableau / Power BI)  
- Pipeline automation with scheduled Databricks jobs  

---

## 👩‍💻 Author
Developed by **Nivedya K**  
📧 [Email](mailto:nivedyak1112@gmail.com) | 🔗 [LinkedIn](https://linkedin.com/in/nivedya-k) | 🐙 [GitHub](https://github.com/Nivedya2000)

---

## 📜 License
This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.
