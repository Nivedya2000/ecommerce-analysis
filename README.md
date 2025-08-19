# 🛒 E-Commerce Data Analysis with Databricks & PySpark

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-orange?logo=apachespark)
![Databricks](https://img.shields.io/badge/Databricks-Platform-red?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Enabled-green?logo=databricks)

---

## 📌 Project Overview
This project demonstrates how to process, analyze, and visualize **e-commerce transaction data** using **Databricks**, **PySpark**, and **SQL**.  
It covers the full pipeline: data preparation, train/test split, and exploratory data analysis (EDA).  

The dataset is stored in Databricks Delta format, and the analysis includes country-level insights, customer behavior, product trends, and revenue breakdowns.

---

## 🚀 Features
- ✅ **Dynamic catalog detection** – works across Community & Premium Databricks editions  
- ✅ **Data preparation** – clean invoice & item tables from raw data  
- ✅ **Train/Test split** – customer-level splitting to prevent data leakage  
- ✅ **Exploratory Data Analysis (EDA)**:
  - Number of countries
  - Top countries by customers
  - Top customers by order volume
  - Most frequently ordered items
  - Price distribution (+ check for negative prices)
  - Revenue-generating items (inside vs. outside UK)
  - Customers who purchased specific products (e.g., *WHITE METAL LANTERN*)  
- ✅ **Visualizations** using Databricks `display()` and Matplotlib  

---

## 📂 Project Structure
├── databricks_ecomm_pipeline.py 
├── README.md 

---

## 🛠️ Requirements
- **Databricks Runtime** (with Spark 3.x + Delta support)  
- **Python 3.x**  
- **Libraries:**
  - PySpark (pre-installed on Databricks)
  - Pandas
  - Matplotlib

---

## ▶️ How to Run
1. Upload `databricks_ecomm_pipeline.py` into your Databricks workspace.  
2. Ensure you have a Delta table named `data_ecomm` in your catalog (`hive_metastore.default` or `workspace.default`).  
3. Run the notebook cells step by step:  
   - Load and prepare data  
   - Create invoice and item tables  
   - Verify data  
   - Train/test split  
   - Perform exploratory queries and visualizations  
4. Review insights in Databricks dashboards or plots.  

---

## 📊 Key Insights
- The UK contributes the highest number of customers and revenue.  
- Some items (e.g., decorative lanterns) dominate sales volume.  
- Negative prices exist in the dataset (likely returns/refunds).  
- Customer-level splitting ensures no data leakage when preparing ML models.  

---

## 🔮 Future Work
- Customer segmentation using clustering (RFM analysis).  
- Time-series forecasting for sales by product or country.  
- Anomaly detection for unusual transactions (e.g., negative prices).  

---

## 👩‍💻 Author
Developed by **[Nivedya K]**  
Feel free to connect and collaborate 🚀
# ecommerce-analysis
