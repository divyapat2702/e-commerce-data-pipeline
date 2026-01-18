# E-Commerce Analytics Data Pipeline (Azure Databricks)

An end-to-end **E-Commerce Analytics Platform** built on **Azure Data Lake + Azure Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)**.
The curated Gold layer powers **Power BI dashboards** for sales, customer, and operational analytics.

---

## 📌 Project Overview

This project demonstrates a **scalable, production-grade data engineering pipeline** for an e-commerce platform (**ShopVista**) using modern lakehouse principles.

**Key highlights:**

* Medallion architecture (Bronze, Silver, Gold)
* Dimensional + Fact modeling
* Databricks ETL/ELT with Unity Catalog
* Secure access to Azure Data Lake (ADLS Gen2)
* Power BI semantic layer and dashboards

---

## 🏗️ Architecture

### High-Level Pipeline Architecture

![Pipeline Architecture](project_architecture.png)

**Flow:**

1. **Source System (ShopVista)**

   * Operational data exported as CSV files
2. **Azure Data Lake Storage (ADLS Gen2)**

   * Centralized raw storage
3. **Azure Databricks + Unity Catalog**

   * ETL/ELT processing
   * Data governance and access control
4. **Medallion Layers**

   * **Bronze**: Raw ingestion
   * **Silver**: Cleaned, standardized data
   * **Gold**: Aggregated, analytics-ready tables
5. **Power BI**

   * Reporting & analytics layer

---

## 📊 Analytics Dashboard

### E-Commerce Executive Dashboard (Power BI)

![E-Commerce Dashboard](ecommerce_analytics_report.jpg)

**Key Metrics:**

* Total Sales & Units Sold
* Repeat Customer Rate
* Customer Count by Region
* Revenue Trends (Monthly)
* Channel Performance (Mobile vs Website)
* Brand & Category Sales Contribution

---

## 🗂️ Repository Structure

```text
e-commerce-data-pipeline/
│
├── 1_Setup/
│   ├── setup-raw.py
│   └── setup_catalog.py
│
├── 2_medallion_processing_dim/
│   ├── 1_dim_bronze.py
│   ├── 2_dim_silver.py
│   └── 3_dim_gold.py
│
├── 3_medallion_processing_fact/
│   ├── 1_brnz_fact.py
│   ├── 1_brnz_fact_shipments.py
│   ├── 1_brnz_fact_ordr_rturn.py
│   ├── 2_slvr_fact.py
│   ├── 2_slvr_fact_ordr_shipments.py
│   ├── 2_slvr_fact_ordr_rturn.py
│   ├── 3_gold_fact.py
│   ├── 3_gold_fact_ordr_shipments.py
│   ├── 3_gold_fact_ordr_rturn.py
│   ├── 4_daily_summary.py
│   ├── 4_monthly_order_shipment_summary.py
│   └── 4_monthly_order_return_summary.py
│
└── manifest.mf
```

---

## 🧱 Medallion Architecture Details

### 🥉 Bronze Layer

* Raw ingestion from CSV files
* Minimal transformations
* Schema enforcement
* Audit columns (ingestion timestamp, source)

**Scripts:**

* `1_dim_bronze.py`
* `1_brnz_fact*.py`

---

### 🥈 Silver Layer

* Data cleansing & standardization
* Deduplication
* Business rule validation
* Referential integrity between facts & dimensions

**Scripts:**

* `2_dim_silver.py`
* `2_slvr_fact*.py`

---

### 🥇 Gold Layer

* Aggregated, analytics-ready tables
* Star schema aligned
* Optimized for Power BI consumption

**Outputs include:**

* Sales facts
* Order, shipment & return summaries
* Daily & monthly KPIs

**Scripts:**

* `3_dim_gold.py`
* `3_gold_fact*.py`
* `4_*_summary.py`

---

## ⚙️ Setup & Deployment

### Prerequisites

* Azure Subscription
* Azure Data Lake Storage Gen2
* Azure Databricks (with Unity Catalog enabled)
* Power BI Desktop / Power BI Service

---

### Step 1: Environment Setup

Run the setup scripts in Databricks:

```bash
1_Setup/setup-raw.py
1_Setup/setup_catalog.py
```

This will:

* Create catalogs, schemas, and external locations
* Configure Unity Catalog permissions

---

### Step 2: Bronze Ingestion

Run Bronze notebooks/scripts to ingest raw CSV data into ADLS-backed tables.

---

### Step 3: Silver Transformation

Execute Silver layer scripts to clean and standardize data.

---

### Step 4: Gold Aggregation

Run Gold layer scripts to generate analytical tables and summaries.

---

### Step 5: Power BI Integration

* Connect Power BI to Databricks SQL / Gold tables
* Build semantic model
* Publish dashboards

---

## 🔐 Security & Governance

* Unity Catalog for:

  * Table-level and column-level security
  * Centralized data governance
* Secure ADLS access via managed identity / access connector

---

## 🚀 Future Enhancements

* Incremental loads using Delta Lake
* CI/CD with Azure DevOps / GitHub Actions
* Data quality checks with expectations
* Real-time ingestion (Event Hub / Kafka)
* Advanced customer segmentation & ML models

---

## 👤 Author

**Divya Pathak**
*Data Engineer | Azure | Databricks | Power BI*

---

## 📄 License

This project is for **educational and portfolio purposes**.
You are free to adapt and extend it for learning or internal use.

---

⭐ If you find this project useful, don’t forget to **star the repository**!
