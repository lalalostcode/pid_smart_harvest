# PART 1: DATA ENGINEERING & ARCHITECTURE

## 🎯 Tujuan
Membangun infrastruktur data yang kokoh untuk mengolah data mentah (CSV) menjadi Data Warehouse yang siap untuk analisis dan Machine Learning.

## 🏗️ Arsitektur Sistem (Modern Data Stack)
1.  **Data Sources:**
    *   BPS (Produksi Pangan 2010-2015) - CSV Tahunan.
    *   Kaggle (Data Iklim 2010-2020) - CSV Harian.
2.  **ETL Process (Python Pandas):**
    *   **Extract:** Membaca raw data.
    *   **Transform:**
        *   Cleaning & Standardisasi nama provinsi.
        *   Imputasi data hilang.
        *   *Linear Interpolation* untuk mengisi data panen 2016-2022.
        *   *Disaggregation* (Tahunan -> Bulanan).
        *   *Aggregation* (Cuaca Harian -> Bulanan).
    *   **Load:** Insert ke MySQL.
3.  **Data Warehouse (MySQL 8.0):**
    *   **Star Schema Design:**
        *   **Fact Tables:** `fact_production_monthly`, `fact_weather_monthly`.
        *   **Dimension Tables:** `dim_time`, `dim_province`, `dim_commodity`.
4.  **Orchestration (Apache Airflow):**
    *   Mengotomatisasi workflow ETL agar berjalan terjadwal dan terpantau.

## 🔑 Key Takeaways
*   Transformasi data yang kompleks diperlukan untuk menyelaraskan granularitas data (Tahunan vs Harian -> Bulanan).
*   Desain Star Schema memudahkan query analitik dan ML.
*   Containerization (Docker) memastikan lingkungan yang konsisten.
