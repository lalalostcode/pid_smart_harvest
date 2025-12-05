# PART 3: REAL-TIME STREAMING & VISUALIZATION

## 🎯 Tujuan
Mensimulasikan skenario dunia nyata di mana data masuk secara terus-menerus (*streaming*) dan menyajikan *insight* melalui dashboard interaktif.

## 📡 Real-time Streaming (Apache Kafka)
1.  **Architecture:**
    *   **Producer:** Membaca data "test" (2019-2020) dan mengirimnya ke Kafka Topic (`harvest-weather-stream`) dengan kecepatan 100 data/detik.
    *   **Consumer:**
        *   Mendengarkan topic Kafka.
        *   Load model ML (`.pkl`) yang sudah dilatih.
        *   Melakukan prediksi *on-the-fly*.
        *   Menyimpan hasil prediksi ke MySQL (`fact_crop_prediction`) secara *batch*.
2.  **Why Kafka?**
    *   Decoupling antara sumber data dan pemroses data.
    *   Scalability untuk menangani volume data besar di masa depan (IoT sensors).

## 📊 Visualization (Streamlit Dashboard)
1.  **Fitur Utama:**
    *   **Interactive Filters:** Filter berdasarkan Tahun, Komoditas (Multiselect), dan Provinsi.
    *   **Dynamic Charts:** Line Chart (Trend), Bar Chart (Top Provinces), Pie Chart (Production Share).
    *   **ML Simulator:** Fitur interaktif untuk "bermain" dengan parameter cuaca (Suhu, Hujan) dan melihat prediksi hasil panen secara langsung.
2.  **Tech Stack:**
    *   Python (Streamlit, Plotly).
    *   Direct Connection ke MySQL Data Warehouse.

## 🔑 Key Takeaways
*   Integrasi Kafka memungkinkan sistem bereaksi terhadap data baru secara instan.
*   Dashboard Streamlit memberikan *actionable insight* bagi pengambil keputusan.
*   Sistem ini adalah *End-to-End Solution*: Dari Raw Data -> Warehouse -> AI -> Dashboard.
