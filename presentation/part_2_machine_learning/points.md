# PART 2: MACHINE LEARNING & PREDICTION

## 🎯 Tujuan
Mengembangkan model prediktif untuk mengestimasi hasil panen tanaman pangan berdasarkan parameter cuaca.

## 🤖 Metodologi ML
1.  **Data Preparation:**
    *   Mengambil data bersih dari Data Warehouse (`vw_production_detail` join `vw_weather_detail`).
    *   **Feature Selection:** Curah Hujan (mm), Suhu (°C), Kelembaban (%).
    *   **Target Variable:** Produksi (Ton).
    *   **Train-Test Split:**
        *   Train: 2010-2018 (9 Tahun).
        *   Test: 2019-2020 (2 Tahun).
2.  **Modeling (Linear Regression):**
    *   Memilih algoritma *Linear Regression* sebagai baseline karena interpretabilitas tinggi.
    *   Melatih **7 Model Terpisah** (satu untuk setiap komoditas: Padi, Jagung, Kedelai, dll) karena karakteristik tanaman berbeda.
3.  **Evaluation:**
    *   Metrik: RMSE (Error) dan R-Squared (Kecocokan).
    *   **Hasil:**
        *   Best Model: **Ubi Jalar (R² 0.32)**.
        *   Worst Model: **Ubi Kayu (R² 0.03)**.
    *   *Insight:* Cuaca saja tidak cukup; perlu faktor lain (pupuk, luas lahan).

## 🔑 Key Takeaways
*   Pendekatan "Satu Model per Komoditas" lebih efektif daripada satu model global.
*   Evaluasi menunjukkan perlunya fitur tambahan untuk meningkatkan akurasi.
*   Model disimpan (`.pkl`) untuk digunakan dalam prediksi real-time.
