-- ==========================================
-- SMART HARVEST DATA WAREHOUSE SCHEMA
-- Star Schema Design for Analytics
-- ==========================================

CREATE DATABASE IF NOT EXISTS harvest_dw;
USE harvest_dw;

-- ==========================================
-- 1. DIMENSION TABLES (Master Data)
-- ==========================================

-- Dimension: Province
CREATE TABLE IF NOT EXISTS dim_province (
    province_id INT AUTO_INCREMENT PRIMARY KEY,
    province_name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_province_name (province_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT PRIMARY KEY COMMENT 'Format: YYYYMM (e.g., 201501 = Jan 2015)',
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    quarter INT,
    season VARCHAR(20) COMMENT 'Kemarau (Apr-Sep) / Hujan (Oct-Mar)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_year_month (year, month),
    INDEX idx_season (season)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dimension: Commodity
CREATE TABLE IF NOT EXISTS dim_commodity (
    commodity_id INT AUTO_INCREMENT PRIMARY KEY,
    commodity_name VARCHAR(50) NOT NULL UNIQUE COMMENT 'padi, jagung, kedelai, etc.',
    commodity_type VARCHAR(50) COMMENT 'Pangan Utama / Palawija / Umbi-umbian',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_commodity_name (commodity_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ==========================================
-- 2. FACT TABLES (Transactional Data)
-- ==========================================

-- Fact: Weather Monthly (Cuaca Bulanan)
CREATE TABLE IF NOT EXISTS fact_weather_monthly (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    time_id INT NOT NULL,
    province_id INT NOT NULL,
    total_rainfall_mm DECIMAL(10,2) COMMENT 'Total curah hujan (mm)',
    avg_temperature_c DECIMAL(5,2) COMMENT 'Suhu rata-rata (Celsius)',
    avg_humidity_pct DECIMAL(5,2) COMMENT 'Kelembaban rata-rata (%)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    
    INDEX idx_time_province (time_id, province_id),
    INDEX idx_province (province_id),
    UNIQUE KEY unique_weather (time_id, province_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fact: Production Monthly (Produksi Historis)
CREATE TABLE IF NOT EXISTS fact_production_monthly (
    production_id INT AUTO_INCREMENT PRIMARY KEY,
    time_id INT NOT NULL,
    province_id INT NOT NULL,
    commodity_id INT NOT NULL,
    production_ton DECIMAL(12,2) NOT NULL COMMENT 'Produksi dalam ton',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    FOREIGN KEY (commodity_id) REFERENCES dim_commodity(commodity_id),
    
    INDEX idx_time_province_commodity (time_id, province_id, commodity_id),
    INDEX idx_province (province_id),
    INDEX idx_commodity (commodity_id),
    UNIQUE KEY unique_production (time_id, province_id, commodity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fact: Crop Prediction (Hasil Prediksi ML)
CREATE TABLE IF NOT EXISTS fact_crop_prediction (
    prediction_id INT AUTO_INCREMENT PRIMARY KEY,
    time_id INT NOT NULL,
    province_id INT NOT NULL,
    commodity_id INT NOT NULL,
    predicted_ton DECIMAL(12,2) NOT NULL COMMENT 'Hasil prediksi (ton)',
    model_name VARCHAR(100) DEFAULT 'LinearRegression_v1' COMMENT 'Nama model ML',
    confidence_score DECIMAL(5,4) COMMENT 'Confidence score (0-1)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Waktu prediksi dibuat',
    
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    FOREIGN KEY (commodity_id) REFERENCES dim_commodity(commodity_id),
    
    INDEX idx_time_province_commodity (time_id, province_id, commodity_id),
    INDEX idx_model (model_name),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ==========================================
-- 3. POPULATE DIMENSION TABLES (Master Data)
-- ==========================================

-- Populate dim_commodity
INSERT INTO dim_commodity (commodity_name, commodity_type) VALUES
('padi', 'Pangan Utama'),
('jagung', 'Pangan Utama'),
('kedelai', 'Palawija'),
('kacang_tanah', 'Palawija'),
('kacang_hijau', 'Palawija'),
('ubi_kayu', 'Umbi-umbian'),
('ubi_jalar', 'Umbi-umbian')
ON DUPLICATE KEY UPDATE commodity_type=VALUES(commodity_type);

-- ==========================================
-- 4. HELPER VIEWS FOR ANALYTICS
-- ==========================================

-- View: Production with full details
CREATE OR REPLACE VIEW vw_production_detail AS
SELECT 
    p.production_id,
    t.year,
    t.month,
    t.month_name,
    t.season,
    prov.province_name,
    c.commodity_name,
    c.commodity_type,
    p.production_ton,
    p.created_at
FROM fact_production_monthly p
JOIN dim_time t ON p.time_id = t.time_id
JOIN dim_province prov ON p.province_id = prov.province_id
JOIN dim_commodity c ON p.commodity_id = c.commodity_id;

-- View: Prediction with full details
CREATE OR REPLACE VIEW vw_prediction_detail AS
SELECT 
    pred.prediction_id,
    t.year,
    t.month,
    t.month_name,
    t.season,
    prov.province_name,
    c.commodity_name,
    pred.predicted_ton,
    pred.model_name,
    pred.confidence_score,
    pred.created_at
FROM fact_crop_prediction pred
JOIN dim_time t ON pred.time_id = t.time_id
JOIN dim_province prov ON pred.province_id = prov.province_id
JOIN dim_commodity c ON pred.commodity_id = c.commodity_id;

-- View: Weather with full details
CREATE OR REPLACE VIEW vw_weather_detail AS
SELECT 
    w.weather_id,
    t.year,
    t.month,
    t.month_name,
    t.season,
    prov.province_name,
    w.total_rainfall_mm,
    w.avg_temperature_c,
    w.avg_humidity_pct,
    w.created_at
FROM fact_weather_monthly w
JOIN dim_time t ON w.time_id = t.time_id
JOIN dim_province prov ON w.province_id = prov.province_id;

-- ==========================================
-- 5. SUMMARY
-- ==========================================
-- Schema created successfully!
-- Next steps:
-- 1. Populate dim_province from existing data
-- 2. Populate dim_time (2010-2025)
-- 3. Migrate fact_production_monthly from harvest data
-- 4. Migrate fact_weather_monthly from weather data
-- 5. Generate predictions into fact_crop_prediction
-- ==========================================
