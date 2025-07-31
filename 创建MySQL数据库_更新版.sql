-- 创建天气数据库
CREATE DATABASE IF NOT EXISTS weather_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 使用数据库
USE weather_db;

-- 创建统一的小时天气数据表
CREATE TABLE IF NOT EXISTS hourly_weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location_id VARCHAR(20) NOT NULL,           -- 地点ID（如：101120101）
    location_name VARCHAR(50),                  -- 地点名称（如：济南、历下等）
    province VARCHAR(50),                       -- 省份名称（如：山东省）
    city VARCHAR(50),                          -- 城市名称（如：济南市）
    datetime DATETIME NOT NULL,                 -- 日期时间（YYYY-MM-DD HH:MM:SS）
    temp_celsius DECIMAL(4,1),                 -- 温度(°C)
    humidity_percent DECIMAL(4,1),             -- 湿度(%)
    precip_mm DECIMAL(6,2),                    -- 降水量(mm)
    pressure_hpa DECIMAL(6,1),                 -- 气压(hPa)
    wind_speed_kmh DECIMAL(5,1),               -- 风速(km/h)
    wind_dir VARCHAR(10),                      -- 风向
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_location_datetime (location_id, datetime)  -- 防止重复数据
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci STATS_SAMPLE_PAGES=100 STATS_AUTO_RECALC=1;

-- 创建统一的每日天气汇总表
CREATE TABLE IF NOT EXISTS daily_weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location_id VARCHAR(20) NOT NULL,           -- 地点ID（如：101120101）
    location_name VARCHAR(50),                  -- 地点名称（如：济南、历下等）
    date DATE NOT NULL,                         -- 日期（YYYY-MM-DD）
    min_temp DECIMAL(4,1),                     -- 最低温度(°C)
    max_temp DECIMAL(4,1),                     -- 最高温度(°C)
    avg_temp DECIMAL(4,1),                     -- 平均温度(°C)
    total_precip DECIMAL(6,2),                 -- 总降水量(mm)
    avg_wind_speed DECIMAL(5,1),               -- 平均风速(km/h)
    avg_humidity DECIMAL(4,1),                 -- 平均湿度(%)
    record_count INT,                          -- 小时记录数量
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_location_date (location_id, date)  -- 防止重复数据
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci STATS_SAMPLE_PAGES=100 STATS_AUTO_RECALC=1;

-- 创建索引
CREATE INDEX idx_hourly_location_datetime ON hourly_weather(location_id, datetime);
CREATE INDEX idx_hourly_datetime ON hourly_weather(datetime);
CREATE INDEX idx_hourly_location_name ON hourly_weather(location_name);
CREATE INDEX idx_daily_location_date ON daily_weather(location_id, date);
CREATE INDEX idx_daily_date ON daily_weather(date);
CREATE INDEX idx_daily_location_name ON daily_weather(location_name);

-- 显示创建的表
SHOW TABLES;

-- 显示表结构
DESCRIBE hourly_weather;
DESCRIBE daily_weather;
