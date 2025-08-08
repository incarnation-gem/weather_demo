#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据库连接工具
"""

import pymysql
import pandas as pd
from datetime import datetime
import logging

# 全局缓存变量
_location_cache = {}

import os

# 从环境变量获取数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),  # 强烈建议使用环境变量
    'database': os.getenv('DB_NAME', 'weather_db'),
    'charset': 'utf8mb4',
    'autocommit': True
}

def get_mysql_connection():
    """获取MySQL数据库连接"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        raise

def init_mysql_database():
    """初始化MySQL数据库表结构"""
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # 创建统一的小时天气数据表
        create_hourly_table = """
        CREATE TABLE IF NOT EXISTS hourly_weather (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id VARCHAR(20) NOT NULL,
            location_name VARCHAR(50),
            province VARCHAR(50),
            city VARCHAR(50),
            datetime DATETIME NOT NULL,
            temp_celsius DECIMAL(4,1),
            humidity_percent DECIMAL(4,1),
            precip_mm DECIMAL(6,2),
            pressure_hpa DECIMAL(6,1),
            wind_scale VARCHAR(5),
            wind_dir VARCHAR(10),
            text VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_location_datetime (location_id, datetime)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
        STATS_SAMPLE_PAGES=100 STATS_AUTO_RECALC=1
        """
        
        # 创建统一的每日天气汇总表（直接使用API的weatherDaily数据）
        create_daily_table = """
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id VARCHAR(20) NOT NULL,
            location_name VARCHAR(50),
            province VARCHAR(50) COMMENT '省份',
            city VARCHAR(50) COMMENT '城市',
            date DATE NOT NULL,
            temp_min_celsius DECIMAL(4,1) COMMENT '最低温度(摄氏度)',
            temp_max_celsius DECIMAL(4,1) COMMENT '最高温度(摄氏度)',
            humidity_percent DECIMAL(4,1) COMMENT '湿度(%)',
            precip_mm DECIMAL(6,2) COMMENT '降水量(mm)',
            pressure_hpa DECIMAL(6,1) COMMENT '气压(hPa)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_location_date (location_id, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        STATS_SAMPLE_PAGES=100 STATS_AUTO_RECALC=1
        """
        
        # 创建索引
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_hourly_location_datetime ON hourly_weather(location_id, datetime)",
            "CREATE INDEX IF NOT EXISTS idx_hourly_datetime ON hourly_weather(datetime)",
            "CREATE INDEX IF NOT EXISTS idx_hourly_location_name ON hourly_weather(location_name)",
            "CREATE INDEX IF NOT EXISTS idx_daily_location_date ON daily_weather(location_id, date)",
            "CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_weather(date)",
            "CREATE INDEX IF NOT EXISTS idx_daily_location_name ON daily_weather(location_name)"
        ]
        
        cursor.execute(create_hourly_table)
        cursor.execute(create_daily_table)
        
        for index_sql in create_indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                # MySQL中IF NOT EXISTS语法可能不支持，忽略错误
                pass
        
        conn.commit()
        print("✅ MySQL数据库表结构初始化完成（统一表结构）")
        
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def save_hourly_to_mysql(hourly_data, location_id, location_name, csv_path=None):
    """保存小时天气数据到MySQL（统一使用hourly_weather表）"""
    if not hourly_data:
        print("⚠️  没有小时数据需要保存")
        return
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # 准备插入语句
        insert_sql = """
        INSERT INTO hourly_weather 
        (location_id, location_name, province, city, datetime, temp_celsius, humidity_percent, precip_mm, pressure_hpa, 
         wind_scale, wind_dir, text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        province = VALUES(province),
        city = VALUES(city),
        temp_celsius = VALUES(temp_celsius),
        humidity_percent = VALUES(humidity_percent),
        precip_mm = VALUES(precip_mm),
        pressure_hpa = VALUES(pressure_hpa),
        wind_scale = VALUES(wind_scale),
        wind_dir = VALUES(wind_dir),
        text = VALUES(text)
        """
        
        new_count = 0
        duplicate_count = 0
        
        for hour_data in hourly_data:
            try:
                # 准备数据 - 根据API实际返回的字段名
                # 转换时间格式：从 "2025-07-21T00:00+08:00" 到 "2025-07-21 00:00:00"
                time_str = hour_data['time']
                if 'T' in time_str:
                    # 提取日期和时间部分，去掉时区信息
                    datetime_str = time_str.split('T')[0] + ' ' + time_str.split('T')[1].split('+')[0]
                else:
                    datetime_str = time_str
                
                # 获取省市信息
                location_info = get_location_province_city(location_id, csv_path)
                province = location_info['province']
                city = location_info['city']
                
                data = (
                    location_id,
                    location_name,
                    province,  # 使用动态省市信息
                    city,      # 使用动态省市信息
                    datetime_str,  # 转换后的时间格式
                    hour_data.get('temp'),
                    hour_data.get('humidity'),
                    hour_data.get('precip', 0.0),  # 处理None值
                    hour_data.get('pressure'),
                    hour_data.get('windScale'),  # 使用windScale而不是windSpeed
                    hour_data.get('windDir'),
                    hour_data.get('text')   # 天气现象描述
                )
                
                cursor.execute(insert_sql, data)
                
                if cursor.rowcount == 1:
                    new_count += 1
                else:
                    duplicate_count += 1
                    
            except Exception as e:
                print(f"⚠️  保存小时数据失败: {e}")
                continue
        
        conn.commit()
        print(f"✅ 小时数据保存完成: 新增{new_count}条，更新{duplicate_count}条")
        
    except Exception as e:
        print(f"❌ 保存小时数据失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def calculate_daily_summaries_mysql(location_id, location_name, csv_path=None):
    """计算并保存每日天气汇总到MySQL（统一使用daily_weather表）"""
    try:
        conn = get_mysql_connection()
        
        # 查询聚合数据
        query = """
        INSERT INTO daily_weather 
        (location_id, location_name, date, min_temp, max_temp, avg_temp, total_precip, 
         avg_wind_speed, avg_humidity, record_count)
        SELECT 
            location_id,
            location_name,
            DATE(datetime) as date,
            MIN(temp_celsius) as min_temp,
            MAX(temp_celsius) as max_temp,
            AVG(temp_celsius) as avg_temp,
            SUM(COALESCE(precip_mm, 0)) as total_precip,
            AVG(wind_speed_kmh) as avg_wind_speed,
            AVG(humidity_percent) as avg_humidity,
            COUNT(*) as record_count
        FROM hourly_weather 
        WHERE location_id = %s AND location_name = %s
        GROUP BY location_id, location_name, DATE(datetime)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        min_temp = VALUES(min_temp),
        max_temp = VALUES(max_temp),
        avg_temp = VALUES(avg_temp),
        total_precip = VALUES(total_precip),
        avg_wind_speed = VALUES(avg_wind_speed),
        avg_humidity = VALUES(avg_humidity),
        record_count = VALUES(record_count)
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (location_id, location_name))
        conn.commit()
        
        print("✅ 每日天气汇总计算完成")
        
    except Exception as e:
        print(f"❌ 计算每日汇总失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()



def save_districts_hourly_to_mysql(hourly_data, location_id, location_name, csv_path=None):
    """保存区县小时天气数据到MySQL"""
    if not hourly_data:
        print("⚠️  没有区县小时数据需要保存")
        return
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # 准备插入语句
        insert_sql = """
        INSERT INTO hourly_weather 
        (location_id, location_name, province, city, datetime, temp_celsius, humidity_percent, precip_mm, pressure_hpa, 
         wind_scale, wind_dir, text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        province = VALUES(province),
        city = VALUES(city),
        temp_celsius = VALUES(temp_celsius),
        humidity_percent = VALUES(humidity_percent),
        precip_mm = VALUES(precip_mm),
        pressure_hpa = VALUES(pressure_hpa),
        wind_scale = VALUES(wind_scale),
        wind_dir = VALUES(wind_dir),
        text = VALUES(text)
        """
        
        new_count = 0
        duplicate_count = 0
        
        for hour_data in hourly_data:
            try:
                # 准备数据 - 根据API实际返回的字段名
                # 转换时间格式：从 "2025-07-21T00:00+08:00" 到 "2025-07-21 00:00:00"
                time_str = hour_data['time']
                if 'T' in time_str:
                    # 提取日期和时间部分，去掉时区信息
                    datetime_str = time_str.split('T')[0] + ' ' + time_str.split('T')[1].split('+')[0]
                else:
                    datetime_str = time_str
                
                # 获取省市信息
                location_info = get_location_province_city(location_id, csv_path)
                province = location_info['province']
                city = location_info['city']
                
                data = (
                    location_id,
                    location_name,
                    province,  # 使用动态省市信息
                    city,      # 使用动态省市信息
                    datetime_str,  # 转换后的时间格式
                    hour_data.get('temp'),
                    hour_data.get('humidity'),
                    hour_data.get('precip', 0.0),  # 处理None值
                    hour_data.get('pressure'),
                    hour_data.get('windScale'),  # 使用windScale而不是windSpeed
                    hour_data.get('windDir'),
                    hour_data.get('text')   # 天气现象描述
                )
                
                cursor.execute(insert_sql, data)
                
                if cursor.rowcount == 1:
                    new_count += 1
                else:
                    duplicate_count += 1
                    
            except Exception as e:
                print(f"⚠️  保存区县{location_name}小时数据失败: {e}")
                continue
        
        conn.commit()
        print(f"✅ 区县{location_name}小时数据保存完成: 新增{new_count}条，更新{duplicate_count}条")
        return (new_count, duplicate_count)
        
    except Exception as e:
        print(f"❌ 保存区县{location_name}小时数据失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def calculate_districts_daily_summaries_mysql():
    """计算并保存所有区县每日天气汇总到MySQL"""
    try:
        conn = get_mysql_connection()
        
        # 查询聚合数据
        query = """
        INSERT INTO daily_weather 
        (location_id, location_name, date, min_temp, max_temp, avg_temp, total_precip, 
         avg_wind_speed, avg_humidity, record_count)
        SELECT 
            location_id,
            location_name,
            DATE(datetime) as date,
            MIN(temp_celsius) as min_temp,
            MAX(temp_celsius) as max_temp,
            AVG(temp_celsius) as avg_temp,
            SUM(COALESCE(precip_mm, 0)) as total_precip,
            AVG(wind_speed_kmh) as avg_wind_speed,
            AVG(humidity_percent) as avg_humidity,
            COUNT(*) as record_count
        FROM hourly_weather 
        GROUP BY location_id, location_name, DATE(datetime)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        min_temp = VALUES(min_temp),
        max_temp = VALUES(max_temp),
        avg_temp = VALUES(avg_temp),
        total_precip = VALUES(total_precip),
        avg_wind_speed = VALUES(avg_wind_speed),
        avg_humidity = VALUES(avg_humidity),
        record_count = VALUES(record_count)
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        
        print("✅ 区县每日天气汇总计算完成")
        
    except Exception as e:
        print(f"❌ 计算区县每日汇总失败: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def get_location_province_city(location_id, csv_path=None):
    """根据location_id获取省市信息 - 支持动态CSV路径
    
    Args:
        location_id: 位置ID
        csv_path: CSV文件路径，如果为None则使用环境变量或默认映射
    
    Returns:
        dict: 包含province和city信息的字典
    """
    if not location_id:
        raise ValueError("location_id不能为空")
    
    # 如果提供了csv_path，优先使用
    if csv_path and os.path.exists(csv_path):
        try:
            import csv
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    loc_id = row.get('location_id', '').strip()
                    province = row.get('province', '').strip()
                    city = row.get('city', '').strip()
                    
                    if loc_id == location_id and province and city:
                        return {'province': province, 'city': city}
        except Exception:
            # 如果指定路径失败，继续尝试其他方式
            pass
    
    # 使用环境变量指定的CSV路径
    env_csv_path = os.getenv('CITY_CSV_PATH')
    if env_csv_path and os.path.exists(env_csv_path):
        try:
            import csv
            with open(env_csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    loc_id = row.get('location_id', '').strip()
                    province = row.get('province', '').strip()
                    city = row.get('city', '').strip()
                    
                    if loc_id == location_id and province and city:
                        return {'province': province, 'city': city}
        except Exception:
            pass
    
        # 未找到省市信息，提供有意义的错误提示
    raise ValueError(
        f"未找到location_id '{location_id}' 对应的省市信息。\n"
        f"请通过以下方式之一提供城市信息：\n"
        f"1. 设置 csv_path 参数指向包含省市信息的CSV文件\n"
        f"2. 设置环境变量 CITY_CSV_PATH 指向CSV文件\n"
        f"3. 确保CSV文件包含 location_id, province, city 列"
    )

def save_daily_weather_mysql(weather_daily_data, location_id, location_name, csv_path=None):
    """直接保存API返回的weatherDaily数据到MySQL"""
    if not weather_daily_data:
        print("⚠️  没有每日天气数据需要保存")
        return (0, 0)
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        
        # 获取省市信息
        location_info = get_location_province_city(location_id, csv_path)
        province = location_info['province']
        city = location_info['city']
        
        # 准备插入语句（使用新的字段名）
        insert_sql = """
        INSERT INTO daily_weather 
        (location_id, location_name, province, city, date, temp_min_celsius, 
         temp_max_celsius, humidity_percent, precip_mm, pressure_hpa)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        province = VALUES(province),
        city = VALUES(city),
        temp_min_celsius = VALUES(temp_min_celsius),
        temp_max_celsius = VALUES(temp_max_celsius),
        humidity_percent = VALUES(humidity_percent),
        precip_mm = VALUES(precip_mm),
        pressure_hpa = VALUES(pressure_hpa)
        """
        
        try:
            # 准备数据 - 直接使用API返回的weatherDaily字段
            data = (
                location_id,
                location_name,
                province,
                city,
                weather_daily_data.get('date'),  # 日期
                weather_daily_data.get('tempMin'),  # 最低温度
                weather_daily_data.get('tempMax'),  # 最高温度
                weather_daily_data.get('humidity'),  # 湿度
                weather_daily_data.get('precip', '0.0'),  # 降水量
                weather_daily_data.get('pressure')  # 气压
            )
            
            cursor.execute(insert_sql, data)
            
            if cursor.rowcount == 1:
                new_count = 1
                updated_count = 0
            else:
                new_count = 0
                updated_count = 1
                
            conn.commit()
            print(f"✅ {location_name} 每日数据保存完成: 新增{new_count}条，更新{updated_count}条")
            return (new_count, updated_count)
                
        except Exception as e:
            print(f"⚠️  保存每日数据失败: {e}")
            return (0, 0)
        
    except Exception as e:
        print(f"❌ 保存每日数据失败: {e}")
        return (0, 0)
    finally:
        if 'conn' in locals():
            conn.close()

def get_mysql_stats(location_name=None):
    """获取MySQL数据库统计信息
    
    Args:
        location_name: 指定城市名称，如果为None则返回所有数据
    """
    try:
        conn = get_mysql_connection()
        
        if location_name:
            # 获取指定城市的记录数
            target_hourly = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM hourly_weather WHERE location_name = %s", 
                conn, params=[location_name]
            ).iloc[0]['count']
            
            target_daily = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM daily_weather WHERE location_name = %s", 
                conn, params=[location_name]
            ).iloc[0]['count']
            
            other_hourly = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM hourly_weather WHERE location_name != %s", 
                conn, params=[location_name]
            ).iloc[0]['count']
            
            other_daily = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM daily_weather WHERE location_name != %s", 
                conn, params=[location_name]
            ).iloc[0]['count']
            
            result = {
                'target_hourly': target_hourly,
                'target_daily': target_daily,
                'other_hourly': other_hourly,
                'other_daily': other_daily
            }
        else:
            # 获取所有数据的总统计
            total_hourly = pd.read_sql_query("SELECT COUNT(*) as count FROM hourly_weather", conn).iloc[0]['count']
            total_daily = pd.read_sql_query("SELECT COUNT(*) as count FROM daily_weather", conn).iloc[0]['count']
            
            result = {
                'total_hourly': total_hourly,
                'total_daily': total_daily
            }
        
        conn.close()
        return result
        
    except Exception as e:
        print(f"❌ 获取统计信息失败: {e}")
        return None

if __name__ == "__main__":
    # 测试数据库连接
    try:
        init_mysql_database()
        stats = get_mysql_stats()
        if stats:
            if 'total_hourly' in stats:
                print(f"📊 数据库统计: 总小时数据{stats['total_hourly']}条，总每日数据{stats['total_daily']}条")
            else:
                print(f"📊 数据库统计: 目标城市小时数据{stats['target_hourly']}条，每日数据{stats['target_daily']}条")
    except Exception as e:
        print(f"❌ 测试失败: {e}") 
