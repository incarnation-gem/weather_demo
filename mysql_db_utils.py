#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据库连接工具
"""

import pymysql
import pandas as pd
from datetime import datetime
import logging

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '200126kobe',  # MySQL密码
    'database': 'weather_db',
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
            datetime DATETIME NOT NULL,
            temp DECIMAL(4,1),
            humidity DECIMAL(4,1),
            precip DECIMAL(6,2),
            pressure DECIMAL(6,1),
            wind_speed DECIMAL(5,1),
            wind_dir VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_location_datetime (location_id, datetime)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # 创建统一的每日天气汇总表
        create_daily_table = """
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id VARCHAR(20) NOT NULL,
            location_name VARCHAR(50),
            date DATE NOT NULL,
            min_temp DECIMAL(4,1),
            max_temp DECIMAL(4,1),
            avg_temp DECIMAL(4,1),
            total_precip DECIMAL(6,2),
            avg_wind_speed DECIMAL(5,1),
            avg_humidity DECIMAL(4,1),
            record_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_location_date (location_id, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        # 创建位置信息表
        create_location_table = """
        CREATE TABLE IF NOT EXISTS location_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id VARCHAR(20) NOT NULL UNIQUE,
            location_name VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
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
        cursor.execute(create_location_table)
        
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

def save_hourly_to_mysql(hourly_data, location_id="101120101", location_name="济南"):
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
        (location_id, location_name, datetime, temp, humidity, precip, pressure, 
         wind_speed, wind_dir)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        temp = VALUES(temp),
        humidity = VALUES(humidity),
        precip = VALUES(precip),
        pressure = VALUES(pressure),
        wind_speed = VALUES(wind_speed),
        wind_dir = VALUES(wind_dir)
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
                
                data = (
                    location_id,
                    location_name,
                    datetime_str,  # 转换后的时间格式
                    hour_data.get('temp'),
                    hour_data.get('humidity'),
                    hour_data.get('precip', 0.0),  # 处理None值
                    hour_data.get('pressure'),
                    hour_data.get('windSpeed'),
                    hour_data.get('windDir')
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

def calculate_daily_summaries_mysql(location_id="101120101", location_name="济南"):
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
            MIN(temp) as min_temp,
            MAX(temp) as max_temp,
            AVG(temp) as avg_temp,
            SUM(COALESCE(precip, 0)) as total_precip,
            AVG(wind_speed) as avg_wind_speed,
            AVG(humidity) as avg_humidity,
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

def get_districts_info():
    """获取区县信息，返回location_id到中文名称的映射"""
    districts_map = {
        "101120102": "长清",
        "101120103": "商河", 
        "101120104": "章丘",
        "101120105": "平阴",
        "101120106": "济阳",
        "101120107": "历下",
        "101120108": "市中",
        "101120109": "槐荫",
        "101120110": "天桥",
        "101120111": "历城",
        "101121601": "莱芜",
        "101121603": "钢城"
    }
    return districts_map

def save_districts_hourly_to_mysql(hourly_data, location_id, location_name):
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
        (location_id, location_name, datetime, temp, humidity, precip, pressure, 
         wind_speed, wind_dir)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        location_name = VALUES(location_name),
        temp = VALUES(temp),
        humidity = VALUES(humidity),
        precip = VALUES(precip),
        pressure = VALUES(pressure),
        wind_speed = VALUES(wind_speed),
        wind_dir = VALUES(wind_dir)
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
                
                data = (
                    location_id,
                    location_name,
                    datetime_str,  # 转换后的时间格式
                    hour_data.get('temp'),
                    hour_data.get('humidity'),
                    hour_data.get('precip', 0.0),  # 处理None值
                    hour_data.get('pressure'),
                    hour_data.get('windSpeed'),
                    hour_data.get('windDir')
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
            MIN(temp) as min_temp,
            MAX(temp) as max_temp,
            AVG(temp) as avg_temp,
            SUM(COALESCE(precip, 0)) as total_precip,
            AVG(wind_speed) as avg_wind_speed,
            AVG(humidity) as avg_humidity,
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

def get_mysql_stats():
    """获取MySQL数据库统计信息"""
    try:
        conn = get_mysql_connection()
        
        # 获取济南市记录数（从hourly_weather表中筛选）
        city_hourly_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM hourly_weather WHERE location_name = '济南'", conn
        ).iloc[0]['count']
        
        city_daily_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM daily_weather WHERE location_name = '济南'", conn
        ).iloc[0]['count']
        
        # 获取区县记录数（排除济南）
        districts_hourly_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM hourly_weather WHERE location_name != '济南'", conn
        ).iloc[0]['count']
        
        districts_daily_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM daily_weather WHERE location_name != '济南'", conn
        ).iloc[0]['count']
        
        # 获取最新数据时间
        city_latest_hourly = pd.read_sql_query(
            "SELECT MAX(datetime) as latest FROM hourly_weather WHERE location_name = '济南'", conn
        ).iloc[0]['latest']
        
        city_latest_daily = pd.read_sql_query(
            "SELECT MAX(date) as latest FROM daily_weather WHERE location_name = '济南'", conn
        ).iloc[0]['latest']
        
        districts_latest_hourly = pd.read_sql_query(
            "SELECT MAX(datetime) as latest FROM hourly_weather WHERE location_name != '济南'", conn
        ).iloc[0]['latest']
        
        districts_latest_daily = pd.read_sql_query(
            "SELECT MAX(date) as latest FROM daily_weather WHERE location_name != '济南'", conn
        ).iloc[0]['latest']
        
        conn.close()
        
        return {
            'city_hourly_count': city_hourly_count,
            'city_daily_count': city_daily_count,
            'districts_hourly_count': districts_hourly_count,
            'districts_daily_count': districts_daily_count,
            'city_latest_hourly': city_latest_hourly,
            'city_latest_daily': city_latest_daily,
            'districts_latest_hourly': districts_latest_hourly,
            'districts_latest_daily': districts_latest_daily
        }
        
    except Exception as e:
        print(f"❌ 获取统计信息失败: {e}")
        return None

if __name__ == "__main__":
    # 测试数据库连接
    try:
        init_mysql_database()
        stats = get_mysql_stats()
        if stats:
            print(f"📊 数据库统计: 小时数据{stats['hourly_count']}条，每日数据{stats['daily_count']}条")
    except Exception as e:
        print(f"❌ 测试失败: {e}") 