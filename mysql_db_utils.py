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
                location_info = get_location_province_city(location_id)
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
                location_info = get_location_province_city(location_id)
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

def get_location_province_city(location_id):
    """根据location_id获取省市信息"""
    location_mapping = {
        '101120101': {'province': '山东省', 'city': '济南市'},
        '101120102': {'province': '山东省', 'city': '济南市'},
        '101120103': {'province': '山东省', 'city': '济南市'},
        '101120104': {'province': '山东省', 'city': '济南市'},
        '101120105': {'province': '山东省', 'city': '济南市'},
        '101120106': {'province': '山东省', 'city': '济南市'},
        '101120107': {'province': '山东省', 'city': '济南市'},
        '101120108': {'province': '山东省', 'city': '济南市'},
        '101120109': {'province': '山东省', 'city': '济南市'},
        '101120110': {'province': '山东省', 'city': '济南市'},
        '101120111': {'province': '山东省', 'city': '济南市'},
        '101121601': {'province': '山东省', 'city': '济南市'},
        '101121603': {'province': '山东省', 'city': '济南市'},
        '101120201': {'province': '山东省', 'city': '青岛市'},
        '101120202': {'province': '山东省', 'city': '青岛市'},
        '101120203': {'province': '山东省', 'city': '青岛市'},
        '101120204': {'province': '山东省', 'city': '青岛市'},
        '101120205': {'province': '山东省', 'city': '青岛市'},
        '101120206': {'province': '山东省', 'city': '青岛市'},
        '101120207': {'province': '山东省', 'city': '青岛市'},
        '101120208': {'province': '山东省', 'city': '青岛市'},
        '101120209': {'province': '山东省', 'city': '青岛市'},
        '101120210': {'province': '山东省', 'city': '青岛市'},
        '101120211': {'province': '山东省', 'city': '青岛市'},
        '101120301': {'province': '山东省', 'city': '淄博市'},
        '101120302': {'province': '山东省', 'city': '淄博市'},
        '101120303': {'province': '山东省', 'city': '淄博市'},
        '101120304': {'province': '山东省', 'city': '淄博市'},
        '101120305': {'province': '山东省', 'city': '淄博市'},
        '101120306': {'province': '山东省', 'city': '淄博市'},
        '101120307': {'province': '山东省', 'city': '淄博市'},
        '101120308': {'province': '山东省', 'city': '淄博市'},
        '101120309': {'province': '山东省', 'city': '淄博市'},
        '101120401': {'province': '山东省', 'city': '德州市'},
        '101120402': {'province': '山东省', 'city': '德州市'},
        '101120403': {'province': '山东省', 'city': '德州市'},
        '101120405': {'province': '山东省', 'city': '德州市'},
        '101120406': {'province': '山东省', 'city': '德州市'},
        '101120407': {'province': '山东省', 'city': '德州市'},
        '101120408': {'province': '山东省', 'city': '德州市'},
        '101120409': {'province': '山东省', 'city': '德州市'},
        '101120410': {'province': '山东省', 'city': '德州市'},
        '101120411': {'province': '山东省', 'city': '德州市'},
        '101120412': {'province': '山东省', 'city': '德州市'},
        '101120413': {'province': '山东省', 'city': '德州市'},
        '101120501': {'province': '山东省', 'city': '烟台市'},
        '101120502': {'province': '山东省', 'city': '烟台市'},
        '101120504': {'province': '山东省', 'city': '烟台市'},
        '101120505': {'province': '山东省', 'city': '烟台市'},
        '101120506': {'province': '山东省', 'city': '烟台市'},
        '101120507': {'province': '山东省', 'city': '烟台市'},
        '101120508': {'province': '山东省', 'city': '烟台市'},
        '101120509': {'province': '山东省', 'city': '烟台市'},
        '101120510': {'province': '山东省', 'city': '烟台市'},
        '101120511': {'province': '山东省', 'city': '烟台市'},
        '101120512': {'province': '山东省', 'city': '烟台市'},
        '101120513': {'province': '山东省', 'city': '烟台市'},
        '101120601': {'province': '山东省', 'city': '潍坊市'},
        '101120602': {'province': '山东省', 'city': '潍坊市'},
        '101120603': {'province': '山东省', 'city': '潍坊市'},
        '101120604': {'province': '山东省', 'city': '潍坊市'},
        '101120605': {'province': '山东省', 'city': '潍坊市'},
        '101120606': {'province': '山东省', 'city': '潍坊市'},
        '101120607': {'province': '山东省', 'city': '潍坊市'},
        '101120608': {'province': '山东省', 'city': '潍坊市'},
        '101120609': {'province': '山东省', 'city': '潍坊市'},
        '101120610': {'province': '山东省', 'city': '潍坊市'},
        '101120611': {'province': '山东省', 'city': '潍坊市'},
        '101120612': {'province': '山东省', 'city': '潍坊市'},
        '101120613': {'province': '山东省', 'city': '潍坊市'},
        '101120701': {'province': '山东省', 'city': '济宁市'},
        '101120702': {'province': '山东省', 'city': '济宁市'},
        '101120703': {'province': '山东省', 'city': '济宁市'},
        '101120704': {'province': '山东省', 'city': '济宁市'},
        '101120705': {'province': '山东省', 'city': '济宁市'},
        '101120706': {'province': '山东省', 'city': '济宁市'},
        '101120707': {'province': '山东省', 'city': '济宁市'},
        '101120708': {'province': '山东省', 'city': '济宁市'},
        '101120709': {'province': '山东省', 'city': '济宁市'},
        '101120710': {'province': '山东省', 'city': '济宁市'},
        '101120711': {'province': '山东省', 'city': '济宁市'},
        '101120712': {'province': '山东省', 'city': '济宁市'},
        '101120801': {'province': '山东省', 'city': '泰安市'},
        '101120802': {'province': '山东省', 'city': '泰安市'},
        '101120803': {'province': '山东省', 'city': '泰安市'},
        '101120804': {'province': '山东省', 'city': '泰安市'},
        '101120805': {'province': '山东省', 'city': '泰安市'},
        '101120806': {'province': '山东省', 'city': '泰安市'},
        '101120807': {'province': '山东省', 'city': '泰安市'},
        '101120901': {'province': '山东省', 'city': '临沂市'},
        '101120902': {'province': '山东省', 'city': '临沂市'},
        '101120903': {'province': '山东省', 'city': '临沂市'},
        '101120904': {'province': '山东省', 'city': '临沂市'},
        '101120905': {'province': '山东省', 'city': '临沂市'},
        '101120906': {'province': '山东省', 'city': '临沂市'},
        '101120907': {'province': '山东省', 'city': '临沂市'},
        '101120908': {'province': '山东省', 'city': '临沂市'},
        '101120909': {'province': '山东省', 'city': '临沂市'},
        '101120910': {'province': '山东省', 'city': '临沂市'},
        '101120911': {'province': '山东省', 'city': '临沂市'},
        '101120912': {'province': '山东省', 'city': '临沂市'},
        '101120913': {'province': '山东省', 'city': '临沂市'},
        '101121001': {'province': '山东省', 'city': '菏泽市'},
        '101121002': {'province': '山东省', 'city': '菏泽市'},
        '101121003': {'province': '山东省', 'city': '菏泽市'},
        '101121004': {'province': '山东省', 'city': '菏泽市'},
        '101121005': {'province': '山东省', 'city': '菏泽市'},
        '101121006': {'province': '山东省', 'city': '菏泽市'},
        '101121007': {'province': '山东省', 'city': '菏泽市'},
        '101121008': {'province': '山东省', 'city': '菏泽市'},
        '101121009': {'province': '山东省', 'city': '菏泽市'},
        '101121010': {'province': '山东省', 'city': '菏泽市'},
        '101121101': {'province': '山东省', 'city': '滨州市'},
        '101121102': {'province': '山东省', 'city': '滨州市'},
        '101121103': {'province': '山东省', 'city': '滨州市'},
        '101121104': {'province': '山东省', 'city': '滨州市'},
        '101121105': {'province': '山东省', 'city': '滨州市'},
        '101121106': {'province': '山东省', 'city': '滨州市'},
        '101121107': {'province': '山东省', 'city': '滨州市'},
        '101121108': {'province': '山东省', 'city': '滨州市'},
        '101121201': {'province': '山东省', 'city': '东营市'},
        '101121202': {'province': '山东省', 'city': '东营市'},
        '101121203': {'province': '山东省', 'city': '东营市'},
        '101121204': {'province': '山东省', 'city': '东营市'},
        '101121205': {'province': '山东省', 'city': '东营市'},
        '101121206': {'province': '山东省', 'city': '东营市'},
        '101121301': {'province': '山东省', 'city': '威海市'},
        '101121302': {'province': '山东省', 'city': '威海市'},
        '101121303': {'province': '山东省', 'city': '威海市'},
        '101121304': {'province': '山东省', 'city': '威海市'},
        '101121307': {'province': '山东省', 'city': '威海市'},
        '101121401': {'province': '山东省', 'city': '枣庄市'},
        '101121402': {'province': '山东省', 'city': '枣庄市'},
        '101121403': {'province': '山东省', 'city': '枣庄市'},
        '101121404': {'province': '山东省', 'city': '枣庄市'},
        '101121405': {'province': '山东省', 'city': '枣庄市'},
        '101121406': {'province': '山东省', 'city': '枣庄市'},
        '101121407': {'province': '山东省', 'city': '枣庄市'},
        '101121501': {'province': '山东省', 'city': '日照市'},
        '101121502': {'province': '山东省', 'city': '日照市'},
        '101121503': {'province': '山东省', 'city': '日照市'},
        '101121504': {'province': '山东省', 'city': '日照市'},
        '101121505': {'province': '山东省', 'city': '日照市'},
        '101121701': {'province': '山东省', 'city': '聊城市'},
        '101121702': {'province': '山东省', 'city': '聊城市'},
        '101121703': {'province': '山东省', 'city': '聊城市'},
        '101121704': {'province': '山东省', 'city': '聊城市'},
        '101121705': {'province': '山东省', 'city': '聊城市'},
        '101121706': {'province': '山东省', 'city': '聊城市'},
        '101121707': {'province': '山东省', 'city': '聊城市'},
        '101121708': {'province': '山东省', 'city': '聊城市'},
        '101121709': {'province': '山东省', 'city': '聊城市'},
    }
    
    return location_mapping.get(location_id, {'province': '山东省', 'city': '未知市'})

def save_daily_weather_mysql(weather_daily_data, location_id, location_name):
    """直接保存API返回的weatherDaily数据到MySQL"""
    if not weather_daily_data:
        print("⚠️  没有每日天气数据需要保存")
        return (0, 0)
    
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        
        # 获取省市信息
        location_info = get_location_province_city(location_id)
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