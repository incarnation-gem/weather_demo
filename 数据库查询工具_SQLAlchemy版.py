#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL天气数据查询工具 - SQLAlchemy版
使用SQLAlchemy连接，彻底消除pandas警告
"""

import pandas as pd
from sqlalchemy import create_engine, text
from mysql_db_utils import DB_CONFIG

class WeatherDataQuery:
    """天气数据查询类 - SQLAlchemy版本"""
    
    def __init__(self):
        """初始化SQLAlchemy引擎"""
        # 构建MySQL连接字符串
        user = DB_CONFIG['user']
        password = DB_CONFIG['password']
        host = DB_CONFIG['host']
        database = DB_CONFIG['database']
        
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.engine = create_engine(connection_string)
    
    def get_hourly_data(self, location_id="101120101", days=7, limit=None):
        """获取小时数据"""
        if limit:
            query = text("""
                SELECT * FROM hourly_weather 
                WHERE location_id = :location_id 
                AND datetime >= DATE_SUB(NOW(), INTERVAL :days DAY)
                ORDER BY datetime DESC 
                LIMIT :limit
            """)
            df = pd.read_sql_query(query, self.engine, 
                                 params={'location_id': location_id, 'days': days, 'limit': limit})
        else:
            query = text("""
                SELECT * FROM hourly_weather 
                WHERE location_id = :location_id 
                AND datetime >= DATE_SUB(NOW(), INTERVAL :days DAY)
                ORDER BY datetime DESC
            """)
            df = pd.read_sql_query(query, self.engine, 
                                 params={'location_id': location_id, 'days': days})
        
        return df
    
    def get_daily_summary(self, location_id="101120101", days=7):
        """获取每日汇总数据"""
        query = text("""
            SELECT * FROM daily_weather 
            WHERE location_id = :location_id 
            AND date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            ORDER BY date DESC
        """)
        
        df = pd.read_sql_query(query, self.engine, 
                             params={'location_id': location_id, 'days': days})
        return df
    
    def get_daily_summary_from_hourly(self, location_id="101120101", days=7):
        """从小时数据计算每日汇总"""
        query = text("""
            SELECT 
                DATE(datetime) as date,
                MIN(temp) as min_temp,
                MAX(temp) as max_temp,
                AVG(temp) as avg_temp,
                SUM(precip) as total_precip,
                AVG(wind_speed) as avg_wind_speed,
                AVG(humidity) as avg_humidity,
                COUNT(*) as record_count
            FROM hourly_weather 
            WHERE location_id = :location_id 
            AND datetime >= DATE_SUB(NOW(), INTERVAL :days DAY)
            GROUP BY DATE(datetime)
            ORDER BY date DESC
        """)
        
        df = pd.read_sql_query(query, self.engine, 
                             params={'location_id': location_id, 'days': days})
        return df
    
    def get_daily_data(self, location_id="101120101", days=7):
        """获取每日数据（兼容性方法）"""
        return self.get_daily_summary(location_id, days)
    
    def get_statistics(self, location_id="101120101"):
        """获取统计信息"""
        # 获取记录数
        hourly_count_query = text("SELECT COUNT(*) as count FROM hourly_weather WHERE location_id = :location_id")
        hourly_count = pd.read_sql_query(hourly_count_query, self.engine, 
                                       params={'location_id': location_id})
        
        daily_count_query = text("SELECT COUNT(*) as count FROM daily_weather WHERE location_id = :location_id")
        daily_count = pd.read_sql_query(daily_count_query, self.engine, 
                                      params={'location_id': location_id})
        
        # 获取时间范围
        time_range_query = text("SELECT MIN(datetime) as min_time, MAX(datetime) as max_time FROM hourly_weather WHERE location_id = :location_id")
        time_range = pd.read_sql_query(time_range_query, self.engine, 
                                     params={'location_id': location_id})
        
        # 获取温度统计
        temp_stats_query = text("SELECT MIN(temp) as min_temp, MAX(temp) as max_temp, AVG(temp) as avg_temp FROM hourly_weather WHERE location_id = :location_id")
        temp_stats = pd.read_sql_query(temp_stats_query, self.engine, 
                                     params={'location_id': location_id})
        
        # 获取降水统计
        precip_stats_query = text("SELECT SUM(precip) as total_precip, AVG(precip) as avg_precip FROM hourly_weather WHERE location_id = :location_id")
        precip_stats = pd.read_sql_query(precip_stats_query, self.engine, 
                                       params={'location_id': location_id})
        
        return {
            'hourly_count': hourly_count.iloc[0]['count'] if not hourly_count.empty else 0,
            'daily_count': daily_count.iloc[0]['count'] if not daily_count.empty else 0,
            'time_range': time_range,
            'temp_stats': temp_stats,
            'precip_stats': precip_stats
        }
    
    def search_weather_data(self, location_id="101120101", start_date=None, end_date=None, 
                           min_temp=None, max_temp=None, min_humidity=None, max_humidity=None):
        """搜索天气数据"""
        query_parts = ["SELECT * FROM hourly_weather WHERE location_id = :location_id"]
        params = {'location_id': location_id}
        
        if start_date:
            query_parts.append("AND DATE(datetime) >= :start_date")
            params['start_date'] = start_date
        
        if end_date:
            query_parts.append("AND DATE(datetime) <= :end_date")
            params['end_date'] = end_date
        
        if min_temp is not None:
            query_parts.append("AND temp >= :min_temp")
            params['min_temp'] = min_temp
        
        if max_temp is not None:
            query_parts.append("AND temp <= :max_temp")
            params['max_temp'] = max_temp
        
        if min_humidity is not None:
            query_parts.append("AND humidity >= :min_humidity")
            params['min_humidity'] = min_humidity
        
        if max_humidity is not None:
            query_parts.append("AND humidity <= :max_humidity")
            params['max_humidity'] = max_humidity
        
        query_parts.append("ORDER BY datetime DESC")
        
        query = text(" ".join(query_parts))
        df = pd.read_sql_query(query, self.engine, params=params)
        return df

def main():
    """主函数 - 演示查询功能"""
    print("🌤️  MySQL天气数据查询工具 (SQLAlchemy版)")
    print("=" * 60)
    print("✅ 使用SQLAlchemy连接，无警告信息")
    print("=" * 60)
    
    query_tool = WeatherDataQuery()
    
    try:
        # 获取统计信息
        print("📊 数据库统计信息:")
        stats = query_tool.get_statistics()
        print(f"   小时数据: {stats['hourly_count']} 条")
        print(f"   每日数据: {stats['daily_count']} 条")
        
        if not stats['time_range'].empty:
            min_time = stats['time_range'].iloc[0]['min_time']
            max_time = stats['time_range'].iloc[0]['max_time']
            if min_time and max_time:
                print(f"   数据时间范围: {min_time} 至 {max_time}")
        
        if not stats['temp_stats'].empty:
            temp_row = stats['temp_stats'].iloc[0]
            print(f"   温度范围: {temp_row['min_temp']:.1f}°C ~ {temp_row['max_temp']:.1f}°C (平均{temp_row['avg_temp']:.1f}°C)")
        
        if not stats['precip_stats'].empty:
            precip_row = stats['precip_stats'].iloc[0]
            print(f"   总降水量: {precip_row['total_precip']:.1f}mm (平均{precip_row['avg_precip']:.2f}mm/小时)")
        
        print("\n📋 最近7天的每日天气汇总:")
        daily_summary = query_tool.get_daily_summary(days=7)
        if not daily_summary.empty:
            for _, row in daily_summary.iterrows():
                precip = row['total_precip'] if pd.notna(row['total_precip']) else 0.0
                print(f"   {row['date']}: {row['min_temp']:.1f}°C~{row['max_temp']:.1f}°C "
                      f"(平均{row['avg_temp']:.1f}°C), 降水{precip:.1f}mm, "
                      f"风速{row['avg_wind_speed']:.1f}km/h, 湿度{row['avg_humidity']:.1f}%")
        else:
            print("   暂无每日汇总数据")
        
        print("\n⏰ 最近10条小时数据:")
        hourly_data = query_tool.get_hourly_data(days=7, limit=10)
        if not hourly_data.empty:
            for _, row in hourly_data.iterrows():
                precip = row['precip'] if pd.notna(row['precip']) else 0.0
                print(f"   {row['datetime']}: {row['temp']:.1f}°C, "
                      f"湿度{row['humidity']:.1f}%, 降水{precip:.1f}mm, "
                      f"风速{row['wind_speed']:.1f}km/h")
        else:
            print("   暂无小时数据")
        
        print("\n🔍 搜索示例 - 温度大于25°C的数据:")
        hot_days = query_tool.search_weather_data(min_temp=25)
        if not hot_days.empty:
            # 只显示前5条
            for _, row in hot_days.head(5).iterrows():
                print(f"   {row['datetime']}: {row['temp']:.1f}°C")
            if len(hot_days) > 5:
                print(f"   ... 还有 {len(hot_days) - 5} 条记录")
        else:
            print("   没有找到符合条件的记录")
            
        print("\n✅ 查询完成，完全无警告信息！")
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")

if __name__ == "__main__":
    main() 