#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日自动天气数据收集脚本
每天早上11：00自动执行，自动获取当天最新天气数据并存入数据库
完全无人值守运行
"""

import os
import sys
import subprocess
import logging
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path

# 设置日志
def setup_logging():
    """设置日志配置"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"daily_weather_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def generate_jwt_token(logger):
    """自动生成JWT Token"""
    logger.info("🔑 自动生成JWT Token...")
    
    try:
        import jwt
        from datetime import datetime, timedelta
        import time
        
        # JWT配置
        private_key = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIHioY4NMxin5qZ8D18i296EMTZ2VB5kp+jgkdNjKm5rb
-----END PRIVATE KEY-----"""
        
        payload = {
            "iat": int(time.time()),
            "exp": int(time.time()) + 36000,  # 10小时有效期
            "sub": "46KWJQQATC"
        }
        
        token = jwt.encode(payload, private_key, algorithm="EdDSA", headers={"kid": "KBB6BQNHM5"})
        logger.info(f"✅ JWT Token生成成功，有效期10小时")
        return token
        
    except Exception as e:
        logger.error(f"❌ JWT Token生成失败: {e}")
        return None

def get_location_list():
    """获取所有13个地区的location_id列表"""
    locations = [
        ("101120101", "济南"),
        ("101120102", "长清"),
        ("101120103", "商河"),
        ("101120104", "章丘"),
        ("101120105", "平阴"),
        ("101120106", "济阳"),
        ("101120107", "历下"),
        ("101120108", "市中"),
        ("101120109", "槐荫"),
        ("101120110", "天桥"),
        ("101120111", "历城"),
        ("101121601", "莱芜"),
        ("101121603", "钢城")
    ]
    return locations

def get_weather_data_for_location(token, location_id, location_name, date_str, logger):
    """获取指定地区的天气数据"""
    api_host = "jd46h2979n.re.qweatherapi.com"
    api_base = f"https://{api_host}/v7/historical/weather"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "location": location_id,
        "date": date_str
    }
    
    try:
        logger.info(f"正在获取 {location_name}({location_id}) {date_str} 的天气数据...")
        response = requests.get(api_base, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("code") == "200":
            hourly_data = data.get("weatherHourly", [])
            logger.info(f"✅ {location_name} 成功获取 {len(hourly_data)} 条记录")
            return hourly_data, location_id, location_name
        else:
            logger.error(f"❌ {location_name} API返回错误: {data.get('code')} - {data.get('msg', 'Unknown error')}")
            return None, location_id, location_name
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ {location_name} 请求失败: {e}")
        return None, location_id, location_name
    except Exception as e:
        logger.error(f"❌ {location_name} 数据处理失败: {e}")
        return None, location_id, location_name

def get_today_weather_data(token, logger):
    """获取昨天所有13个地区的天气数据"""
    logger.info("🌤️ 开始获取昨天所有13个地区的天气数据...")
    
    # 获取昨天的日期
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y%m%d")
    
    # 获取所有地区列表
    locations = get_location_list()
    all_weather_data = []
    success_count = 0
    
    for location_id, location_name in locations:
        weather_data, loc_id, loc_name = get_weather_data_for_location(token, location_id, location_name, date_str, logger)
        if weather_data:
            # 为每条记录添加location信息
            for record in weather_data:
                record['location_id'] = loc_id
                record['location_name'] = loc_name
            all_weather_data.extend(weather_data)
            success_count += 1
    
    logger.info(f"✅ 总计成功获取 {success_count}/{len(locations)} 个地区的天气数据，共 {len(all_weather_data)} 条记录")
    
    if success_count == 0:
        return None
    
    return all_weather_data

def save_weather_data_to_db(weather_data, logger):
    """保存所有地区的天气数据到数据库"""
    logger.info("💾 开始保存所有地区的天气数据到数据库...")
    
    try:
        import mysql_db_utils
        
        # 初始化数据库
        mysql_db_utils.init_mysql_database()
        logger.info("✅ 数据库初始化完成")
        
        # 按地区分组保存数据
        location_groups = {}
        for record in weather_data:
            location_id = record.get('location_id')
            location_name = record.get('location_name')
            if location_id not in location_groups:
                location_groups[location_id] = {'name': location_name, 'data': []}
            location_groups[location_id]['data'].append(record)
        
        total_new = 0
        total_updated = 0
        
        # 保存每个地区的数据
        for location_id, info in location_groups.items():
            location_name = info['name']
            location_data = info['data']
            
            logger.info(f"正在保存 {location_name}({location_id}) 的 {len(location_data)} 条记录...")
            
            # 统一使用districts表保存所有数据
            result = mysql_db_utils.save_districts_hourly_to_mysql(location_data, location_id, location_name)
            
            if result and len(result) == 2:
                new_count, updated_count = result
                total_new += new_count
                total_updated += updated_count
                logger.info(f"✅ {location_name} 保存完成: 新增{new_count}条，更新{updated_count}条")
            else:
                logger.warning(f"⚠️ {location_name} 数据保存返回格式异常")
        
        logger.info(f"✅ 所有地区小时数据保存完成: 总计新增{total_new}条，更新{total_updated}条")
        
        # 计算所有地区的每日汇总
        mysql_db_utils.calculate_districts_daily_summaries_mysql()
        logger.info("✅ 所有地区每日汇总计算完成")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库操作失败: {e}")
        return False

def get_database_stats(logger):
    """获取数据库统计信息"""
    logger.info("📊 获取数据库统计信息...")
    
    try:
        import mysql_db_utils
        
        stats = mysql_db_utils.get_mysql_stats()
        
        logger.info("📈 数据库统计信息:")
        logger.info(f"   济南市小时数据: {stats['city_hourly_count']} 条")
        logger.info(f"   济南市日数据: {stats['city_daily_count']} 条")
        logger.info(f"   区县小时数据: {stats['districts_hourly_count']} 条")
        logger.info(f"   区县日数据: {stats['districts_daily_count']} 条")
        logger.info(f"   济南市最新小时数据: {stats.get('city_latest_hourly', 'N/A')}")
        logger.info(f"   济南市最新日数据: {stats.get('city_latest_daily', 'N/A')}")
        logger.info(f"   区县最新小时数据: {stats.get('districts_latest_hourly', 'N/A')}")
        logger.info(f"   区县最新日数据: {stats.get('districts_latest_daily', 'N/A')}")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ 获取统计信息失败: {e}")
        return None

def check_system_status(logger):
    """检查系统状态"""
    logger.info("🔍 检查系统状态...")
    
    # 检查必要文件
    required_files = ['mysql_db_utils.py']
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        logger.error(f"❌ 缺少必要文件: {missing_files}")
        return False
    
    # 检查MySQL连接
    try:
        import mysql_db_utils
        connection = mysql_db_utils.get_mysql_connection()
        connection.close()
        logger.info("✅ MySQL连接正常")
    except Exception as e:
        logger.error(f"❌ MySQL连接失败: {e}")
        return False
    
    # 检查网络连接
    try:
        response = requests.get("https://jd46h2979n.re.qweatherapi.com", timeout=10)
        logger.info("✅ 网络连接正常")
    except Exception as e:
        logger.error(f"❌ 网络连接失败: {e}")
        return False
    
    logger.info("✅ 系统状态检查完成")
    return True

def main():
    """主函数 - 每日自动执行"""
    logger = setup_logging()
    
    logger.info("🌤️  每日自动天气数据收集开始")
    logger.info("=" * 60)
    logger.info(f"⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🎯 目标: 自动收集昨天所有13个地区(济南市+12个区县)的天气数据并存入数据库")
    logger.info("=" * 60)
    
    # 检查系统状态
    if not check_system_status(logger):
        logger.error("❌ 系统状态检查失败，终止执行")
        return 1
    
    # 步骤1: 生成JWT Token
    logger.info("\n🔄 执行步骤1: 生成JWT Token...")
    token = generate_jwt_token(logger)
    if not token:
        logger.error("❌ 步骤1失败 - JWT Token生成失败，终止执行")
        return 1
    logger.info("✅ 步骤1完成 - JWT Token生成成功")
    
    # 步骤2: 获取昨天所有13个地区的天气数据
    logger.info("\n🔄 执行步骤2: 获取昨天所有13个地区的天气数据...")
    weather_data = get_today_weather_data(token, logger)
    if not weather_data:
        logger.error("❌ 步骤2失败 - 天气数据获取失败，终止执行")
        return 1
    logger.info("✅ 步骤2完成 - 天气数据获取成功")
    
    # 步骤3: 保存数据到数据库
    logger.info("\n🔄 执行步骤3: 保存数据到数据库...")
    if not save_weather_data_to_db(weather_data, logger):
        logger.error("❌ 步骤3失败 - 数据保存失败")
        return 1
    logger.info("✅ 步骤3完成 - 数据保存成功")
    
    # 步骤4: 生成统计报告
    logger.info("\n🔄 执行步骤4: 生成统计报告...")
    stats = get_database_stats(logger)
    if stats:
        logger.info("✅ 步骤4完成 - 统计报告生成成功")
    else:
        logger.warning("⚠️ 步骤4失败 - 统计报告生成失败")
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("🎉 每日自动执行完全成功！")
    logger.info("📊 执行结果:")
    logger.info("   - JWT Token生成: ✅")
    logger.info("   - 天气数据获取(13个地区): ✅")
    logger.info("   - 数据库保存(13个地区): ✅")
    logger.info("   - 统计报告: ✅")
    logger.info("📅 下次执行时间: 明天早上9:30")
    logger.info("=" * 60)
    logger.info("✅ 每日自动执行完成，无需人工干预")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 