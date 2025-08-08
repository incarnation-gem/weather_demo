#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日自动天气数据收集脚本
每天自动执行，自动获取当天最新天气数据并存入数据库
完全无人值守运行
"""

# 配置：修改这里即可切换数收集范围
# 按省份收集："全国城市（区分省）/福建省.csv"
# 全国收集："全国城市（区分省）/总表&省份汇总/全国城市列表.csv"

import os
import sys
import subprocess
import logging
import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path



CSV_PATH = os.getenv('CITY_CSV_PATH')
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
    """获取指定CSV文件中的地区列表"""
    import csv
    import os
    
    locations = []
    csv_path = CSV_PATH  # 使用顶部配置的CSV_PATH
    
    if os.path.exists(csv_path):
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                loaded_count = 0
                for row in reader:
                    location_id = str(row.get('location_id', '')).strip()
                    city_name = str(row.get('location_name', '')).strip()
                    
                    # 过滤掉注释行和无效行
                    if location_id and city_name and not location_id.startswith('#') and not city_name.startswith('#'):
                        locations.append((location_id, city_name))
                        loaded_count += 1
                
                print(f"📊 从{os.path.basename(csv_path)}成功加载{loaded_count}个地区")
        except Exception as e:
            print(f"⚠️ 读取城市列表失败: {e}")
            return []
    else:
        print(f"❌ 找不到城市列表文件: {csv_path}")
        return []
    
    # 去重并排序
    unique_locations = list(set(locations))
    unique_locations.sort(key=lambda x: x[0])
    
    print(f"✅ 已加载{len(unique_locations)}个地区数据")
    return unique_locations

def get_weather_data_for_location(token, location_id, location_name, date_str, logger, max_retries=3, retry_delay=2):
    """获取指定地区的天气数据（同时获取小时和每日数据，带即时重试机制)"""
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
    
    for attempt in range(max_retries + 1):
        try:
            if attempt == 0:
                logger.info(f"正在获取 {location_name}({location_id}) {date_str} 的天气数据...")
            else:
                logger.info(f"🔄 正在重试 {location_name}({location_id}) 第{attempt}次...")
                
            response = requests.get(api_base, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("code") == "200":
                hourly_data = data.get("weatherHourly", [])
                daily_data = data.get("weatherDaily", [])
                
                # 规范化每日数据为列表，便于统一处理
                if isinstance(daily_data, dict):
                    daily_list = [daily_data]
                elif isinstance(daily_data, list):
                    daily_list = daily_data
                else:
                    daily_list = []
                
                actual_daily_count = len(daily_list)
                
                if attempt == 0:
                    logger.info(f"✅ {location_name} 成功获取 {len(hourly_data)} 条小时记录和 {actual_daily_count} 条每日记录")
                else:
                    logger.info(f"✅ {location_name} 重试成功！获取 {len(hourly_data)} 条小时记录和 {actual_daily_count} 条每日记录")
                
                return hourly_data, daily_list, location_id, location_name
            else:
                error_msg = f"API返回错误: {data.get('code')} - {data.get('msg', 'Unknown error')}"
                if attempt < max_retries:
                    logger.warning(f"⚠️ {location_name} {error_msg}，{retry_delay}秒后重试...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"❌ {location_name} {error_msg}，已重试{max_retries}次仍失败")
                    
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logger.warning(f"⚠️ {location_name} 请求失败: {e}，{retry_delay}秒后重试...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"❌ {location_name} 请求失败: {e}，已重试{max_retries}次仍失败")
                
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"⚠️ {location_name} 数据处理失败: {e}，{retry_delay}秒后重试...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error(f"❌ {location_name} 数据处理失败: {e}，已重试{max_retries}次仍失败")
    
    return None, None, location_id, location_name

def get_today_weather_data(token, logger):
    """获取昨天所有地区的天气数据（小时和每日数据，带定时进度报告）"""
    logger.info("🌤️ 开始获取昨天所有地区的天气数据...")
    
    # 获取昨天的日期
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y%m%d")
    
    # 获取所有地区列表
    locations = get_location_list()
    all_hourly_data = []
    all_daily_data = []
    success_count = 0
    failed_locations = []  # 记录失败的地区
    
    # 进度报告相关变量
    start_time = time.time()
    last_report_time = start_time
    report_interval = 60  # 每60秒报告一次进度
    
    for i, (location_id, location_name) in enumerate(locations, 1):
        hourly_data, daily_data, loc_id, loc_name = get_weather_data_for_location(token, location_id, location_name, date_str, logger)
        
        if hourly_data or daily_data:
            # 为小时记录添加location信息
            if hourly_data:
                for record in hourly_data:
                    record['location_id'] = loc_id
                    record['location_name'] = loc_name
                all_hourly_data.extend(hourly_data)
            
            if daily_data:
                # daily_data现在已经是列表格式
                for record in daily_data:
                    record['location_id'] = loc_id
                    record['location_name'] = loc_name
                all_daily_data.extend(daily_data)
            
            success_count += 1
        else:
            # 记录失败的地区
            failed_locations.append(location_name)
        
        # 定时报告进度
        current_time = time.time()
        if current_time - last_report_time >= report_interval or i == len(locations):
            elapsed = current_time - start_time
            speed = i / elapsed * 60  # 每分钟处理多少个地区
            remaining_time = (len(locations) - i) / (i / elapsed) if i > 0 else 0
            
            logger.info(f"⏱️ 进度更新: {i}/{len(locations)} 地区完成 "
                       f"(成功率: {success_count/i*100:.1f}%, "
                       f"速度: {speed:.1f}地区/分钟, "
                       f"预计剩余: {remaining_time/60:.1f}分钟)")
            last_report_time = current_time
        
        # 限流机制：每个API请求后暂停0.5秒，避免频率限制
        time.sleep(0.5)
    
    logger.info(f"✅ 总计成功获取 {success_count}/{len(locations)} 个地区的数据: {len(all_hourly_data)} 条小时记录, {len(all_daily_data)} 条每日记录")
    
    # 记录失败的地区详情
    if failed_locations:
        logger.info(f"❌ 失败地区详情: {', '.join(failed_locations)}")
    
    if success_count == 0:
        return None, None, 0, [], failed_locations
    
    return all_hourly_data, all_daily_data, success_count, locations, failed_locations

def save_weather_data_to_db(hourly_data, daily_data, logger):
    """保存所有地区的天气数据到数据库（包括小时和每日数据）"""
    logger.info("💾 开始保存所有地区的天数据到数据库...")
    
    try:
        import mysql_db_utils
        
        # 初始化数据库
        mysql_db_utils.init_mysql_database()
        logger.info("✅ 数据库初始化完成")
        
        # 保存小时数据
        if hourly_data:
            # 按地区分组保存小时数据
            hourly_groups = {}
            for record in hourly_data:
                location_id = record.get('location_id')
                location_name = record.get('location_name')
                if location_id not in hourly_groups:
                    hourly_groups[location_id] = {'name': location_name, 'data': []}
                hourly_groups[location_id]['data'].append(record)
            
            total_hourly_new = 0
            total_hourly_updated = 0
            
            for location_id, info in hourly_groups.items():
                location_name = info['name']
                location_data = info['data']
                
                logger.info(f"正在保存 {location_name}({location_id}) 的 {len(location_data)} 条小时记录...")
                
                result = mysql_db_utils.save_districts_hourly_to_mysql(location_data, location_id, location_name, CSV_PATH)
                
                if result and len(result) == 2:
                    new_count, updated_count = result
                    total_hourly_new += new_count
                    total_hourly_updated += updated_count
                    logger.info(f"✅ {location_name} 小时数据保存完成: 新增{new_count}条，更新{updated_count}条")
                else:
                    logger.warning(f"⚠️ {location_name} 小时数据保存返回格式异常")
            
            logger.info(f"✅ 所有地区小时数据保存完成: 总计新增{total_hourly_new}条，更新{total_hourly_updated}条")
        
        # 保存每日数据
        if daily_data:
            # 按地区分组保存每日数据
            daily_groups = {}
            for record in daily_data:
                location_id = record.get('location_id')
                location_name = record.get('location_name')
                if location_id not in daily_groups:
                    daily_groups[location_id] = {'name': location_name, 'data': None}
                daily_groups[location_id]['data'] = record  # 每日数据每天只有一条
            
            total_daily_new = 0
            total_daily_updated = 0
            
            for location_id, info in daily_groups.items():
                location_name = info['name']
                location_data = info['data']
                
                logger.info(f"正在保存 {location_name}({location_id}) 的每日记录...")
                
                result = mysql_db_utils.save_daily_weather_mysql(location_data, location_id, location_name)
                
                if result and len(result) == 2:
                    new_count, updated_count = result
                    total_daily_new += new_count
                    total_daily_updated += updated_count
                    logger.info(f"✅ {location_name} 每日数据保存完成: 新增{new_count}条，更新{updated_count}条")
                else:
                    logger.warning(f"⚠️ {location_name} 每日数据保存返回格式异常")
            
            logger.info(f"✅ 所有地区每日数据保存完成: 总计新增{total_daily_new}条，更新{total_daily_updated}条")
        
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
        logger.info(f"   全国小时数据总条数: {stats['total_hourly']} 条")
        logger.info(f"   全国日数据总条数: {stats['total_daily']} 条")

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
    start_time = datetime.now()
    logger = setup_logging()
    
    logger.info("🌤️  每日自动天气数据收集开始")
    logger.info("=" * 60)
    logger.info(f"⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🎯 目标: 自动收集昨天所有个地区的天气数据并存入数据库")
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
    
    # 步骤2: 获取昨天所有地区的天气数据
    logger.info("\n🔄 执行步骤2: 获取昨天所有地区的天气数据...")
    result = get_today_weather_data(token, logger)
    if len(result) == 5:
        hourly_data, daily_data, success_count, locations, failed_locations = result
    else:
        # 兼容旧版本
        hourly_data, daily_data, success_count, locations = result
        failed_locations = []
    
    if not hourly_data and not daily_data:
        logger.error("❌ 步骤2失败 - 天气数据获取失败，终止执行")
        return 1
    logger.info("✅ 步骤2完成 - 天气数据获取成功")
    
    # 步骤3: 保存数据到数据库
    logger.info("\n🔄 执行步骤3: 保存数据到数据库...")
    if not save_weather_data_to_db(hourly_data, daily_data, logger):
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
    
    # 总结 - 完全动态化的日志总结
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    处理日期 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    下次执行 = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 02:00")
    
    # 计算成功率和质量指标
    成功率 = (success_count / len(locations) * 100) if locations else 0
    失败数 = len(failed_locations) if 'failed_locations' in locals() else (len(locations) - success_count)
    小时数据期望 = success_count * 24 if success_count > 0 else 0
    每日数据期望 = success_count if success_count > 0 else 0
    
    logger.info("\n" + "=" * 70)
    logger.info("🎉 每日自动执行完全成功！")
    logger.info("📊 详细执行报告:")
    logger.info(f"   📅 处理日期: {处理日期}")
    logger.info(f"   📍 地区统计: {success_count}/{len(locations)} 个地区成功")
    logger.info(f"   ✅ 成功率: {成功率:.1f}%")
    if 失败数 > 0:
        logger.info(f"   ❌ 失败地区: {失败数}个")
        try:
            if 'failed_locations' in locals() and failed_locations:
                logger.info(f"   ❌ 失败地区详情: {', '.join(failed_locations)}")
        except Exception as e:
            logger.warning(f"   ⚠️ 无法显示失败地区详情: {e}")
    try:
        logger.info(f"   📊 小时数据: {len(hourly_data) if hourly_data else 0}条 (期望: {小时数据期望}条)")
        logger.info(f"   📊 每日数据: {len(daily_data) if daily_data else 0}条 (期望: {每日数据期望}条)")
        logger.info(f"   ⏱️ 执行耗时: {execution_time:.1f}秒")
        
        # 数据质量指标
        小时完整性 = (len(hourly_data) / 小时数据期望 * 100) if 小时数据期望 > 0 and hourly_data else 0
        每日完整性 = (len(daily_data) / 每日数据期望 * 100) if 每日数据期望 > 0 and daily_data else 0
    except Exception as e:
        logger.warning(f"   ⚠️ 计算数据统计时出错: {e}")
        logger.info(f"   📊 小时数据: 0条 (期望: {小时数据期望}条)")
        logger.info(f"   📊 每日数据: 0条 (期望: {每日数据期望}条)")
        logger.info(f"   ⏱️ 执行耗时: {execution_time:.1f}秒")
        小时完整性 = 0
        每日完整性 = 0
    
    if 小时数据期望 > 0:
        logger.info(f"   📊 小时数据完整性: {小时完整性:.1f}%")
    if 每日数据期望 > 0:
        logger.info(f"   📊 每日数据完整性: {每日完整性:.1f}%")
    
    logger.info("📅 下次执行时间: 明天凌晨02:00")
    logger.info("=" * 70)
    logger.info("✅ 每日自动执行完成，无需人工干预")
    



    # 立刻进行失败地区重试机制
    if 'failed_locations' in locals() and failed_locations:
        logger.info("\n🔄 开始重试失败地区...")
        retry_success_count = 0
        retry_failed_locations = []
        
        for location_name in failed_locations:
            # 从locations中找到对应的location_id
            location_id = None
            for loc_id, loc_name in locations:
                if loc_name == location_name:
                    location_id = loc_id
                    break
            
            if location_id:
                logger.info(f"🔄 重试获取 {location_name}({location_id}) 的天气数据...")
                
                # 获取昨天的日期
                yesterday = datetime.now() - timedelta(days=1)
                date_str = yesterday.strftime("%Y%m%d")
                
                # 重试获取数据
                hourly_data, daily_data, loc_id, loc_name = get_weather_data_for_location(
                    token, location_id, location_name, date_str, logger, max_retries=5, retry_delay=3
                )
                
                if hourly_data or daily_data:
                    # 为小时记录添加location信息
                    if hourly_data:
                        for record in hourly_data:
                            record['location_id'] = loc_id
                            record['location_name'] = loc_name
                    
                    if daily_data:
                        for record in daily_data:
                            record['location_id'] = loc_id
                            record['location_name'] = loc_name
                    
                    # 保存重试成功的数据
                    if hourly_data or daily_data:
                        if save_weather_data_to_db(hourly_data, daily_data, logger):
                            logger.info(f"✅ {location_name} 重试成功！数据已保存")
                            retry_success_count += 1
                        else:
                            logger.warning(f"⚠️ {location_name} 重试成功但保存失败")
                            retry_failed_locations.append(location_name)
                    else:
                        logger.warning(f"⚠️ {location_name} 重试失败")
                        retry_failed_locations.append(location_name)
                else:
                    logger.warning(f"⚠️ {location_name} 重试失败")
                    retry_failed_locations.append(location_name)
                
                # 重试间隔
                time.sleep(1)
            else:
                logger.warning(f"⚠️ 无法找到 {location_name} 的location_id")
                retry_failed_locations.append(location_name)
        
        # 重试结果总结
        if retry_success_count > 0:
            logger.info(f"\n🎉 重试结果: {retry_success_count}/{len(failed_locations)} 个失败地区重试成功")
            if retry_failed_locations:
                logger.info(f"❌ 仍有 {len(retry_failed_locations)} 个地区失败: {', '.join(retry_failed_locations)}")
        else:
            logger.info(f"\n⚠️ 重试结果: 所有 {len(failed_locations)} 个失败地区重试失败")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 

   