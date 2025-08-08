# 🌤️ 全国天气数据收集与分析系统 v2.0

基于MySQL数据库的全国天气数据收集与分析系统，支持从和风天气API获取全国34个省级行政区、370+城市、3000+地区的详细天气数据，具备实时监控、自动重试、数据分析和可视化功能。

## 🚀 快速开始

### 1. 环境要求
- **Python**: 3.8+
- **MySQL**: 8.0+
- **和风天气API**: 需要注册账号获取权限
- **操作系统**: Windows/macOS/Linux 全平台支持

### 2. 安装依赖
```bash
pip install pymysql pandas requests PyJWT
```

### 3. 配置数据库
```bash
# 创建数据库
mysql -u root -p < 创建MySQL数据库_更新版.sql

# 初始化数据库结构
python -c "import mysql_db_utils; mysql_db_utils.init_mysql_database()"

# 测试数据库连接
python -c "import mysql_db_utils; conn = mysql_db_utils.get_mysql_connection(); print('✅ 数据库连接成功'); conn.close()"
```

### 4. 配置环境变量（推荐）
```bash
# 数据库配置
export DB_HOST=localhost
export DB_USER=root
export DB_PASSWORD=your_password
export DB_NAME=weather_db

# 数据范围配置
export CITY_CSV_PATH="全国城市（区分省）/山东省.csv"  # 按省份收集
export CITY_CSV_PATH="全国城市（区分省）/总表&省份汇总/全国城市列表.csv"  # 全国收集
```

## 📁 项目结构

```
气象数据收集/
├── 每日自动执行.py                 # 每日自动数据收集（含重试与日志）
├── mysql_db_utils.py               # MySQL数据库工具函数
├── 创建MySQL数据库_更新版.sql       # 数据库初始化脚本
├── 定时任务须知.md                  # 定时任务使用指南（macOS launchd）
├── README.md                       # 项目说明文档（当前）
├── logs/                           # 系统运行日志
│   ├── daily_weather_YYYYMMDD.log
│   ├── daily_weather_stdout.log
│   ├── daily_weather_stderr.log
│   └── retry_failed_*.log
├── 全国城市（区分省）/              # 城市与地区列表
│   ├── 总表&省份汇总/
│   │   ├── 全国城市列表.csv         # 全国3700+地区列表
│   │   └── 省份汇总.csv
│   └── 省/市级 CSV 若干

```

## 🎯 核心功能

### ✅ 数据收集能力
- **全国覆盖**: 支持34个省级行政区、370+城市、3000+地区
- **实时收集**: 每小时数据、每日汇总数据
- **历史回溯**: 支持任意日期范围的历史数据获取
- **自动重试**: 失败地区自动重试，最多5次，指数退避
- **限流保护**: 智能频率控制，避免API限制

### ✅ 实时监控特性
- **定时进度报告**: 每60秒显示收集进度
- **实时成功率**: 动计算成功率和失败统计
- **速度监控**: 显示处理速度和剩余时间估算
- **失败跟踪**: 实时记录失败地区，支持重试

### ✅ 数据存储优化
- **统一表结构**: `hourly_weather` + `daily_weather` 两张表
- **智能索引**: 基于时间、地点的多维度索引
- **数据去重**: 自动识别重复数据，智能更新
- **完整性检查**: 数质量验证和统计

### ✅ 灵活配置
- **省份级收集**: 可选择任意省份进行数据收集
- **城市级收集**: 支持单个或多个城市
- **地区级收集**: 精确定位到区县级别
- **环境变量**: 灵活配置数据库和收集范围

## 📊 数据库结构

### hourly_weather 表（小时级详细数据）
| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| id | INT | 主键，自增 | 1 |
| location_id | VARCHAR(20) | 地点ID | 101010100 |
| location_name | VARCHAR(50) | 地点名称 | 北京市 |
| province | VARCHAR(50) | 所属省份 | 北京市 |
| city | VARCHAR(50) | 所属城市 | 北京市 |
| datetime | DATETIME | 时间戳 | 2025-08-08 14:00:00 |
| temp_celsius | DECIMAL(4,1) | 温度(°C) | 28.5 |
| humidity_percent | DECIMAL(4,1) | 湿度(%) | 65.0 |
| precip_mm | DECIMAL(6,2) | 降水量(mm) | 0.00 |
| pressure_hpa | DECIMAL(6,1) | 气压(hPa) | 1013.2 |
| wind_scale | VARCHAR(5) | 风力等级 | 3级 |
| wind_dir | VARCHAR(10) | 风向 | 东南风 |
| text | VARCHAR(20) | 天气描述 | 晴 |

### daily_weather 表（每日汇总数据）
| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| id | INT | 主键，自增 | 1 |
| location_id | VARCHAR(20) | 地点ID | 101010100 |
| location_name | VARCHAR(50) | 地点名称 | 北京市 |
| province | VARCHAR(50) | 所属省份 | 北京市 |
| city | VARCHAR(50) | 所属城市 | 北京市 |
| date | DATE | 日期 | 2025-08-08 |
| temp_min_celsius | DECIMAL(4,1) | 最低温度(°C) | 22.1 |
| temp_max_celsius | DECIMAL(4,1) | 最高温度(°C) | 31.5 |
| humidity_percent | DECIMAL(4,1) | 平均湿度(%) | 68.5 |
| precip_mm | DECIMAL(6,2) | 总降水量(mm) | 2.5 |
| pressure_hpa | DECIMAL(6,1) | 平均气压(hPa) | 1012.8 |

## 🔧 使用指南

### 1. 每日自动收集（推荐）
```bash
# 设置收集范围（可选）
export CITY_CSV_PATH="全国城市（区分省）/山东省.csv"

# 执行每日自动收集
python 每日自动执行.py
```

## 📈 实时监控示例

运行时的实时输出示例：

```
🌤️ 开始获取昨天所有地区的天气数据...
⏱️ 进度更新: 30/152 地区完成 (成功率: 96.7%, 速度: 8.5地区/分钟, 预计剩余: 14.3分钟)
⏱️ 进度更新: 60/152 地区完成 (成功率: 98.3%, 速度: 9.2地区/分钟, 预计剩余: 10.0分钟)
⏱️ 进度更新: 90/152 地区完成 (成功率: 97.8%, 速度: 9.1地区/分钟, 预剩余: 6.8分钟)
⏱️ 进度更新: 152/152 地区完成 (成功率: 97.4%, 速度: 8.9地区/分钟, 预计剩余: 0.0分钟)
✅ 总计成功获取 148/152 个地区的数据: 3648 条小时记录, 152 条每日记录
❌ 失败地区详情: 即墨区, 胶州市
```

## 🔍 常用SQL查询示例

```sql
-- 查看最新数据
SELECT location_name, datetime, temp_celsius, humidity_percent 
FROM hourly_weather 
WHERE province = '山东省' 
ORDER BY datetime DESC 
LIMIT 10;

-- 统计各省份数据量
SELECT province, COUNT(DISTINCT location_name) as city_count,
       COUNT(*) as total_records
FROM hourly_weather 
GROUP BY province 
ORDER BY total_records DESC;

-- 某日温度统计
SELECT location_name, 
       MIN(temp_celsius) as min_temp,
       MAX(temp_celsius) as max_temp,
       AVG(temp_celsius) as avg_temp
FROM hourly_weather 
WHERE DATE(datetime) = '2025-08-08'
GROUP BY location_name;

-- 降水统计
SELECT location_name, SUM(precip_mm) as total_precip
FROM daily_weather 
WHERE date BETWEEN '20250801' AND '20250807'
GROUP BY location_name
HAVING total_precip > 0
ORDER BY total_precip DESC;
```

## ⚙️ 定时任务配置

### macOS (launchd)
```bash
# 查看定时任务状态
launchctl list | grep com.weather.daily

# 启动定时任务
launchctl load ~/Library/LaunchAgents/com.weather.daily.plist

# 停止定时任务
launchctl unload ~/Library/LaunchAgents/com.weather.daily.plist

# 查看实时日志
tail -f logs/daily_weather_$(date +%Y%m%d).log
```

更多详细说明与常见问题请参考项目内文档：`定时任务须知.md`。

### Linux (cron)
```bash
# 添加到crontab（每天凌晨2点执行
0 2 * * * cd /path/to/project && python 每日自动执行.py
```

## 📋 环境配置清单

### 必需环境变量
```bash
# 数据库配置
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=weather_db

# 数据收集范围
CITY_CSV_PATH="全国城市（区分省）/山东省.csv"

# 可选：JWT配置（如需自定义）
# JWT_PRIVATE_KEY="your_private_key"
# JWT_KID="your_key_id"
```

### 文件权限
```bash
chmod +x 每日自动执行.py
```

## 🎯 项目优势

### 🚀 性能优化
- **批量操作**: 数据库批量插入，减少IO次数
- **连接池**: MySQL连接复用，提高性能
- **智能索引**: 基于查询模式的索引优化
- **内存管理**: 大数据集分批次处理

### 🔒 稳定性保
- **异常处理**: 完整的错误捕获和恢复机制
- **重试策略**: 指数退避重试，避免API封禁
- **限流保护**: 智能频率控制
- **数据完整性**: 自动去重和完整性验证

### 📊 扩展性强
- **全国覆盖**: 支持所有省级行政区
- **灵活配置**: 环境变量驱动，零代码修改
- **多数据源**: 预留接口支持多天气API
- **水平扩展**: 支持分布式部署

## 🌟 新增特性（v2.0）

- ✅ **实时进度监控**: 每60秒报告收集进度
- ✅ **全国数据支持**: 扩展至34个省级行政区
- ✅ **智能重试**: 失败地区自动重试5次
- ✅ **性能优化**: 处理速度提升50%
- ✅ **时间估算**: 实时剩余时间预测
- ✅ **失败跟踪**: 详细失败原因记录

## 📞 技术支持与维护

### 常见问题排查
1. **数据库连接失败**: 检查MySQL服务状态和权限
2. **API认证失败**: 确认JWT Token有效性
3. **数据收集缓慢**: 检查网络连接和API限制
4. **地区缺失**: 验证CSV文件完整性

### 维护建议
- **定期更新**: 每月更新JWT Token
- **监控日志**: 关注失败率和异常情况
- **数据备份**: 定期备份数据库
- **性能监控**: 监控处理速度和资源使用

### 扩展开发
- **API集成**: 支持OpenWeatherMap、心知天气等
- **可视化**: 集成Grafana图表展示
- **预测模型**: 添加机器学习天气预测
- **告警通知**: 极端天气自动通知

---

**项目状态**: ✅ 生产环境就绪 | **最后更新**: 2025-08-08 | **版本**: v3.0