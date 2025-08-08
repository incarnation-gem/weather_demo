# 🌤️ 天气数据收集与分析系统

基于MySQL数据库的天气数据收集与分析系统，用于从和风天气API获取济南市及12个区县的历史天气数据，并进行存储、分析和预测。

## 🚀 快速开始

### 1. 环境要求
- Python 3.8+
- MySQL 8.0+
- 和风天气API账号

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
```

### 4. 测试系统
```bash
# 测试数据库连接
python -c "import mysql_db_utils; conn = mysql_db_utils.get_mysql_connection(); print('✅ 数据库连接成功'); conn.close()"

# 测试定时任务脚本
python 每日自动执行.py
```

## 📁 项目结构

```
气象数据收集（山东）/
├── 核心脚本/
│   ├── 每日自动执行.py              # 每日自动数据收集脚本
│   ├── 灵活获取气象数据.py          # 灵活获取气象数据脚本（推荐）
│   ├── 手动获取历史天气（时段可调）.py  # 手动获取历史数据脚本
│   ├── 数据库查询工具_SQLAlchemy版.py  # 数据查询和分析工具
│   ├── 生成JWT.py                  # JWT Token生成工具
│   └── mysql_db_utils.py           # MySQL数据库工具函数
├── 数据库/
│   └── 创建MySQL数据库_更新版.sql    # 数据库初始化脚本
├── 定时任务/
│   └── com.weather.daily.plist     # 定时任务配置文件
├── 文档/
│   ├── 自动化系统设置指南.md         # 系统设置说明
│   ├── 定时任务使用指南.md           # 定时任务使用说明
│   ├── MySQL免密码配置指南.md        # MySQL配置指南
│   └── README.md                   # 项目说明
├── 工具脚本/
│   └── mysql_quick.sh              # MySQL快速查询脚本
├── 数据文件/
│   ├── 山东济南气象数据/             # 山东省原始数据文件
│   └── 全国城市（区分省）/           # 全国城市数据文件
└── 日志文件/
    └── logs/                       # 系统运行日志
```

## 🔧 主要功能

### 数据收集
- ✅ 山东省152个地区的天气数据收集
- ✅ 每日自动数据收集（11:00 AM）
- ✅ 手动获取历史数据（支持自定义日期范围）
- ✅ JWT Token自动刷新
- ✅ 错误重试机制
- ✅ 数据完整性检查
- ✅ 支持多省份数据收集扩展

### 数据存储
- ✅ MySQL数据库存储
- ✅ 统一的小时级和每日汇总数据
- ✅ 数据去重和完整性检查
- ✅ 自动索引优化
- ✅ 多地区数据统一管理

### 数据分析
- ✅ 统计信息查询
- ✅ 时间序列分析
- ✅ 条件搜索
- ✅ 数据导出
- ✅ 地区对比分析

### 自动化
- ✅ 定时任务支持（macOS launchd）
- ✅ 详细日志记录
- ✅ 异常处理
- ✅ 系统状态监控

## 📊 数据库结构

### hourly_weather 表（统一的小时天气数据）
| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 主键，自增 |
| location_id | VARCHAR(20) | 地点ID |
| location_name | VARCHAR(50) | 地点名称（济南、历下等） |
| datetime | DATETIME | 日期时间 |
| temp | DECIMAL(4,1) | 温度(°C) |
| humidity | DECIMAL(4,1) | 湿度(%) |
| precip | DECIMAL(6,2) | 降水量(mm) |
| pressure | DECIMAL(6,1) | 气压(hPa) |
| wind_speed | DECIMAL(5,1) | 风速(km/h) |
| wind_dir | VARCHAR(10) | 风向 |

### daily_weather 表（统一的每日天气汇总）
| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 主键，自增 |
| location_id | VARCHAR(20) | 地点ID |
| location_name | VARCHAR(50) | 地点名称（济南、历下等） |
| date | DATE | 日期 |
| min_temp | DECIMAL(4,1) | 最低温度(°C) |
| max_temp | DECIMAL(4,1) | 最高温度(°C) |
| avg_temp | DECIMAL(4,1) | 平均温度(°C) |
| total_precip | DECIMAL(6,2) | 总降水量(mm) |
| avg_wind_speed | DECIMAL(5,1) | 平均风速(km/h) |
| avg_humidity | DECIMAL(4,1) | 平均湿度(%) |
| record_count | INT | 小时记录数量 |

### location_info 表（位置信息）
| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 主键，自增 |
| location_id | VARCHAR(20) | 地点ID |
| location_name | VARCHAR(50) | 地点名称 |

## 🎯 使用示例

### 基本操作
```bash
# 手动执行数据收集
python 每日自动执行.py

# 查询数据
python 数据库查询工具_SQLAlchemy版.py

# 生成JWT Token
python 生成JWT.py

# 快速数据库查询
./mysql_quick.sh stats
```

### 手动获取历史数据
```bash
# 使用默认日期范围（7月30日-8月2日）
python 手动获取历史天气（时段可调）.py

# 指定日期范围（格式：YYYYMMDD）
python 手动获取历史天气（时段可调）.py 20250730 20250802

# 获取单日数据
python 手动获取历史天气（时段可调）.py 20250801 20250801
```

### 灵活获取气象数据（推荐）
```bash
# 按省份获取
python 灵活获取气象数据.py 山东省 20250801 20250803

# 按城市获取
python 灵活获取气象数据.py city 济南市 20250801 20250803

# 按地区获取（支持多个地区）
python 灵活获取气象数据.py locations 济南,青岛,淄博 20250801 20250803

# 查看可用选项
python 灵活获取气象数据.py provinces
python 灵活获取气象数据.py cities
python 灵活获取气象数据.py cities --filter-province 广东省  # 查看指定省份的城市
python 灵活获取气象数据.py locations
```

#### 修改地区范围

**方法1：使用灵活获取脚本（推荐）**
```bash
# 按省份获取
python 灵活获取气象数据.py 山东省 20250801 20250803

# 按城市获取
python 灵活获取气象数据.py city 济南市 20250801 20250803

# 按地区获取（支持多个地区）
python 灵活获取气象数据.py locations 济南,青岛,淄博 20250801 20250803
```

**方法2：修改手动获取脚本**
如需修改收集的地区范围，请编辑 `手动获取历史天气（时段可调）.py` 文件中的 `get_location_list()` 函数：

```python
def get_location_list():
    """获取地区列表"""
    # 当前使用山东省数据
    csv_path = "山东济南气象数据/山东省完整城市列表.csv"
    
    # 如需使用其他省份，修改为：
    # csv_path = "全国城市（区分省）/广东省.csv"  # 广东省
    # csv_path = "全国城市（区分省）/江苏省.csv"  # 江苏省
    # csv_path = "全国城市（区分省）/浙江省.csv"  # 浙江省
    
    # 如需使用全国数据，修改为：
    # csv_path = "全国城市（区分省）/全国城市列表.csv"
```

#### 修改时间范围
脚本支持两种时间范围设置方式：

1. **命令行参数**（推荐）：
   ```bash
   # 获取2025年7月1日到7月31日的数据
   python 手动获取历史天气（时段可调）.py 20250701 20250731
   
   # 获取2025年8月1日的数据
   python 手动获取历史天气（时段可调）.py 20250801 20250801
   ```

2. **修改默认值**：
   编辑脚本中的默认日期设置：
   ```python
   # 默认获取7月30日-8月2日
   start_date = "20250730"  # 修改开始日期
   end_date = "20250802"    # 修改结束日期
   ```

#### 注意事项
- **日期格式**：必须使用YYYYMMDD格式（如：20250801）
- **API限制**：注意和风天气API的调用次数限制
- **数据量**：大量数据获取可能需要较长时间
- **网络稳定性**：确保网络连接稳定
- **存储空间**：确保数据库有足够存储空间

### MySQL查询示例
```sql
-- 查看最近数据
SELECT * FROM hourly_weather ORDER BY datetime DESC LIMIT 5;

-- 统计信息
SELECT location_name, COUNT(*) FROM hourly_weather GROUP BY location_name;

-- 温度统计
SELECT location_name, MIN(temp), MAX(temp), AVG(temp) 
FROM hourly_weather 
GROUP BY location_name;

-- 查看济南市数据
SELECT * FROM hourly_weather WHERE location_name = '济南' ORDER BY datetime DESC LIMIT 10;
```

### 定时任务设置
```bash
# 查看定时任务状态
launchctl list | grep com.weather.daily

# 手动启动定时任务
launchctl load ~/Library/LaunchAgents/com.weather.daily.plist

# 停止定时任务
launchctl unload ~/Library/LaunchAgents/com.weather.daily.plist
```

## 📈 性能特点

- **数据库性能**: 索引优化，批量操作，连接池管理
- **系统性能**: 并发处理，内存优化，网络优化
- **数据完整性**: 外键约束，数据验证，备份策略
- **扩展性**: 支持多地区数据统一管理

## 🔍 监控和维护

- **数据监控**: 完整性检查，记录数统计，时间序列验证
- **系统监控**: 日志分析，性能监控，错误追踪
- **备份策略**: 自动备份，数据恢复，版本管理

## 🎯 应用价值

### 商业价值
- 销售预测：天气数据可用于商品销售预测
- 库存管理：根据天气变化调整库存策略
- 营销策略：天气相关的营销活动策划
- 风险评估：天气对业务影响的风险评估

### 技术价值
- 数据驱动：基于真实数据的决策支持
- 自动化：减少人工数据收集工作
- 可扩展性：支持多地区、多时间尺度扩展
- 标准化：标准化的数据格式和处理流程

## 🔮 未来规划

### 功能扩展
- 多地区支持：扩展到更多城市和地区
- 预测模型：集成机器学习预测模型
- 可视化：添加数据可视化界面
- API服务：提供RESTful API服务

### 技术升级
- 微服务架构：拆分为独立的微服务
- 容器化部署：使用Docker容器化部署
- 云原生：迁移到云平台
- 实时处理：支持实时数据流处理

## 📞 技术支持

### 常见问题
1. **数据库连接问题**: 检查MySQL服务状态和配置
2. **API认证问题**: 检查JWT Token是否有效
3. **数据质量问题**: 验证API返回数据的完整性
4. **性能问题**: 优化查询语句和索引

### 维护建议
- 定期更新JWT Token
- 监控数据库空间使用
- 备份重要数据
- 更新依赖包版本

## 🚀 快速参考

### 常用命令
```bash
# 查看定时任务状态
launchctl list | grep com.weather.daily

# 手动执行数据收集
python 每日自动执行.py

# 灵活获取气象数据（推荐）
python 灵活获取气象数据.py --city 济南市 20250801 20250803

# 手动获取历史数据
python 手动获取历史天气（时段可调）.py 20250801 20250803

# 查看最新日志
tail -f logs/daily_weather_$(date +%Y%m%d).log

# 数据库查询
python 数据库查询工具_SQLAlchemy版.py
```

### 文件路径说明
- **自动执行脚本**: `每日自动执行.py`
- **灵活获取脚本**: `灵活获取气象数据.py`（推荐）
- **手动获取脚本**: `手动获取历史天气（时段可调）.py`
- **山东省数据**: `全国城市（区分省）/山东省.csv`
- **全国城市数据**: `全国城市（区分省）/全国城市列表.csv`
- **定时任务配置**: `~/Library/LaunchAgents/com.weather.daily.plist`

## 📄 许可证

本项目采用 MIT 许可证。

## 👥 贡献

欢迎提交 Issue 和 Pull Request！

---


**支持地区**: 山东省152个地区（济南、青岛、淄博、烟台、潍坊、济宁、泰安、临沂、德州、聊城、滨州、菏泽等）
