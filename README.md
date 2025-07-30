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
气象预测货物销售状况/
├── 核心脚本/
│   ├── 每日自动执行.py              # 每日自动数据收集脚本
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
└── 数据文件/
    └── 山东济南气象数据/             # 原始数据文件
```

## 🔧 主要功能

### 数据收集
- ✅ 济南市及12个区县的天气数据收集
- ✅ 每日自动数据收集（11:00 AM）
- ✅ JWT Token自动刷新
- ✅ 错误重试机制
- ✅ 数据完整性检查

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

## 📄 许可证

本项目采用 MIT 许可证。

## 👥 贡献

欢迎提交 Issue 和 Pull Request！

---

**项目状态**: ✅ 生产就绪  
**最后更新**: 2024年12月  
**维护者**: 天气数据系统团队  
**支持地区**: 济南市及12个区县（历下、历城、商河、天桥、市中、平阴、槐荫、济阳、章丘、莱芜、钢城、长清）
