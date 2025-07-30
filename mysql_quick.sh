#!/bin/bash
# MySQL快速查询脚本 - 免密码版本

# 设置MySQL密码（请替换为你的实际密码）
MYSQL_PASSWORD="200126kobe"

# 数据库名称
DATABASE="weather_db"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🌤️  MySQL快速查询工具${NC}"
echo "=================================="

# 检查参数
if [ $# -eq 0 ]; then
    echo -e "${YELLOW}用法: $0 [选项]${NC}"
    echo "选项:"
    echo "  stats     - 显示数据库统计信息"
    echo "  daily     - 显示每日汇总数据"
    echo "  hourly    - 显示最近小时数据"
    echo "  tables    - 显示表结构"
    echo "  custom    - 执行自定义SQL查询"
    echo ""
    echo "示例:"
    echo "  $0 stats"
    echo "  $0 daily"
    echo "  $0 custom 'SELECT COUNT(*) FROM weather_hourly'"
    exit 1
fi

# 执行查询
case $1 in
    "stats")
        echo -e "${GREEN}📊 数据库统计信息:${NC}"
        mysql -u root -p$MYSQL_PASSWORD -e "
        USE $DATABASE;
        SELECT '小时数据' as 类型, COUNT(*) as 数量 FROM weather_hourly
        UNION ALL
        SELECT '每日数据' as 类型, COUNT(*) as 数量 FROM weather_daily;
        SELECT '时间范围' as 信息, 
               MIN(datetime) as 最早时间, 
               MAX(datetime) as 最晚时间 
        FROM weather_hourly;
        "
        ;;
    "daily")
        echo -e "${GREEN}📋 每日天气汇总:${NC}"
        mysql -u root -p$MYSQL_PASSWORD -e "
        USE $DATABASE;
        SELECT date as 日期, 
               CONCAT(min_temp, '°C~', max_temp, '°C') as 温度范围,
               CONCAT(avg_temp, '°C') as 平均温度,
               CONCAT(total_precip, 'mm') as 降水量,
               CONCAT(avg_wind_speed, 'km/h') as 平均风速,
               CONCAT(avg_humidity, '%') as 平均湿度
        FROM weather_daily 
        ORDER BY date DESC 
        LIMIT 7;
        "
        ;;
    "hourly")
        echo -e "${GREEN}⏰ 最近小时数据:${NC}"
        mysql -u root -p$MYSQL_PASSWORD -e "
        USE $DATABASE;
        SELECT datetime as 时间,
               CONCAT(temp, '°C') as 温度,
               CONCAT(humidity, '%') as 湿度,
               CONCAT(precip, 'mm') as 降水量,
               CONCAT(wind_speed, 'km/h') as 风速
        FROM weather_hourly 
        ORDER BY datetime DESC 
        LIMIT 10;
        "
        ;;
    "tables")
        echo -e "${GREEN}🗄️ 数据库表结构:${NC}"
        mysql -u root -p$MYSQL_PASSWORD -e "
        USE $DATABASE;
        SHOW TABLES;
        DESCRIBE weather_hourly;
        DESCRIBE weather_daily;
        "
        ;;
    "custom")
        if [ -z "$2" ]; then
            echo -e "${RED}❌ 请提供SQL查询语句${NC}"
            echo "示例: $0 custom 'SELECT COUNT(*) FROM weather_hourly'"
            exit 1
        fi
        echo -e "${GREEN}🔍 执行自定义查询:${NC}"
        echo -e "${YELLOW}$2${NC}"
        mysql -u root -p$MYSQL_PASSWORD -e "
        USE $DATABASE;
        $2;
        "
        ;;
    *)
        echo -e "${RED}❌ 未知选项: $1${NC}"
        echo "使用 '$0' 查看帮助信息"
        exit 1
        ;;
esac

echo -e "${GREEN}✅ 查询完成！${NC}" 