# 🔐 MySQL免密码配置指南

## 📋 配置方法总览

为了避免每次查询MySQL数据库时都要输入密码，我们提供了以下几种解决方案：

### ✅ **推荐方案：Python脚本（已配置完成）**

**优点**：
- 无需额外配置
- 使用现有的数据库连接配置
- 功能完整，无警告信息

**使用方法**：
```bash
python 数据库查询工具_SQLAlchemy版.py
```

### 🔧 **方案1：MySQL配置文件**

#### 步骤1：创建配置文件
```bash
# 创建配置文件
nano ~/.my.cnf
```

#### 步骤2：添加配置内容
```ini
[client]
host=localhost
user=root
password=你的MySQL密码

[mysql]
host=localhost
user=root
password=你的MySQL密码
```

#### 步骤3：设置文件权限
```bash
chmod 600 ~/.my.cnf
```

#### 使用方法：
```bash
# 直接使用mysql命令，无需输入密码
mysql -e "USE weather_db; SELECT COUNT(*) FROM weather_hourly;"
```

### 🌍 **方案2：环境变量**

#### 步骤1：设置环境变量
```bash
# 临时设置（当前会话有效）
export MYSQL_PWD=你的MySQL密码

# 永久设置（添加到shell配置文件）
echo 'export MYSQL_PWD=你的MySQL密码' >> ~/.zshrc
source ~/.zshrc
```

#### 使用方法：
```bash
mysql -u root -e "USE weather_db; SELECT COUNT(*) FROM hourly_weather;"
```

### 🛠️ **方案3：Shell脚本（已创建）**

#### 步骤1：配置脚本
编辑 `mysql_quick.sh` 文件，将 `你的MySQL密码` 替换为实际密码。

#### 使用方法：
```bash
# 查看帮助
./mysql_quick.sh

# 查看统计信息
./mysql_quick.sh stats

# 查看每日汇总
./mysql_quick.sh daily

# 查看最近小时数据
./mysql_quick.sh hourly

# 查看表结构
./mysql_quick.sh tables

# 执行自定义查询
./mysql_quick.sh custom "SELECT COUNT(*) FROM weather_hourly"
```

## 🎯 **推荐使用方式**

### 日常查询
```bash
# 使用Python脚本（推荐）
python 数据库查询工具_SQLAlchemy版.py

# 或使用Shell脚本
./mysql_quick.sh stats
```

### 快速查询
```bash
# 查看数据库统计
./mysql_quick.sh stats

# 查看最近天气
./mysql_quick.sh daily
```

### 自定义查询
```bash
# 使用Shell脚本
./mysql_quick.sh custom "SELECT * FROM weather_hourly WHERE temp > 25 LIMIT 5"

# 或使用Python脚本（在代码中修改查询）
```

## 🔒 **安全注意事项**

### 1. 文件权限
```bash
# 确保配置文件权限正确
chmod 600 ~/.my.cnf
chmod 700 mysql_quick.sh
```

### 2. 密码安全
- 不要在公共场合显示密码
- 定期更换MySQL密码
- 使用强密码

### 3. 访问控制
- 限制数据库访问权限
- 只给必要的用户授权
- 定期审查访问日志

## 🚀 **快速开始**

### 立即可用的方法
1. **Python脚本**：`python 数据库查询工具_SQLAlchemy版.py`
2. **Shell脚本**：`./mysql_quick.sh stats`

### 需要配置的方法
1. **配置文件**：编辑 `~/.my.cnf`
2. **环境变量**：设置 `MYSQL_PWD`

## 📊 **测试连接**

### 测试Python连接
```bash
python 数据库查询工具_SQLAlchemy版.py
```

### 测试Shell脚本
```bash
./mysql_quick.sh stats
```

### 测试MySQL命令
```bash
mysql -e "USE weather_db; SELECT COUNT(*) FROM hourly_weather;"
```

## 🔧 **故障排除**

### 常见问题

1. **连接被拒绝**
   - 检查MySQL服务是否运行
   - 确认用户名和密码正确
   - 检查主机地址

2. **权限不足**
   - 确认用户有数据库访问权限
   - 检查数据库是否存在

3. **配置文件不生效**
   - 检查文件路径是否正确
   - 确认文件权限设置
   - 重启终端或重新加载配置

### 调试命令
```bash
# 检查MySQL服务状态
brew services list | grep mysql

# 检查配置文件
cat ~/.my.cnf

# 检查环境变量
echo $MYSQL_PWD

# 测试连接
mysql -u root -p -e "SELECT 1;"
```

## 📝 **总结**

**推荐使用顺序**：
1. **Python脚本** - 功能最完整，无需额外配置
2. **Shell脚本** - 快速查询，需要配置密码
3. **配置文件** - 系统级配置，适合长期使用
4. **环境变量** - 临时使用，灵活性高

选择最适合你的方案，享受免密码查询的便利！🎉 