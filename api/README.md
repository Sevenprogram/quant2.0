# 多交易所余额查询与飞书表格同步工具

## 功能说明

本工具可以：
1. 批量查询多个客户在多个交易所的账户余额
2. 自动将余额数据同步到飞书多维表格
3. 支持 Binance、OKX、Gate、Bybit 等多个交易所
4. 支持每个交易所的多个账户（主账户、子账户等）

## 安装依赖

```bash
pip install ccxt requests
```

## 配置文件

### 1. clients.json - 客户和交易所配置

创建 `clients.json` 文件，配置客户信息和交易所 API 密钥：

```json
[
  {
    "name": "客户A",
    "exchanges": {
      "binance": {
        "main": {
          "apiKey": "你的Binance API Key",
          "secret": "你的Binance Secret"
        },
        "sub_1": {
          "apiKey": "子账户1的API Key",
          "secret": "子账户1的Secret"
        }
      },
      "okx": {
        "main": {
          "apiKey": "你的OKX API Key",
          "secret": "你的OKX Secret",
          "password": "你的OKX Passphrase"
        }
      }
    }
  }
]
```

**注意事项：**
- OKX 需要 `password` 字段（Passphrase）
- 其他交易所通常只需要 `apiKey` 和 `secret`
- 确保 API 密钥有查询余额的权限

### 2. feishu_config.json - 飞书配置

复制 `feishu_config.json.example` 为 `feishu_config.json`，并填入你的飞书应用信息：

```json
{
  "app_id": "你的飞书App ID",
  "app_secret": "你的飞书App Secret",
  "app_token": "你的多维表格App Token",
  "table_id": "你的表格ID",
  "clear_existing": true
}
```

**如何获取飞书配置：**

1. **创建飞书应用**
   - 访问 [飞书开放平台](https://open.feishu.cn/)
   - 创建企业自建应用
   - 获取 `app_id` 和 `app_secret`

2. **配置应用权限**
   - 在应用管理页面，添加以下权限：
     - `bitable:app` - 查看、编辑和管理多维表格
     - `bitable:app:readonly` - 查看多维表格（如果只需要读取）

3. **获取 App Token 和 Table ID**
   - 打开你的飞书多维表格
   - 在浏览器地址栏中可以看到 URL，例如：
     ```
     https://example.feishu.cn/base/AppToken123?table=TableId456
     ```
   - `AppToken123` 就是 `app_token`
   - `TableId456` 就是 `table_id`

4. **配置表格字段**
   
   在飞书表格中创建以下字段（字段名必须完全匹配）：
   - **客户名称** (文本类型)
   - **交易所** (文本类型)
   - **账户类型** (文本类型)
   - **币种** (文本类型)
   - **余额** (数字类型 或 文本类型)
   - **更新时间** (日期时间类型 或 文本类型)

   **字段顺序可以任意，但字段名必须完全匹配！**

5. **clear_existing 参数**
   - `true`: 每次运行前清空表格所有数据，然后写入新数据（推荐）
   - `false`: 追加数据到表格末尾（会累积历史数据）

## 使用方法

### 基本使用

```bash
cd quant/api
python main.py
```

### 运行流程

1. 程序会读取 `clients.json` 中的所有客户配置
2. 为每个客户初始化交易所连接
3. 查询所有账户的余额
4. 将结果保存到 `balances_report.json`
5. 如果配置了飞书，会自动写入飞书表格

### 输出示例

```
======正在处理客户: 客户A ======
  --- BINANCE ---
    正在获取 main 账户余额...
      结果: {'BTC': 1.5, 'USDT': 10000.0, 'ETH': 10.0}
    正在获取 sub_1 账户余额...
      结果: {'USDT': 5000.0}

✓ 余额数据已保存到 balances_report.json

==================================================
开始写入飞书表格...
==================================================
✓ 飞书访问令牌获取成功
✓ 成功写入 3 条记录到飞书表格

✓ 总共成功写入 3 条记录到飞书表格
```

## 数据格式

### JSON 文件格式 (balances_report.json)

```json
{
  "客户A": {
    "binance": {
      "main": {
        "BTC": 1.5,
        "USDT": 10000.0,
        "ETH": 10.0
      },
      "sub_1": {
        "USDT": 5000.0
      }
    }
  }
}
```

### 飞书表格格式

每条记录包含：
- 客户名称
- 交易所
- 账户类型
- 币种
- 余额
- 更新时间

## 支持的交易所

理论上支持所有 ccxt 库支持的交易所，包括但不限于：
- Binance
- OKX (OKEx)
- Gate.io
- Bybit
- Huobi
- Coinbase
- 等等...

## 常见问题

### 1. 无法连接交易所

- 检查 API Key 和 Secret 是否正确
- 确认 API 密钥有查询余额的权限
- 检查网络连接
- 某些交易所可能需要 IP 白名单

### 2. 飞书写入失败

- 检查 `app_id` 和 `app_secret` 是否正确
- 确认应用有相应的权限
- 检查 `app_token` 和 `table_id` 是否正确
- 确认表格字段名与代码中的字段名完全匹配

### 3. 字段映射错误

- 确保飞书表格中的字段名与代码中使用的字段名完全一致
- 字段类型要匹配（数字字段用数字类型，日期用日期时间类型）
- 如果字段名不匹配，程序会跳过该字段并输出警告

### 4. 数据被清空

- 如果设置了 `clear_existing: true`，每次运行都会清空旧数据
- 如果需要保留历史数据，设置 `clear_existing: false`

## 安全建议

1. **不要将配置文件提交到 Git**
   - 将 `clients.json` 和 `feishu_config.json` 添加到 `.gitignore`
   - 使用环境变量或加密存储敏感信息

2. **限制 API 权限**
   - 只授予查询余额的权限
   - 不要授予交易或提币权限

3. **定期更换密钥**
   - 定期更换 API 密钥
   - 使用子账户而非主账户

## 扩展功能

### 定时任务

可以使用 cron (Linux/Mac) 或任务计划程序 (Windows) 定时运行：

```bash
# 每小时运行一次
0 * * * * cd /path/to/quant/api && python main.py
```

### 邮件通知

可以在代码中添加邮件通知功能，当余额异常时发送邮件。

### 数据可视化

飞书表格支持数据透视表和图表，可以在表格中创建可视化报表。

## 许可证

MIT License
