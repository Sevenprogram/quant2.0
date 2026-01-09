# ==========================================
# 当前程序使用的客户名称
# ==========================================
# 在不同服务器部署时，修改此字段来指定当前运行哪个客户的任务
ACTIVE_CLIENT_NAME = "Customer_A"


# ==========================================
# 所有客户的 API 配置列表
# ==========================================
CLIENTS = [
    {
        "name": "Customer_A",
        "exchanges": {
            # === Binance 交易所配置 ===
            "binance": {
                # 母账户
                "main": { 
                    "apiKey": "BINANCE_MAIN_API_KEY", 
                    "secret": "BINANCE_MAIN_SECRET" 
                },
                # 子账户 1
                "sub_1": { 
                    "apiKey": "BINANCE_SUB1_API_KEY", 
                    "secret": "BINANCE_SUB1_SECRET" 
                },
                # 子账户 2 (可根据实际情况添加更多)
                "sub_2": { 
                    "apiKey": "BINANCE_SUB2_API_KEY", 
                    "secret": "BINANCE_SUB2_SECRET" 
                }
            },
            
            # === OKX 交易所配置 ===
            "okx": {
                # 母账户 (OKX 需要 password/passphrase)
                "main": { 
                    "apiKey": "OKX_MAIN_API_KEY", 
                    "secret": "OKX_MAIN_SECRET", 
                    "password": "OKX_MAIN_PASSWORD" 
                },
                # 子账户 1
                "sub_1": { 
                    "apiKey": "OKX_SUB1_API_KEY", 
                    "secret": "OKX_SUB1_SECRET", 
                    "password": "OKX_SUB1_PASSWORD" 
                },
                # 子账户 2
                "sub_2": { 
                    "apiKey": "OKX_SUB2_API_KEY", 
                    "secret": "OKX_SUB2_SECRET", 
                    "password": "OKX_SUB2_PASSWORD" 
                }
            },
            
            # === Gate 交易所配置 ===
            "gate": {
                # 母账户
                "main": { 
                    "apiKey": "GATE_MAIN_API_KEY", 
                    "secret": "GATE_MAIN_SECRET" 
                },
                # 子账户 1
                "sub_1": { 
                    "apiKey": "GATE_SUB1_API_KEY", 
                    "secret": "GATE_SUB1_SECRET" 
                },
                # 子账户 2
                "sub_2": { 
                    "apiKey": "GATE_SUB2_API_KEY", 
                    "secret": "GATE_SUB2_SECRET" 
                }
            },
            
            # === Bybit 交易所配置 ===
            "bybit": {
                # 母账户
                "main": { 
                    "apiKey": "BYBIT_MAIN_API_KEY", 
                    "secret": "BYBIT_MAIN_SECRET" 
                },
                # 子账户 1
                "sub_1": { 
                    "apiKey": "BYBIT_SUB1_API_KEY", 
                    "secret": "BYBIT_SUB1_SECRET" 
                },
                # 子账户 2
                "sub_2": { 
                    "apiKey": "BYBIT_SUB2_API_KEY", 
                    "secret": "BYBIT_SUB2_SECRET" 
                }
            }
        }
    },
    {
        "name": "Customer_B",
        "exchanges": {
            "binance": {
                "main": { "apiKey": "B_BINANCE_MAIN_KEY", "secret": "B_BINANCE_MAIN_SECRET" },
                "sub_1": { "apiKey": "B_BINANCE_SUB1_KEY", "secret": "B_BINANCE_SUB1_SECRET" }
            },
            "okx": {
                "main": { "apiKey": "B_OKX_MAIN_KEY", "secret": "B_OKX_MAIN_SECRET", "password": "pass" },
                "sub_1": { "apiKey": "B_OKX_SUB1_KEY", "secret": "B_OKX_SUB1_SECRET", "password": "pass" }
            }
        }
    }
]


# ==========================================
# 飞书多维表格配置
# ==========================================
FEISHU_CONFIG = {
    "app_id": "cli_a873b250ab79100e", 
    "app_secret": "XbchQHDZ7E9BX3szc5PmJe21eNiRrkl7",
    "app_token": "Rr6AbkgkOaqThdsXjTXc7ephnEc",
    
    # 【可选】默认数据表 ID
    "table_id": "tblUjhaTb6sqgPgl",
    
    # 客户分流配置: 客户名 -> 表格ID
    # 程序会根据 ACTIVE_CLIENT_NAME 自动从这里查找对应的 table_id
    "tables": {
        "Customer_A": "tblUjhaTb6sqgPgl",
        "Customer_B": "tbl_for_B"
    },
    
    # 写入模式: True = 每次清空表格; False = 追加
    "clear_existing": True
}
