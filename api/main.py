import ccxt
import json
import os
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional

# 配置文件路径，用于存储客户的API信息
CLIENTS_FILE = 'clients.json'
FEISHU_CONFIG_FILE = 'feishu_config.json'

class ExchangeManager:
    """
    交易所管理器类
    负责初始化交易所连接并获取账户余额
    """
    def __init__(self, client_data):
        """
        初始化 ExchangeManager
        :param client_data: 包含客户名称和交易所配置的字典
        """
        self.client_name = client_data.get('name', 'Unknown')
        self.exchanges_config = client_data.get('exchanges', {})
        self.exchanges = {} # 存储初始化后的交易所对象
        self._init_exchanges()

    def _create_client(self, exchange_id, auth_config):
        """
        创建单个交易所客户端实例
        :param exchange_id: 交易所ID (如 'binance', 'okx')
        :param auth_config: 包含 apiKey, secret, password 的字典
        :return: ccxt 交易所实例 或 None
        """
        api_key = auth_config.get('apiKey')
        secret = auth_config.get('secret')
        password = auth_config.get('password')

        # 如果缺少必要的 API Key 或 Secret，则跳过
        if not api_key or not secret:
            return None
        
        try:
            # 动态获取 ccxt 中的交易所类
            exchange_class = getattr(ccxt, exchange_id)
            
            # 基础配置
            config = {
                'apiKey': api_key,
                'secret': secret,
                'enableRateLimit': True, # 启用速率限制，防止被交易所封禁
            }
            # 如果需要密码（如 OKX），则添加
            if password:
                config['password'] = password
                
            # 针对特定交易所的额外配置
            if exchange_id == 'binance':
                # Binance 需要指定默认账户类型为现货 (spot)
                config['options'] = {'defaultType': 'spot'}
            
            # 初始化并返回交易所实例
            return exchange_class(config)
        except Exception as e:
            print(f"[{self.client_name}] 初始化 {exchange_id} 失败: {e}")
            return None

    def _init_exchanges(self):
        """
        根据配置初始化所有交易所客户端
        """
        # 遍历客户配置中的每个交易所 (例如 'binance', 'okx')
        for exchange_name, accounts in self.exchanges_config.items():
            self.exchanges[exchange_name] = {}
            
            # 遍历该交易所下的所有账户类型 (例如 'main', 'sub_1')
            for account_type, auth_config in accounts.items():
                # 创建客户端实例
                client = self._create_client(exchange_name, auth_config)
                if client:
                    # 将初始化成功的客户端存入 self.exchanges
                    self.exchanges[exchange_name][account_type] = client

    def get_balance(self, exchange_client):
        """
        获取指定交易所客户端的余额
        :param exchange_client: ccxt 交易所实例
        :return: 非零余额字典 或 错误信息
        """
        try:
            # 调用统一的 fetch_balance 接口
            balance = exchange_client.fetch_balance()
            # 过滤掉余额为 0 的资产，只保留有资产的币种
            return {k: v for k, v in balance['total'].items() if v > 0}
        except Exception as e:
            return f"获取余额错误: {str(e)}"

    def fetch_client_balances(self):
        """
        获取当前客户所有已配置交易所的余额
        :return: 包含所有余额信息的字典
        """
        client_results = {}
        print(f"\n======正在处理客户: {self.client_name} ======")
        
        if not self.exchanges:
            print("  未检测到有效的交易所配置。")
            return {}

        # 遍历已初始化的交易所
        for exchange_name, accounts in self.exchanges.items():
            client_results[exchange_name] = {}
            print(f"  --- {exchange_name.upper()} ---")
            
            # 遍历该交易所下的所有账户 (main, sub 等)
            for account_type, client in accounts.items():
                print(f"    正在获取 {account_type} 账户余额...")
                bal = self.get_balance(client)
                client_results[exchange_name][account_type] = bal
                print(f"      结果: {bal}")
                
        return client_results

class FeishuManager:
    """
    飞书表格管理器类
    负责将余额数据写入飞书多维表格
    """
    def __init__(self, config: Dict[str, Any]):
        """
        初始化飞书管理器
        :param config: 包含 app_id, app_secret, app_token, table_id 的配置字典
        """
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.app_token = config.get('app_token')
        self.table_id = config.get('table_id')
        self.base_url = 'https://open.feishu.cn/open-apis'
        self.access_token = None
        self.token_expires_at = 0
        
    def get_access_token(self) -> Optional[str]:
        """
        获取飞书访问令牌
        :return: access_token 或 None
        """
        # 如果 token 未过期，直接返回
        if self.access_token and datetime.now().timestamp() < self.token_expires_at:
            return self.access_token
            
        if not self.app_id or not self.app_secret:
            print("错误: 缺少飞书 app_id 或 app_secret")
            return None
            
        try:
            url = f"{self.base_url}/auth/v3/tenant_access_token/internal"
            payload = {
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0:
                self.access_token = data.get('tenant_access_token')
                # token 有效期通常是 2 小时，提前 5 分钟刷新
                self.token_expires_at = datetime.now().timestamp() + data.get('expire', 7200) - 300
                print("✓ 飞书访问令牌获取成功")
                return self.access_token
            else:
                print(f"获取飞书访问令牌失败: {data.get('msg')}")
                return None
        except Exception as e:
            print(f"获取飞书访问令牌异常: {e}")
            return None
    
    def format_balance_data(self, all_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        将余额数据转换为飞书表格行格式
        :param all_results: 所有客户的余额数据
        :return: 表格行数据列表
        """
        rows = []
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for client_name, exchanges in all_results.items():
            for exchange_name, accounts in exchanges.items():
                for account_type, balance_data in accounts.items():
                    # 如果余额数据是错误信息，记录错误
                    if isinstance(balance_data, str):
                        rows.append({
                            "客户名称": client_name,
                            "交易所": exchange_name.upper(),
                            "账户类型": account_type,
                            "币种": "错误",
                            "余额": balance_data,
                            "更新时间": timestamp
                        })
                        continue
                    
                    # 遍历每个币种的余额
                    for currency, amount in balance_data.items():
                        rows.append({
                            "客户名称": client_name,
                            "交易所": exchange_name.upper(),
                            "账户类型": account_type,
                            "币种": currency,
                            "余额": f"{amount:.8f}".rstrip('0').rstrip('.'),
                            "更新时间": timestamp
                        })
        
        return rows
    
    def get_table_fields(self) -> Optional[List[Dict[str, Any]]]:
        """
        获取表格字段信息
        :return: 字段列表或 None
        """
        token = self.get_access_token()
        if not token:
            return None
            
        if not self.app_token or not self.table_id:
            print("错误: 缺少飞书 app_token 或 table_id")
            return None
            
        try:
            url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('code') == 0:
                return data.get('data', {}).get('items', [])
            else:
                print(f"获取表格字段失败: {data.get('msg')}")
                return None
        except Exception as e:
            print(f"获取表格字段异常: {e}")
            return None
    
    def convert_to_feishu_format(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        将数据转换为飞书 API 要求的格式
        :param rows: 原始行数据
        :return: 飞书格式的行数据
        """
        # 获取表格字段映射
        fields = self.get_table_fields()
        field_map = {}
        
        if fields:
            # 构建字段名到字段ID和类型的映射
            for field in fields:
                field_name = field.get('field_name', '')
                field_id = field.get('field_id', '')
                field_type = field.get('type', 1)  # 1=文本, 2=数字, 15=日期时间
                if field_name and field_id:
                    field_map[field_name] = {
                        'id': field_id,
                        'type': field_type
                    }
        else:
            print("警告: 无法获取表格字段，将尝试使用字段名作为字段ID")
        
        feishu_rows = []
        for row in rows:
            fields_data = {}
            for key, value in row.items():
                if key in field_map:
                    # 使用字段ID和类型信息
                    field_info = field_map[key]
                    field_id = field_info['id']
                    field_type = field_info['type']
                else:
                    # 如果字段不存在于映射中，尝试使用字段名（某些情况下可能有效）
                    print(f"警告: 字段 '{key}' 在表格中不存在，将跳过")
                    continue
                
                # 根据字段类型设置值
                if field_type == 2:  # 数字类型
                    try:
                        # 尝试转换为数字
                        num_value = float(str(value).replace(',', ''))
                        fields_data[field_id] = num_value
                    except (ValueError, TypeError):
                        # 如果转换失败，使用原值（飞书可能会拒绝）
                        fields_data[field_id] = str(value)
                elif field_type == 15:  # 日期时间类型
                    try:
                        # 转换为时间戳（毫秒）
                        if isinstance(value, str):
                            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            fields_data[field_id] = int(dt.timestamp() * 1000)
                        else:
                            fields_data[field_id] = value
                    except (ValueError, TypeError):
                        fields_data[field_id] = str(value)
                else:  # 文本类型（默认）
                    fields_data[field_id] = str(value)
            
            if fields_data:  # 只有当有有效字段时才添加
                feishu_rows.append({"fields": fields_data})
        
        return feishu_rows
    
    def write_to_feishu(self, all_results: Dict[str, Any], clear_existing: bool = False) -> bool:
        """
        将余额数据写入飞书表格
        :param all_results: 所有客户的余额数据
        :param clear_existing: 是否清空现有数据（默认不清空，追加）
        :return: 是否成功
        """
        token = self.get_access_token()
        if not token:
            return False
            
        if not self.app_token or not self.table_id:
            print("错误: 缺少飞书 app_token 或 table_id")
            return False
        
        # 转换数据格式
        rows = self.format_balance_data(all_results)
        if not rows:
            print("警告: 没有数据需要写入")
            return False
        
        # 转换为飞书格式
        feishu_rows = self.convert_to_feishu_format(rows)
        
        # 如果需要清空现有数据，先获取所有记录并删除
        if clear_existing:
            if not self._clear_table(token):
                print("警告: 清空表格失败，将继续追加数据")
        
        # 批量写入（飞书 API 每次最多 500 条）
        batch_size = 500
        success_count = 0
        
        for i in range(0, len(feishu_rows), batch_size):
            batch = feishu_rows[i:i + batch_size]
            try:
                url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "records": batch
                }
                response = requests.post(url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0:
                    created_count = len(data.get('data', {}).get('records', []))
                    success_count += created_count
                    print(f"✓ 成功写入 {created_count} 条记录到飞书表格")
                else:
                    print(f"写入飞书表格失败: {data.get('msg')}")
                    return False
            except Exception as e:
                print(f"写入飞书表格异常: {e}")
                return False
        
        print(f"\n✓ 总共成功写入 {success_count} 条记录到飞书表格")
        return True
    
    def _clear_table(self, token: str) -> bool:
        """
        清空表格所有记录
        :param token: 访问令牌
        :return: 是否成功
        """
        try:
            # 先获取所有记录ID
            url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            record_ids = []
            page_token = None
            
            while True:
                params = {"page_size": 500}
                if page_token:
                    params["page_token"] = page_token
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') != 0:
                    return False
                
                items = data.get('data', {}).get('items', [])
                record_ids.extend([item.get('record_id') for item in items])
                
                has_more = data.get('data', {}).get('has_more', False)
                if not has_more:
                    break
                page_token = data.get('data', {}).get('page_token')
            
            # 批量删除记录
            if record_ids:
                delete_url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_delete"
                for i in range(0, len(record_ids), 500):
                    batch_ids = record_ids[i:i + 500]
                    payload = {"record_ids": batch_ids}
                    response = requests.post(delete_url, json=payload, headers=headers, timeout=30)
                    response.raise_for_status()
                    result = response.json()
                    if result.get('code') != 0:
                        return False
                
                print(f"✓ 已清空 {len(record_ids)} 条旧记录")
            
            return True
        except Exception as e:
            print(f"清空表格异常: {e}")
            return False

def load_clients():
    """
    加载 clients.json 配置文件
    :return: 客户配置列表
    """
    if not os.path.exists(CLIENTS_FILE):
        print(f"错误: 找不到配置文件 {CLIENTS_FILE}。请先创建该文件。")
        return []
    
    try:
        with open(CLIENTS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"读取 {CLIENTS_FILE} 失败: {e}")
        return []

def load_feishu_config() -> Optional[Dict[str, Any]]:
    """
    加载飞书配置文件
    :return: 飞书配置字典或 None
    """
    if not os.path.exists(FEISHU_CONFIG_FILE):
        print(f"提示: 找不到飞书配置文件 {FEISHU_CONFIG_FILE}，将跳过飞书写入功能")
        return None
    
    try:
        with open(FEISHU_CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"读取 {FEISHU_CONFIG_FILE} 失败: {e}")
        return None

def main():
    # 1. 加载客户配置
    clients_data = load_clients()
    if not clients_data:
        print("未找到客户数据或文件为空。")
        return

    all_results = {}
    
    # 2. 遍历每个客户
    for client_data in clients_data:
        # 为每个客户创建一个 ExchangeManager 实例
        manager = ExchangeManager(client_data)
        client_name = client_data.get('name')
        
        # 3. 获取该客户所有账户的余额
        all_results[client_name] = manager.fetch_client_balances()
    
    # 4. 保存结果到 JSON 文件（备份）
    try:
        with open('balances_report.json', 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print("\n✓ 余额数据已保存到 balances_report.json")
    except Exception as e:
        print(f"\n保存 JSON 文件失败: {e}")
    
    # 5. 写入飞书表格
    feishu_config = load_feishu_config()
    if feishu_config:
        print("\n" + "="*50)
        print("开始写入飞书表格...")
        print("="*50)
        feishu_manager = FeishuManager(feishu_config)
        # clear_existing=True 表示每次清空旧数据后写入新数据
        # 如果希望追加数据，可以设置为 False
        feishu_manager.write_to_feishu(all_results, clear_existing=feishu_config.get('clear_existing', True))
    else:
        print("\n提示: 未配置飞书，跳过表格写入")

if __name__ == "__main__":
    main()
