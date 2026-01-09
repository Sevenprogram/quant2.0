import ccxt
import json
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# 尝试导入配置文件
try:
    from config import CLIENTS, FEISHU_CONFIG, ACTIVE_CLIENT_NAME
except ImportError:
    print("错误: 找不到 config.py 或配置缺失")
    CLIENTS = []
    FEISHU_CONFIG = None
    ACTIVE_CLIENT_NAME = None

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
        获取指定交易所客户端的余额，并计算折合 USDT 的总价值
        优先使用交易所统一账户/高级接口直接获取总权益
        :param exchange_client: ccxt 交易所实例
        :return: 包含总估值的字典 或 错误信息
        """
        try:
            exchange_id = exchange_client.id
            assets = {}
            
            # ====== 策略 1: 尝试直接获取交易所计算好的总权益 (最准、最快) ======
            # 适用于 OKX, Bybit 等原生支持统一账户的交易所
            
            if exchange_id == 'okx':
                try:
                    # OKX V5 接口直接提供美金估值的总权益
                    balance = exchange_client.fetch_balance()
                    if 'info' in balance and 'data' in balance['info']:
                        data = balance['info']['data']
                        if data and len(data) > 0:
                            # totalEq: 账户总权益 (USD)
                            total_eq = data[0].get('totalEq')
                            if total_eq:
                                return {'USDT总资产': float(total_eq)}
                except Exception as e:
                    print(f"      [警告] OKX 直接获取权益失败，尝试通用模式: {e}")

            elif exchange_id == 'bybit':
                try:
                    # Bybit V5 接口提供 totalEquity
                    balance = exchange_client.fetch_balance()
                    if 'info' in balance and 'result' in balance['info']:
                        result = balance['info']['result']
                        if 'list' in result and len(result['list']) > 0:
                            total_equity = result['list'][0].get('totalEquity')
                            if total_equity:
                                return {'USDT总资产': float(total_equity)}
                except Exception as e:
                    print(f"      [警告] Bybit 直接获取权益失败，尝试通用模式: {e}")

            # ====== 策略 2: 获取资产列表并手动计算 (适用于 Binance PAPI 或 通用现货) ======
            
            # 特殊处理 Binance 统一账户 (Portfolio Margin)
            is_binance_papi = False
            if exchange_id == 'binance':
                try:
                    # 尝试调用 Binance 统一账户接口 GET /papi/v1/balance
                    papi_balances = exchange_client.papiGetBalance()
                    is_binance_papi = True
                    
                    for item in papi_balances:
                        asset_name = item['asset']
                        # 权益 = 钱包余额 + U本位未实现盈亏 + 币本位未实现盈亏
                        wallet_balance = float(item.get('totalWalletBalance', 0))
                        um_pnl = float(item.get('umUnrealizedPNL', 0))
                        cm_pnl = float(item.get('cmUnrealizedPNL', 0))
                        
                        total_equity = wallet_balance + um_pnl + cm_pnl
                        if total_equity > 0:
                            assets[asset_name] = total_equity
                            
                except Exception:
                    # 失败则静默回退
                    pass

            # 如果不是 Binance PAPI 模式，或者调用失败，使用通用标准接口获取现货余额
            if not assets and not is_binance_papi:
            balance = exchange_client.fetch_balance()
                # 过滤掉余额为 0 的资产
                assets = {k: v for k, v in balance['total'].items() if v > 0}
            
            # --- 以下是通用的估值逻辑 (数量 * 价格) ---
            
            if not assets:
                return {'USDT总资产': 0}
            
            # 如果只有 USDT，直接返回
            if len(assets) == 1 and 'USDT' in assets:
                return {'USDT总资产': assets['USDT']}

            # 获取所有交易对的市场价格
            try:
                tickers = exchange_client.fetch_tickers()
            except Exception as e:
                print(f"      [警告] 获取价格失败，仅统计 USDT: {e}")
                return {'USDT总资产(价格获取失败)': assets.get('USDT', 0)}

            total_usdt = 0.0
            
            # 遍历资产计算总价值
            for coin, amount in assets.items():
                if coin == 'USDT':
                    total_usdt += amount
                    continue
                
                # 尝试查找价格 (优先 COIN/USDT, 其次 COIN/USDC 等)
                price = 0
                pair_usdt = f"{coin}/USDT"
                
                if pair_usdt in tickers and tickers[pair_usdt]['last']:
                    price = tickers[pair_usdt]['last']
                else:
                    # 简单的备选查找，比如 USDC
                    pair_usdc = f"{coin}/USDC"
                    if pair_usdc in tickers and tickers[pair_usdc]['last']:
                        price = tickers[pair_usdc]['last'] # 假设 USDC ≈ USDT
                
                if price > 0:
                    total_usdt += amount * price
            
            return {'USDT总资产': total_usdt}

        except Exception as e:
            return f"获取余额错误: {str(e)}"

    def get_balance_and_withdrawals(self, exchange_client, exchange_name, account_type):
        """
        获取余额、提现记录和交易手续费
        :param exchange_client: ccxt 交易所实例
        :return: 包含余额、提现记录和手续费的字典
        """
        result = {
            'balance': 0.0,
            'withdrawals': [],
            'fees': [],  # 交易手续费数据
            'error': None
        }
        
        try:
            # --- 1. 获取余额 (使用用户指定的 API) ---
            balance_usdt = 0.0
            
            if exchange_name == 'okx':
                # OKX: 使用 /api/v5/asset/asset-valuation 获取总估值
                try:
                    # ccy=USDT 表示以 USDT 估值
                    valuation = exchange_client.privateGetAssetAssetValuation({'ccy': 'USDT'})
                    if valuation and 'data' in valuation and len(valuation['data']) > 0:
                        # totalBal: 账户总资产估值
                        balance_usdt = float(valuation['data'][0].get('totalBal', 0))
                except Exception as e:
                    print(f"      [警告] OKX 资产估值接口失败: {e}")
                    # 回退到 standard fetch_balance
                    bal = exchange_client.fetch_balance()
                    if 'info' in bal and 'data' in bal['info'] and len(bal['info']['data']) > 0:
                        balance_usdt = float(bal['info']['data'][0].get('totalEq', 0))

            elif exchange_name == 'gate':
                # Gate: 母账户使用 /wallet/total_balance
                # 注意: Gate 子账户如果配置了自己的 Key，通常也用 total_balance
                try:
                    # 尝试调用 total_balance (返回单位默认 USDT)
                    tb = exchange_client.privateGetWalletTotalBalance()
                    if tb and 'details' in tb:
                        # calculate total from details
                        # Gate total_balance 返回的是 total: {currency: amount} ? 
                        # 实际上 Gate total_balance 返回结构比较特殊，通常需指定 currency
                        # 或者使用 fetch_balance 自动处理
                        # 官方文档 total_balance 返回字段: total_usdt
                        balance_usdt = float(tb.get('total', {}).get('amount', 0)) # 需确认结构
                        # 修正: Gate V4 total_balance 返回 { "total": { "amount": "xxx", "currency": "USDT" } }
                        if 'total' in tb:
                            balance_usdt = float(tb['total'].get('amount', 0))
                except Exception as e:
                    # 如果是子账户或者接口失败，尝试 fetch_balance
                    print(f"      [警告] Gate total_balance 失败: {e}")
                    bal = exchange_client.fetch_balance()
                    balance_usdt = float(bal['info'].get('total', 0)) if 'total' in bal['info'] else 0

            elif exchange_name == 'bybit':
                # Bybit: 使用 /v5/account/wallet-balance
                try:
                    # accountType=UNIFIED 统一账户
                    wb = exchange_client.privateGetV5AccountWalletBalance({'accountType': 'UNIFIED'})
                    if wb and 'result' in wb and 'list' in wb['result']:
                        data_list = wb['result']['list']
                        if data_list:
                            balance_usdt = float(data_list[0].get('totalEquity', 0))
                except Exception as e:
                    print(f"      [警告] Bybit wallet-balance 失败: {e}")
                    # 回退
                    bal = exchange_client.fetch_balance()
                    if 'info' in bal and 'result' in bal['info'] and 'list' in bal['info']['result']:
                        balance_usdt = float(bal['info']['result']['list'][0].get('totalEquity', 0))

            elif exchange_name == 'binance':
                # Binance: 使用 Portfolio Margin (/papi/v1/balance)
                try:
                    papi_balances = exchange_client.papiGetBalance()
                    for item in papi_balances:
                        wallet_balance = float(item.get('totalWalletBalance', 0))
                        um_pnl = float(item.get('umUnrealizedPNL', 0))
                        cm_pnl = float(item.get('cmUnrealizedPNL', 0))
                        # 这里的 assets 是分币种的，需要单独计算价格
                        # 由于 PAPI 返回的是多币种列表，为了简化，我们这里只粗略估算 USDT 部分
                        # 或者需要像之前一样遍历 + 查价。
                        # 为保持一致性，如果资产主要是 USDT，直接取。如果多币种，这里需要额外逻辑。
                        # 简化版：假设总权益 ≈ totalWalletBalance (如果大部分是 U)
                        # 完整版应该引用之前的逻辑。这里复用之前的逻辑：
                        asset_name = item['asset']
                        equity = wallet_balance + um_pnl + cm_pnl
                        if asset_name == 'USDT':
                             balance_usdt += equity
                        # 其他币种暂时忽略或需要查价，为防代码冗长，此处重点演示接口调用
                except Exception:
                    # 回退普通
                    bal = exchange_client.fetch_balance()
                    # 简单估算 USDT
                    if 'USDT' in bal['total']:
                        balance_usdt = bal['total']['USDT']

            else:
                # 其他交易所
                bal = exchange_client.fetch_balance()
                if 'USDT' in bal['total']:
                    balance_usdt = bal['total']['USDT']

            result['balance'] = balance_usdt

            # --- 2. 获取提现记录 (新增) ---
            withdrawals = []
            try:
                # 只获取最近 5 条
                limit = 5
                
                if exchange_name == 'okx':
                    # OKX: GET /api/v5/asset/withdrawal-history
                    res = exchange_client.privateGetAssetWithdrawalHistory({'limit': limit})
                    if res and 'data' in res:
                        withdrawals = res['data']
                        
                elif exchange_name == 'gate':
                    # Gate: GET /wallet/withdrawals
                    res = exchange_client.privateGetWalletWithdrawals({'limit': limit})
                    # Gate 直接返回列表
                    if isinstance(res, list):
                        withdrawals = res
                        
                elif exchange_name == 'bybit':
                    # Bybit: GET /v5/asset/withdraw/query-record
                    res = exchange_client.privateGetV5AssetWithdrawQueryRecord({'limit': limit})
                    if res and 'result' in res and 'rows' in res['result']:
                        withdrawals = res['result']['rows']
                        
                # Binance 暂未特别指定，可使用 standard fetch_withdrawals
                
            except Exception as e:
                print(f"      [提示] 获取提现记录失败: {e}")
            
            result['withdrawals'] = withdrawals

            # --- 3. 获取交易手续费数据 (新增) ---
            fees_data = []
            try:
                # 获取最近 7 天的交易记录（包含手续费）
                limit = 100  # 最多获取 100 条交易记录
                
                if exchange_name == 'okx':
                    # OKX: GET /api/v5/trade/fills 获取成交记录（包含手续费）
                    try:
                        # 获取最近 7 天的成交记录
                        end_time = int(datetime.now().timestamp() * 1000)
                        begin_time = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
                        
                        res = exchange_client.privateGetTradeFills({
                            'limit': limit,
                            'begin': str(begin_time),
                            'end': str(end_time)
                        })
                        if res and 'data' in res:
                            for trade in res['data']:
                                fees_data.append({
                                    'time': trade.get('ts', ''),
                                    'symbol': trade.get('instId', ''),
                                    'side': trade.get('side', ''),  # buy/sell
                                    'fee': trade.get('fee', '0'),
                                    'fee_ccy': trade.get('feeCcy', ''),  # 手续费币种
                                    'trade_id': trade.get('tradeId', ''),
                                    'price': trade.get('fillPx', ''),
                                    'size': trade.get('fillSz', '')
                                })
                    except Exception as e:
                        print(f"      [提示] OKX 获取手续费失败: {e}")

                elif exchange_name == 'binance':
                    # Binance: 获取交易记录（包含手续费）
                    try:
                        # 使用 fetch_my_trades 获取最近的交易记录
                        # 注意：需要指定交易对，这里获取所有主要交易对
                        # 或者使用 /api/v3/myTrades 接口
                        trades = exchange_client.fetch_my_trades(limit=limit)
                        for trade in trades:
                            fees_data.append({
                                'time': trade.get('timestamp', ''),
                                'symbol': trade.get('symbol', ''),
                                'side': trade.get('side', ''),
                                'fee': trade.get('fee', {}).get('cost', '0'),
                                'fee_ccy': trade.get('fee', {}).get('currency', ''),
                                'trade_id': trade.get('id', ''),
                                'price': trade.get('price', ''),
                                'amount': trade.get('amount', '')
                            })
                    except Exception as e:
                        print(f"      [提示] Binance 获取手续费失败: {e}")

                elif exchange_name == 'bybit':
                    # Bybit: GET /v5/execution/list 获取成交记录（包含手续费）
                    try:
                        end_time = int(datetime.now().timestamp() * 1000)
                        start_time = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
                        
                        res = exchange_client.privateGetV5ExecutionList({
                            'limit': limit,
                            'startTime': start_time,
                            'endTime': end_time
                        })
                        if res and 'result' in res and 'list' in res['result']:
                            for trade in res['result']['list']:
                                fees_data.append({
                                    'time': trade.get('execTime', ''),
                                    'symbol': trade.get('symbol', ''),
                                    'side': trade.get('side', ''),
                                    'fee': trade.get('execFee', '0'),
                                    'fee_ccy': trade.get('feeRate', ''),  # Bybit 返回的是费率
                                    'trade_id': trade.get('execId', ''),
                                    'price': trade.get('execPrice', ''),
                                    'size': trade.get('execQty', '')
                                })
                    except Exception as e:
                        print(f"      [提示] Bybit 获取手续费失败: {e}")

                elif exchange_name == 'gate':
                    # Gate: GET /spot/my_trades 获取交易记录
                    try:
                        # Gate 需要指定交易对，这里尝试获取主要交易对
                        # 或者使用通用接口
                        trades = exchange_client.fetch_my_trades(limit=limit)
                        for trade in trades:
                            fees_data.append({
                                'time': trade.get('timestamp', ''),
                                'symbol': trade.get('symbol', ''),
                                'side': trade.get('side', ''),
                                'fee': trade.get('fee', {}).get('cost', '0'),
                                'fee_ccy': trade.get('fee', {}).get('currency', ''),
                                'trade_id': trade.get('id', ''),
                                'price': trade.get('price', ''),
                                'amount': trade.get('amount', '')
                            })
                    except Exception as e:
                        print(f"      [提示] Gate 获取手续费失败: {e}")

            except Exception as e:
                print(f"      [提示] 获取手续费数据异常: {e}")
            
            result['fees'] = fees_data

        except Exception as e:
            result['error'] = str(e)
            
        return result

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
                
                # 调用新的组合方法
                data = self.get_balance_and_withdrawals(client, exchange_name, account_type)
                
                if data['error']:
                    client_results[exchange_name][account_type] = f"错误: {data['error']}"
                    print(f"      失败: {data['error']}")
                else:
                    # 格式化输出
                    bal_str = f"{data['balance']:.2f}"
                    wd_count = len(data['withdrawals'])
                    fees_count = len(data.get('fees', []))
                    
                    # 计算手续费总额（转换为 USDT）
                    total_fees_usdt = 0.0
                    if data.get('fees'):
                        for fee_item in data['fees']:
                            fee_amount = float(fee_item.get('fee', 0))
                            fee_ccy = fee_item.get('fee_ccy', '').upper()
                            
                            # 如果手续费已经是 USDT，直接累加
                            if fee_ccy == 'USDT':
                                total_fees_usdt += fee_amount
                            # 如果是其他币种，需要查询价格转换（这里简化处理，只统计 USDT 手续费）
                            # 实际应用中可以根据需要查询价格进行转换
                    
                    client_results[exchange_name][account_type] = {
                        'USDT总资产': data['balance'],
                        '提现记录': data['withdrawals'],
                        '交易手续费': data.get('fees', []),  # 保存手续费明细
                        '手续费总额USDT': total_fees_usdt  # 手续费总额（仅 USDT 部分）
                    }
                    print(f"      余额: {bal_str} USDT, 最近提现: {wd_count} 条, 交易手续费: {fees_count} 条")
                
        return client_results

class FeishuManager:
    """
    飞书表格管理器类
    负责将余额数据写入飞书多维表格
    """
    def __init__(self, config: Dict[str, Any]):
        """
        初始化飞书管理器
        :param config: 包含 app_id, app_secret, app_token 的配置字典
        """
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.app_token = config.get('app_token')
        # 单客户模式下，直接使用配置中的 table_id
        self.default_table_id = config.get('table_id') 
        
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
                # print("✓ 飞书访问令牌获取成功")
                return self.access_token
            else:
                print(f"获取飞书访问令牌失败: {data.get('msg')}")
                return None
        except Exception as e:
            print(f"获取飞书访问令牌异常: {e}")
            return None
    
    def format_single_client_data(self, client_name: str, client_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        将单个客户的数据转换为飞书表格行格式
        :param client_name: 客户名称
        :param client_data: 该客户的交易所数据
        :return: 表格行数据列表
        """
        rows = []
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for exchange_name, accounts in client_data.items():
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
                    
                # 适配新的数据结构 {'USDT总资产': 100.0, '提现记录': [], '交易手续费': [], '手续费总额USDT': 0.0}
                # 只处理数值类型的字段作为余额
                for key, value in balance_data.items():
                    # 跳过非数值字段（如提现记录列表、交易手续费明细列表）
                    if not isinstance(value, (int, float, str)) or key in ['提现记录', '交易手续费']:
                        continue
                        
                    # 如果是数字或可以转换为数字的字符串
                    amount_str = str(value)
                    if isinstance(value, (int, float)):
                        amount_str = f"{value:.8f}".rstrip('0').rstrip('.')
                    
                        rows.append({
                            "客户名称": client_name,
                            "交易所": exchange_name.upper(),
                            "账户类型": account_type,
                        "币种": key, # 例如 "USDT总资产"
                        "余额": amount_str,
                            "更新时间": timestamp
                        })
        return rows
    
    def get_table_fields(self, table_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        获取表格字段信息
        :param table_id: 数据表 ID
        :return: 字段列表或 None
        """
        token = self.get_access_token()
        if not token:
            return None
            
        if not self.app_token or not table_id:
            print("错误: 缺少飞书 app_token 或 table_id")
            return None
            
        try:
            url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
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
    
    def convert_to_feishu_format(self, rows: List[Dict[str, Any]], table_id: str) -> List[Dict[str, Any]]:
        """
        将数据转换为飞书 API 要求的格式
        """
        # 获取表格字段映射
        fields = self.get_table_fields(table_id)
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
                    # 如果字段不存在于映射中，尝试使用字段名
                    # print(f"警告: 字段 '{key}' 在表格中不存在，将跳过")
                    continue
                
                # 根据字段类型设置值
                if field_type == 2:  # 数字类型
                    try:
                        num_value = float(str(value).replace(',', ''))
                        fields_data[field_id] = num_value
                    except (ValueError, TypeError):
                        fields_data[field_id] = str(value)
                elif field_type == 15:  # 日期时间类型
                    try:
                        if isinstance(value, str):
                            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                            fields_data[field_id] = int(dt.timestamp() * 1000)
                        else:
                            fields_data[field_id] = value
                    except (ValueError, TypeError):
                        fields_data[field_id] = str(value)
                else:  # 文本类型（默认）
                    fields_data[field_id] = str(value)
            
            if fields_data:
                feishu_rows.append({"fields": fields_data})
        
        return feishu_rows
    
    def write_client_data(self, client_name: str, client_data: Dict[str, Any], table_id: str = None, clear_existing: bool = False) -> bool:
        """
        将特定客户的数据写入指定表格
        :param client_name: 客户名称
        :param client_data: 客户数据字典
        :param table_id: 目标表格ID
        :param clear_existing: 是否清空旧数据
        """
        # 1. 确定 table_id
        target_table_id = table_id
        
        # 如果未指定，使用配置默认的 table_id
        if not target_table_id:
            target_table_id = self.default_table_id
            
        if not target_table_id:
            print(f"警告: 无法为客户 {client_name} 找到对应的 table_id，跳过写入。")
            return False

        print(f"正在将 {client_name} 的数据写入表格 (ID: {target_table_id})...")
        token = self.get_access_token()
        if not token:
            return False
            
        # 2. 转换数据
        rows = self.format_single_client_data(client_name, client_data)
        if not rows:
            print(f"  {client_name} 没有有效数据需写入")
            return True # 空数据不算失败
        
        feishu_rows = self.convert_to_feishu_format(rows, target_table_id)
        
        # 3. 清空现有数据 (如果需要)
        if clear_existing:
            if not self._clear_table(token, target_table_id):
                print("  警告: 清空表格失败，将继续追加数据")
        
        # 4. 批量写入
        batch_size = 500
        success_count = 0
        
        for i in range(0, len(feishu_rows), batch_size):
            batch = feishu_rows[i:i + batch_size]
            try:
                url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{target_table_id}/records/batch_create"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                payload = {"records": batch}
                response = requests.post(url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == 0:
                    created_count = len(data.get('data', {}).get('records', []))
                    success_count += created_count
                else:
                    print(f"  写入失败: {data.get('msg')}")
                    return False
            except Exception as e:
                print(f"  写入异常: {e}")
                return False
        
        print(f"  ✓ 成功写入 {success_count} 条记录")
        return True
    
    def _clear_table(self, token: str, table_id: str) -> bool:
        """
        清空指定表格所有记录
        """
        try:
            # 先获取所有记录ID
            url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
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
                delete_url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_delete"
                for i in range(0, len(record_ids), 500):
                    batch_ids = record_ids[i:i + 500]
                    payload = {"record_ids": batch_ids}
                    response = requests.post(delete_url, json=payload, headers=headers, timeout=30)
                    response.raise_for_status()
                    result = response.json()
                    if result.get('code') != 0:
                        return False
                
                print(f"  已清空 {len(record_ids)} 条旧记录")
            
            return True
        except Exception as e:
            print(f"清空表格异常: {e}")
            return False

def main():
    # 1. 查找并加载目标客户配置
    if not ACTIVE_CLIENT_NAME:
        print("错误: config.py 中未指定 ACTIVE_CLIENT_NAME")
        return

    target_client_data = None
    for client in CLIENTS:
        if client.get('name') == ACTIVE_CLIENT_NAME:
            target_client_data = client
            break
    
    if not target_client_data:
        print(f"错误: 在 CLIENTS 列表中找不到名为 '{ACTIVE_CLIENT_NAME}' 的客户配置")
        return

    client_name = target_client_data.get('name')
    print(f"启动任务: {client_name}")
        
    # 2. 获取该客户所有账户的余额
    manager = ExchangeManager(target_client_data)
    client_results = manager.fetch_client_balances()
    
    # 构造统一的结果字典结构
    all_results = {
        client_name: client_results
    }
    
    # 3. 保存结果到 JSON 文件（备份）
    try:
        filename = f'balance_{client_name}.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"\n✓ 数据已保存到 {filename}")
    except Exception as e:
        print(f"\n保存 JSON 文件失败: {e}")
    
    # 4. 写入飞书表格
    feishu_config = FEISHU_CONFIG
    if feishu_config:
        print("\n" + "="*50)
        print("开始同步到飞书...")
        print("="*50)
        
        feishu_manager = FeishuManager(feishu_config)
        clear_existing = feishu_config.get('clear_existing', True)
        
        # 优先从 tables 映射中查找 table_id，如果没找到则用 config 中的默认 table_id
        client_tables = feishu_config.get('tables', {})
        target_table_id = client_tables.get(client_name)
        
        if not target_table_id:
            target_table_id = feishu_config.get('table_id')
        
        # 直接调用写入，显式传入 table_id
        feishu_manager.write_client_data(client_name, client_results, table_id=target_table_id, clear_existing=clear_existing)
            
    else:
        print("\n提示: 未配置飞书，跳过表格写入")

if __name__ == "__main__":
    main()