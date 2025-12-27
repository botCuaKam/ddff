# trading_bot_lib_part1.py
# PHẦN 1: CÁC HÀM CƠ SỞ - CẬP NHẬT THÊM PHÂN TÍCH THỰC TẾ
import json
import hmac
import hashlib
import time
import threading
import urllib.request
import urllib.parse
import numpy as np
import websocket
import logging
import requests
import os
import math
import traceback
import random
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import ssl

# ========== CẤU HÌNH & HẰNG SỐ ==========
_BINANCE_LAST_REQUEST_TIME = 0
_BINANCE_RATE_LOCK = threading.Lock()
_BINANCE_MIN_INTERVAL = 0.1

_USDT_CACHE = {"cặp": [], "cập_nhật_cuối": 0}
_USDT_CACHE_TTL = 30

_LEVERAGE_CACHE = {"dữ_liệu": {}, "cập_nhật_cuối": 0}
_LEVERAGE_CACHE_TTL = 3600

_SYMBOL_BLACKLIST = {'BTCUSDT', 'ETHUSDT'}

# ========== HÀM TIỆN ÍCH ==========
def setup_logging():
    """Thiết lập hệ thống logging cho toàn bộ ứng dụng"""
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('bot_errors.log')]
    )
    return logging.getLogger()

logger = setup_logging()

def escape_html(text):
    """Escape ký tự HTML để tránh lỗi hiển thị trên Telegram"""
    if not text: return text
    return (text.replace('&', '&amp;').replace('<', '&lt;')
                .replace('>', '&gt;').replace('"', '&quot;'))

def send_telegram(message, chat_id=None, reply_markup=None, bot_token=None, default_chat_id=None):
    """Gửi tin nhắn qua Telegram Bot API"""
    if not bot_token or not (chat_id or default_chat_id):
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    safe_message = escape_html(message)
    
    payload = {"chat_id": chat_id or default_chat_id, "text": safe_message, "parse_mode": "HTML"}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code != 200:
            logger.error(f"Lỗi Telegram ({response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {str(e)}")

# ========== HÀM TẠO BÀN PHÍM TELEGRAM ==========
def create_main_menu():
    """Tạo bàn phím menu chính cho Telegram"""
    return {
        "keyboard": [
            [{"text": "📊 Danh sách Bot"}, {"text": "📊 Thống kê"}],
            [{"text": "➕ Thêm Bot"}, {"text": "⛔ Dừng Bot"}],
            [{"text": "⛔ Quản lý Coin"}, {"text": "📈 Vị thế"}],
            [{"text": "💰 Số dư"}, {"text": "⚙️ Cấu hình"}],
            [{"text": "🎯 Chiến lược"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

def create_cancel_keyboard():
    """Tạo bàn phím hủy bỏ"""
    return {"keyboard": [[{"text": "❌ Hủy bỏ"}]], "resize_keyboard": True, "one_time_keyboard": True}

def create_bot_count_keyboard():
    """Tạo bàn phím chọn số lượng bot"""
    return {
        "keyboard": [[{"text": "1"}, {"text": "3"}, {"text": "5"}], [{"text": "10"}, {"text": "20"}], [{"text": "❌ Hủy bỏ"}]],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_bot_mode_keyboard():
    """Tạo bàn phím chọn chế độ bot"""
    return {
        "keyboard": [
            [{"text": "🤖 Bot Tĩnh - Coin cụ thể"}, {"text": "🔄 Bot Động - Tự tìm coin"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_symbols_keyboard():
    """Tạo bàn phím chọn coin"""
    try:
        symbols = get_all_USDT_pairs(limit=12) or ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]
    except:
        symbols = ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]
    
    keyboard = []
    row = []
    for symbol in symbols:
        row.append({"text": symbol})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row: keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

def create_leverage_keyboard():
    """Tạo bàn phím chọn đòn bẩy"""
    leverages = ["3", "5", "10", "15", "20", "25", "50", "75", "100"]
    keyboard = []
    row = []
    for lev in leverages:
        row.append({"text": f"{lev}x"})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row: keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

def create_percent_keyboard():
    """Tạo bàn phím chọn phần trăm vốn"""
    return {
        "keyboard": [
            [{"text": "1"}, {"text": "3"}, {"text": "5"}, {"text": "10"}],
            [{"text": "15"}, {"text": "20"}, {"text": "25"}, {"text": "50"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_tp_keyboard():
    """Tạo bàn phím chọn Take Profit"""
    return {
        "keyboard": [
            [{"text": "50"}, {"text": "100"}, {"text": "200"}],
            [{"text": "300"}, {"text": "500"}, {"text": "1000"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_sl_keyboard():
    """Tạo bàn phím chọn Stop Loss"""
    return {
        "keyboard": [
            [{"text": "0"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "500"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_roi_trigger_keyboard():
    """Tạo bàn phím chọn ngưỡng ROI"""
    return {
        "keyboard": [
            [{"text": "30"}, {"text": "50"}, {"text": "100"}],
            [{"text": "150"}, {"text": "200"}, {"text": "300"}],
            [{"text": "❌ Tắt tính năng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_pyramiding_n_keyboard():
    """Tạo bàn phím chọn số lần nhồi lệnh"""
    return {
        "keyboard": [
            [{"text": "0"}, {"text": "1"}, {"text": "2"}, {"text": "3"}],
            [{"text": "4"}, {"text": "5"}, {"text": "❌ Tắt tính năng"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

def create_pyramiding_x_keyboard():
    """Tạo bàn phím chọn mốc ROI nhồi lệnh"""
    return {
        "keyboard": [
            [{"text": "100"}, {"text": "200"}, {"text": "300"}],
            [{"text": "400"}, {"text": "500"}, {"text": "1000"}],
            [{"text": "❌ Hủy bỏ"}]
        ],
        "resize_keyboard": True, "one_time_keyboard": True
    }

# ========== HÀM API BINANCE MỚI ==========
def get_24hr_ticker(symbol=None):
    """Lấy thông tin 24h của symbol (hoặc tất cả nếu không chỉ định)"""
    try:
        _wait_for_rate_limit()
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        if symbol:
            url = f"{url}?symbol={symbol.upper()}"
        
        data = binance_api_request(url)
        return data
    except Exception as e:
        logger.error(f"Lỗi lấy 24hr ticker: {str(e)}")
        return None

def get_top_volume_symbols(limit=20, min_volume_usd=100000):
    """Lấy top coin theo khối lượng giao dịch thực tế"""
    try:
        all_tickers = get_24hr_ticker()
        if not all_tickers:
            return []
        
        volume_data = []
        for ticker in all_tickers:
            symbol = ticker.get('symbol', '')
            if not symbol.endswith('USDT'):
                continue
            if symbol in _SYMBOL_BLACKLIST:
                continue
            
            volume = float(ticker.get('volume', 0))
            quote_volume = float(ticker.get('quoteVolume', 0))
            
            # Chỉ lấy coin có volume đô la đủ lớn
            if quote_volume >= min_volume_usd:
                volume_data.append({
                    'symbol': symbol,
                    'volume': volume,
                    'quote_volume': quote_volume,
                    'price_change_percent': float(ticker.get('priceChangePercent', 0))
                })
        
        # Sắp xếp theo quote_volume (USD) giảm dần
        volume_data.sort(key=lambda x: x['quote_volume'], reverse=True)
        
        # Lấy top limit
        top_symbols = [item['symbol'] for item in volume_data[:limit]]
        
        logger.info(f"✅ Đã lấy {len(top_symbols)} coin volume cao nhất")
        return top_symbols
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy top volume: {str(e)}")
        return []

def get_high_volatility_symbols(limit=20, min_volatility_percent=5):
    """Lấy top coin theo biến động giá thực tế"""
    try:
        all_tickers = get_24hr_ticker()
        if not all_tickers:
            return []
        
        volatility_data = []
        for ticker in all_tickers:
            symbol = ticker.get('symbol', '')
            if not symbol.endswith('USDT'):
                continue
            if symbol in _SYMBOL_BLACKLIST:
                continue
            
            high = float(ticker.get('highPrice', 0))
            low = float(ticker.get('lowPrice', 0))
            if low <= 0:
                continue
            
            # Tính biến động phần trăm
            volatility = ((high - low) / low) * 100
            
            if volatility >= min_volatility_percent:
                volatility_data.append({
                    'symbol': symbol,
                    'volatility': volatility,
                    'high': high,
                    'low': low,
                    'price_change_percent': float(ticker.get('priceChangePercent', 0))
                })
        
        # Sắp xếp theo biến động giảm dần
        volatility_data.sort(key=lambda x: x['volatility'], reverse=True)
        
        # Lấy top limit
        top_symbols = [item['symbol'] for item in volatility_data[:limit]]
        
        logger.info(f"✅ Đã lấy {len(top_symbols)} coin biến động cao nhất")
        return top_symbols
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy top biến động: {str(e)}")
        return []

def get_symbol_metrics(symbol):
    """Lấy các chỉ số chi tiết của một symbol"""
    try:
        ticker = get_24hr_ticker(symbol)
        if not ticker or isinstance(ticker, list):
            return None
        
        return {
            'symbol': symbol,
            'price': float(ticker.get('lastPrice', 0)),
            'volume': float(ticker.get('volume', 0)),
            'quote_volume': float(ticker.get('quoteVolume', 0)),
            'price_change_percent': float(ticker.get('priceChangePercent', 0)),
            'high': float(ticker.get('highPrice', 0)),
            'low': float(ticker.get('lowPrice', 0)),
            'bid_price': float(ticker.get('bidPrice', 0)),
            'ask_price': float(ticker.get('askPrice', 0)),
            'count': int(ticker.get('count', 0))
        }
    except Exception as e:
        logger.error(f"Lỗi lấy metrics {symbol}: {str(e)}")
        return None

# ========== HÀM API BINANCE CŨ ==========
def _wait_for_rate_limit():
    """Đợi để tuân thủ rate limit của Binance API"""
    global _BINANCE_LAST_REQUEST_TIME
    with _BINANCE_RATE_LOCK:
        now = time.time()
        delta = now - _BINANCE_LAST_REQUEST_TIME
        if delta < _BINANCE_MIN_INTERVAL:
            time.sleep(_BINANCE_MIN_INTERVAL - delta)
        _BINANCE_LAST_REQUEST_TIME = time.time()

def sign(query, api_secret):
    """Tạo chữ ký HMAC SHA256 cho request Binance"""
    try:
        return hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Lỗi ký: {str(e)}")
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    """Gửi request tới Binance API với retry và error handling"""
    max_retries = 2
    base_url = url

    for attempt in range(max_retries):
        try:
            _wait_for_rate_limit()
            url = base_url

            if headers is None: headers = {}
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers)
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers, method=method)

            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    error_content = response.read().decode()
                    logger.error(f"Lỗi API ({response.status}): {error_content}")
                    if response.status == 401: return None
                    if response.status == 429:
                        sleep_time = 2 ** attempt
                        logger.warning(f"⚠️ 429 Quá nhiều yêu cầu, đợi {sleep_time}s")
                        time.sleep(sleep_time)
                    elif response.status >= 500: time.sleep(0.5)
                    continue

        except urllib.error.HTTPError as e:
            if e.code == 451:
                logger.error("❌ Lỗi 451: Truy cập bị chặn - Kiểm tra VPN/proxy")
                return None
            else: logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")

            if e.code == 401: return None
            if e.code == 429:
                sleep_time = 2 ** attempt
                logger.warning(f"⚠️ HTTP 429 Quá nhiều yêu cầu, đợi {sleep_time}s")
                time.sleep(sleep_time)
            elif e.code >= 500: time.sleep(0.5)
            continue

        except Exception as e:
            logger.error(f"Lỗi kết nối API (lần thử {attempt + 1}): {str(e)}")
            time.sleep(0.5)

    logger.error(f"Thất bại yêu cầu API sau {max_retries} lần thử")
    return None

def get_all_USDT_pairs(limit=50):
    """Lấy danh sách tất cả cặp USDT đang giao dịch"""
    global _USDT_CACHE
    try:
        now = time.time()
        if _USDT_CACHE["cặp"] and (now - _USDT_CACHE["cập_nhật_cuối"] < _USDT_CACHE_TTL):
            return _USDT_CACHE["cặp"][:limit]

        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data: return []

        USDT_pairs = []
        for symbol_info in data.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            if (symbol.endswith('USDT') and symbol_info.get('status') == 'TRADING' 
                and symbol not in _SYMBOL_BLACKLIST):
                USDT_pairs.append(symbol)

        _USDT_CACHE["cặp"] = USDT_pairs
        _USDT_CACHE["cập_nhật_cuối"] = now
        logger.info(f"✅ Đã lấy {len(USDT_pairs)} cặp USDT (loại trừ BTC/ETH)")
        return USDT_pairs[:limit]

    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách coin: {str(e)}")
        return []

def get_max_leverage(symbol, api_key, api_secret):
    """Lấy đòn bẩy tối đa cho một symbol"""
    global _LEVERAGE_CACHE
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        if (symbol in _LEVERAGE_CACHE["dữ_liệu"] and 
            current_time - _LEVERAGE_CACHE["cập_nhật_cuối"] < _LEVERAGE_CACHE_TTL):
            return _LEVERAGE_CACHE["dữ_liệu"][symbol]
        
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if not data: return 100
        
        for s in data['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LEVERAGE' and 'maxLeverage' in f:
                        leverage = int(f['maxLeverage'])
                        _LEVERAGE_CACHE["dữ_liệu"][symbol] = leverage
                        _LEVERAGE_CACHE["cập_nhật_cuối"] = current_time
                        return leverage
        return 100
    except Exception as e:
        logger.error(f"Lỗi đòn bẩy {symbol}: {str(e)}")
        return 100

def get_step_size(symbol, api_key, api_secret):
    """Lấy step size (bước nhảy khối lượng) cho một symbol"""
    if not symbol: return 0.001
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        data = binance_api_request(url)
        if not data: return 0.001
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        logger.error(f"Lỗi step size: {str(e)}")
    return 0.001

def set_leverage(symbol, lev, api_key, api_secret):
    """Thiết lập đòn bẩy cho một symbol"""
    if not symbol: return False
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "leverage": lev, "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/leverage?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        response = binance_api_request(url, method='POST', headers=headers)
        return bool(response and 'leverage' in response)
    except Exception as e:
        logger.error(f"Lỗi cài đặt đòn bẩy: {str(e)}")
        return False

def get_balance(api_key, api_secret):
    """Lấy số dư khả dụng USDT"""
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        data = binance_api_request(url, headers=headers)
        if not data: return None
            
        for asset in data['assets']:
            if asset['asset'] == 'USDT':
                available_balance = float(asset['availableBalance'])
                logger.info(f"💰 Số dư - Khả dụng: {available_balance:.2f} USDT")
                return available_balance
        return 0
    except Exception as e:
        logger.error(f"Lỗi số dư: {str(e)}")
        return None

def get_total_and_available_balance(api_key, api_secret):
    """
    Lấy TỔNG số dư (USDT + USDC) và số dư KHẢ DỤNG tương ứng.
    total_all   = tổng walletBalance (USDT+USDC)
    avail_all   = tổng availableBalance (USDT+USDC)
    """
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": api_key}

        data = binance_api_request(url, headers=headers)
        if not data:
            logger.error("❌ Không lấy được số dư từ Binance")
            return None, None

        total_all = 0.0
        available_all = 0.0

        for asset in data["assets"]:
            if asset["asset"] in ("USDT", "USDC"):
                available_all += float(asset["availableBalance"])
                total_all += float(asset["walletBalance"])

        logger.info(
            f"💰 Tổng số dư (USDT+USDC): {total_all:.2f}, "
            f"Khả dụng: {available_all:.2f}"
        )
        return total_all, available_all
    except Exception as e:
        logger.error(f"Lỗi lấy tổng số dư: {str(e)}")
        return None, None

def get_margin_safety_info(api_key, api_secret):
    """
    Lấy thông tin an toàn ký quỹ:
      - margin_balance = totalMarginBalance (tổng số dư ký quỹ, gồm PnL)
      - maint_margin   = totalMaintMargin (tổng mức duy trì ký quỹ)
      - ratio          = margin_balance / maint_margin  (nếu maint_margin > 0)
    """
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": api_key}

        data = binance_api_request(url, headers=headers)
        if not data:
            logger.error("❌ Không lấy được thông tin ký quỹ từ Binance")
            return None, None, None

        margin_balance = float(data.get("totalMarginBalance", 0.0))
        maint_margin = float(data.get("totalMaintMargin", 0.0))

        if maint_margin <= 0:
            logger.warning(
                f"⚠️ Maint margin <= 0 (margin_balance={margin_balance:.4f}, maint_margin={maint_margin:.4f})"
            )
            return margin_balance, maint_margin, None

        ratio = margin_balance / maint_margin

        logger.info(
            f"🛡️ An toàn ký quỹ: margin_balance={margin_balance:.4f}, "
            f"maint_margin={maint_margin:.4f}, tỷ lệ={ratio:.2f}x"
        )
        return margin_balance, maint_margin, ratio

    except Exception as e:
        logger.error(f"Lỗi lấy thông tin an toàn ký quỹ: {str(e)}")
        return None, None, None

def place_order(symbol, side, qty, api_key, api_secret):
    """Đặt lệnh MARKET"""
    if not symbol: return None
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "timestamp": ts
        }
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        return binance_api_request(url, method='POST', headers=headers)
    except Exception as e:
        logger.error(f"Lỗi lệnh: {str(e)}")
        return None

def cancel_all_orders(symbol, api_key, api_secret):
    """Hủy tất cả lệnh chờ của một symbol"""
    if not symbol: return False
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        binance_api_request(url, method='DELETE', headers=headers)
        return True
    except Exception as e:
        logger.error(f"Lỗi hủy lệnh: {str(e)}")
        return False

def get_current_price(symbol):
    """Lấy giá hiện tại của một symbol"""
    if not symbol: return 0
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            price = float(data['price'])
            return price if price > 0 else 0
        return 0
    except Exception as e:
        logger.error(f"Lỗi giá {symbol}: {str(e)}")
        return 0

def get_positions(symbol=None, api_key=None, api_secret=None):
    """Lấy thông tin vị thế (có thể lọc theo symbol)"""
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        if symbol: params["symbol"] = symbol.upper()
        query = urllib.parse.urlencode(params)
        sig = sign(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': api_key}
        
        positions = binance_api_request(url, headers=headers)
        if not positions: return []
        if symbol:
            for pos in positions:
                if pos['symbol'] == symbol.upper():
                    return [pos]
        return positions
    except Exception as e:
        logger.error(f"Lỗi vị thế: {str(e)}")
        return []

# ========== LỚP QUẢN LÝ CỐT LÕI ==========
class CoinManager:
    """Quản lý danh sách coin đang được các bot sử dụng"""
    def __init__(self):
        self.active_coins = set()
        self._lock = threading.Lock()
    
    def register_coin(self, symbol):
        """Đăng ký coin đang được sử dụng"""
        if not symbol: return
        with self._lock: self.active_coins.add(symbol.upper())
    
    def unregister_coin(self, symbol):
        """Hủy đăng ký coin"""
        if not symbol: return
        with self._lock: self.active_coins.discard(symbol.upper())
    
    def is_coin_active(self, symbol):
        """Kiểm tra coin có đang được sử dụng không"""
        if not symbol: return False
        with self._lock: return symbol.upper() in self.active_coins
    
    def get_active_coins(self):
        """Lấy danh sách coin đang hoạt động"""
        with self._lock: return list(self.active_coins)

class BotExecutionCoordinator:
    """Điều phối quyền tìm coin giữa các bot theo cơ chế FIFO"""
    def __init__(self):
        self._lock = threading.Lock()
        self._bot_queue = queue.Queue()
        self._current_finding_bot = None
        self._found_coins = set()
        self._bots_with_coins = set()
    
    def request_coin_search(self, bot_id):
        """Yêu cầu quyền tìm coin"""
        with self._lock:
            if bot_id in self._bots_with_coins:
                return False
                
            # ✅ SỬA: Cho phép bot đang được chỉ định (_current_finding_bot) được quyền scan
            if self._current_finding_bot is None or self._current_finding_bot == bot_id:
                self._current_finding_bot = bot_id
                return True
            else:
                # Chỉ xếp hàng nếu chưa ở trong queue
                if bot_id not in list(self._bot_queue.queue):
                    self._bot_queue.put(bot_id)
                return False
    
    def finish_coin_search(self, bot_id, found_symbol=None, has_coin_now=False):
        """Hoàn thành việc tìm coin và chuyển quyền cho bot tiếp theo"""
        with self._lock:
            if self._current_finding_bot == bot_id:
                self._current_finding_bot = None
                if found_symbol: self._found_coins.add(found_symbol)
                if has_coin_now: self._bots_with_coins.add(bot_id)
                
                if not self._bot_queue.empty():
                    next_bot = self._bot_queue.get()
                    self._current_finding_bot = next_bot
                    return next_bot
            return None
    
    def bot_has_coin(self, bot_id):
        """Đánh dấu bot đã có coin"""
        with self._lock:
            self._bots_with_coins.add(bot_id)
            new_queue = queue.Queue()
            while not self._bot_queue.empty():
                bot_in_queue = self._bot_queue.get()
                if bot_in_queue != bot_id: new_queue.put(bot_in_queue)
            self._bot_queue = new_queue
    
    def bot_lost_coin(self, bot_id):
        """Đánh dấu bot đã mất coin"""
        with self._lock:
            if bot_id in self._bots_with_coins:
                self._bots_with_coins.remove(bot_id)
    
    def is_coin_available(self, symbol):
        """Kiểm tra coin có sẵn để sử dụng không"""
        with self._lock: return symbol not in self._found_coins

    def bot_processing_coin(self, bot_id):
        """Đánh dấu bot đang xử lý coin (chưa vào lệnh)"""
        with self._lock:
            self._bots_with_coins.add(bot_id)
            # Xóa bot khỏi queue nếu có
            new_queue = queue.Queue()
            while not self._bot_queue.empty():
                bot_in_queue = self._bot_queue.get()
                if bot_in_queue != bot_id:
                    new_queue.put(bot_in_queue)
            self._bot_queue = new_queue
    
    def get_queue_info(self):
        """Lấy thông tin hàng đợi"""
        with self._lock:
            return {
                'current_finding': self._current_finding_bot,
                'queue_size': self._bot_queue.qsize(),
                'queue_bots': list(self._bot_queue.queue),
                'bots_with_coins': list(self._bots_with_coins),
                'found_coins_count': len(self._found_coins)
            }
    
    def get_queue_position(self, bot_id):
        """Lấy vị trí của bot trong hàng đợi"""
        with self._lock:
            if self._current_finding_bot == bot_id: return 0
            else:
                queue_list = list(self._bot_queue.queue)
                return queue_list.index(bot_id) + 1 if bot_id in queue_list else -1

class SmartCoinFinder:
    """Phân tích thị trường và tìm coin phù hợp - SỬA ĐỔI ĐỂ DÙNG DỮ LIỆU THỰC"""
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.last_scan_time = 0
        self.scan_cooldown = 10
        self.analysis_cache = {}
        self.cache_ttl = 30
        
    def get_symbol_leverage(self, symbol):
        """Lấy đòn bẩy của symbol"""
        return get_max_leverage(symbol, self.api_key, self.api_secret)
    
    def calculate_rsi(self, prices, period=14):
        """Tính chỉ số RSI"""
        if len(prices) < period + 1: return 50
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.mean(gains[:period])
        avg_losses = np.mean(losses[:period])
        if avg_losses == 0: return 100
            
        rs = avg_gains / avg_losses
        return 100 - (100 / (1 + rs))
    
    def get_rsi_signal(self, symbol, volume_threshold=10):
        """Phân tích tín hiệu RSI + Volume thực tế"""
        try:
            current_time = time.time()
            cache_key = f"{symbol}_{volume_threshold}"
            
            if (cache_key in self.analysis_cache and 
                current_time - self.analysis_cache[cache_key]['timestamp'] < self.cache_ttl):
                return self.analysis_cache[cache_key]['signal']
            
            data = binance_api_request(
                "https://fapi.binance.com/fapi/v1/klines",
                params={"symbol": symbol, "interval": "5m", "limit": 15}
            )
            if not data or len(data) < 15: return None
            
            prev_prev_candle, prev_candle, current_candle = data[-4], data[-3], data[-2]
            
            prev_prev_close, prev_close, current_close = float(prev_prev_candle[4]), float(prev_candle[4]), float(current_candle[4])
            prev_prev_volume, prev_volume, current_volume = float(prev_prev_candle[5]), float(prev_candle[5]), float(current_candle[5])
            
            closes = [float(k[4]) for k in data]
            rsi_current = self.calculate_rsi(closes)
            
            price_change_prev = prev_close - prev_prev_close
            price_change_current = current_close - prev_close
            
            volume_change_prev = (prev_volume - prev_prev_volume) / prev_prev_volume * 100
            volume_change_current = (current_volume - prev_volume) / prev_volume * 100
            
            price_increasing = price_change_current > 0
            price_decreasing = price_change_current < 0
            price_not_increasing = price_change_current <= 0
            price_not_decreasing = price_change_current >= 0
            
            volume_increasing = volume_change_current > volume_threshold
            volume_decreasing = volume_change_current < -volume_threshold
            
            # Điều kiện tín hiệu RSI
            if rsi_current > 80 and price_increasing and volume_increasing:
                result = "SELL"
            elif rsi_current < 20 and price_decreasing and volume_decreasing:
                result = "SELL"
            elif rsi_current > 80 and price_increasing and volume_decreasing:
                result = "BUY"
            elif rsi_current < 20 and price_decreasing and volume_increasing:
                result = "BUY"
            elif rsi_current > 20 and price_not_decreasing and volume_decreasing:
                result = "BUY"
            elif rsi_current < 80 and price_not_increasing and volume_increasing:
                result = "SELL"
            else:
                result = None
            
            self.analysis_cache[cache_key] = {'signal': result, 'timestamp': current_time}
            return result
            
        except Exception as e:
            logger.error(f"Lỗi phân tích RSI {symbol}: {str(e)}")
            return None
    
    def get_entry_signal(self, symbol):
        """Lấy tín hiệu vào lệnh thực tế từ RSI"""
        return self.get_rsi_signal(symbol, volume_threshold=50)
    
    def get_exit_signal(self, symbol):
        """Lấy tín hiệu thoát lệnh thực tế"""
        return self.get_rsi_signal(symbol, volume_threshold=100)
    
    def has_existing_position(self, symbol):
        """Kiểm tra có vị thế tồn tại trên symbol không"""
        try:
            positions = get_positions(symbol, self.api_key, self.api_secret)
            if positions:
                for pos in positions:
                    if abs(float(pos.get('positionAmt', 0))) > 0:
                        logger.info(f"⚠️ Đã phát hiện vị thế trên {symbol}")
                        return True
            return False
        except Exception as e:
            logger.error(f"Lỗi kiểm tra vị thế {symbol}: {str(e)}")
            return True

    def get_top_volume_coins(self, limit=15, min_volume_usd=50000):
        """Lấy top coin theo khối lượng thực tế từ Binance"""
        return get_top_volume_symbols(limit, min_volume_usd)
    
    def get_high_volatility_coins(self, limit=15, min_volatility_percent=3):
        """Lấy top coin theo biến động thực tế từ Binance"""
        return get_high_volatility_symbols(limit, min_volatility_percent)
    
    def get_coin_metrics(self, symbol):
        """Lấy chỉ số chi tiết của coin"""
        return get_symbol_metrics(symbol)

    def find_best_coin_by_volume(self, excluded_coins=None, required_leverage=10):
        """Tìm coin tốt nhất theo khối lượng"""
        try:
            now = time.time()
            if now - self.last_scan_time < self.scan_cooldown: return None
            self.last_scan_time = now

            # Lấy top coin theo volume từ Binance
            top_volume_coins = self.get_top_volume_coins(limit=20)
            if not top_volume_coins: return None

            valid_symbols = []
            for symbol in top_volume_coins:
                if excluded_coins and symbol in excluded_coins: continue
                if self.has_existing_position(symbol): continue

                max_lev = self.get_symbol_leverage(symbol)
                if max_lev < required_leverage: continue

                # Lấy tín hiệu thực tế
                time.sleep(0.5)  # Tránh rate limit
                entry_signal = self.get_entry_signal(symbol)
                if entry_signal in ["BUY", "SELL"]:
                    valid_symbols.append((symbol, entry_signal))
                    logger.info(f"✅ Đã tìm thấy coin volume cao: {symbol} - {entry_signal}")

            if not valid_symbols: return None
            selected_symbol, _ = random.choice(valid_symbols)

            if self.has_existing_position(selected_symbol): return None
            logger.info(f"🎯 Đã chọn coin volume: {selected_symbol}")
            return selected_symbol

        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin theo volume: {str(e)}")
            return None

    def find_best_coin_by_volatility(self, excluded_coins=None, required_leverage=10):
        """Tìm coin tốt nhất theo biến động"""
        try:
            now = time.time()
            if now - self.last_scan_time < self.scan_cooldown: return None
            self.last_scan_time = now

            # Lấy top coin theo biến động từ Binance
            top_volatility_coins = self.get_high_volatility_coins(limit=20)
            if not top_volatility_coins: return None

            valid_symbols = []
            for symbol in top_volatility_coins:
                if excluded_coins and symbol in excluded_coins: continue
                if self.has_existing_position(symbol): continue

                max_lev = self.get_symbol_leverage(symbol)
                if max_lev < required_leverage: continue

                # Lấy tín hiệu thực tế
                time.sleep(0.5)
                entry_signal = self.get_entry_signal(symbol)
                if entry_signal in ["BUY", "SELL"]:
                    valid_symbols.append((symbol, entry_signal))
                    logger.info(f"✅ Đã tìm thấy coin biến động cao: {symbol} - {entry_signal}")

            if not valid_symbols: return None
            selected_symbol, _ = random.choice(valid_symbols)

            if self.has_existing_position(selected_symbol): return None
            logger.info(f"🎯 Đã chọn coin biến động: {selected_symbol}")
            return selected_symbol

        except Exception as e:
            logger.error(f"❌ Lỗi tìm coin theo biến động: {str(e)}")
            return None

class WebSocketManager:
    """Quản lý kết nối WebSocket thời gian thực"""
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=20)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.price_cache = {}
        self.last_price_update = {}
        
    def add_symbol(self, symbol, callback):
        """Thêm symbol vào theo dõi WebSocket"""
        if not symbol: return
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        """Tạo kết nối WebSocket mới"""
        if self._stop_event.is_set(): return
        
        streams = [f"{symbol.lower()}@trade"]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'data' in data:
                    symbol = data['data']['s']
                    price = float(data['data']['p'])
                    current_time = time.time()
                    
                    if (symbol in self.last_price_update and 
                        current_time - self.last_price_update[symbol] < 0.1):
                        return
                    
                    self.last_price_update[symbol] = current_time
                    self.price_cache[symbol] = price
                    self.executor.submit(callback, price)
            except Exception as e:
                logger.error(f"Lỗi tin nhắn WebSocket {symbol}: {str(e)}")
                
        def on_error(ws, error):
            logger.error(f"Lỗi WebSocket {symbol}: {str(error)}")
            if not self._stop_event.is_set():
                time.sleep(5)
                self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket đã đóng {symbol}: {close_status_code} - {close_msg}")
            if not self._stop_event.is_set() and symbol in self.connections:
                time.sleep(5)
                self._reconnect(symbol, callback)
                
        ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        
        self.connections[symbol] = {'ws': ws, 'thread': thread, 'callback': callback}
        logger.info(f"🔗 WebSocket đã khởi động cho {symbol}")
        
    def _reconnect(self, symbol, callback):
        """Kết nối lại WebSocket khi mất kết nối"""
        logger.info(f"Đang kết nối lại WebSocket cho {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        """Xóa symbol khỏi theo dõi WebSocket"""
        if not symbol: return
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try: self.connections[symbol]['ws'].close()
                except Exception as e: logger.error(f"Lỗi đóng WebSocket {symbol}: {str(e)}")
                del self.connections[symbol]
                logger.info(f"WebSocket đã xóa cho {symbol}")
                
    def stop(self):
        """Dừng tất cả kết nối WebSocket"""
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# Bypass SSL verification
ssl._create_default_https_context = ssl._create_unverified_context