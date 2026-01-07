# trading_bot_lib_part3.py
# PHẦN 3: BOTMANAGER HOÀN CHỈNH VỚI TELEGRAM & DATABASE
from trading_bot_lib_part1 import (
    logger, get_all_usdt_pairs, get_max_leverage, get_step_size,
    set_leverage, get_total_and_available_balance, get_margin_safety_info,
    place_order, cancel_all_orders, get_current_price, get_positions,
    CoinManager, BotExecutionCoordinator, SmartCoinFinder, WebSocketManager,
    send_telegram, get_balance, db_manager
)

from trading_bot_lib_part2 import BalanceProtectionBot, CompoundProfitBot, StaticMarketBot

import time
import threading
import requests
import json
import os
from collections import defaultdict

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

def create_symbols_keyboard(limit=12):
    """Tạo bàn phím chọn coin từ database"""
    try:
        symbols = get_all_usdt_pairs(limit=limit) or ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]
    except:
        symbols = ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "SOLUSDT", "MATICUSDT"]
    
    keyboard = []
    row = []
    for symbol in symbols[:limit]:
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

# ========== LỚP QUẢN LÝ BOT ==========
class BotManager:
    """Quản lý toàn bộ hệ thống bot với database và telegram"""
    
    def __init__(self, api_key=None, api_secret=None, telegram_bot_token=None, telegram_chat_id=None):
        self.ws_manager = WebSocketManager()
        self.bots = {}
        self.running = True
        self.start_time = time.time()
        self.user_states = {}

        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id

        self.bot_coordinator = BotExecutionCoordinator()
        self.coin_manager = CoinManager()
        self.symbol_locks = defaultdict(threading.Lock)

        self._restore_bots_from_db()

        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT HÀNG ĐỢI ĐÃ KHỞI ĐỘNG")

            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()

            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager đã khởi động ở chế độ không cấu hình")

    # ========== DATABASE METHODS ==========
    
    def _restore_bots_from_db(self):
        """Khôi phục bot từ database khi khởi động"""
        try:
            bots_config = db_manager.get_all_bots(status='running')
            
            for bot_config in bots_config:
                try:
                    bot_id = bot_config['bot_id']
                    
                    if bot_id in self.bots:
                        continue
                    
                    bot_mode = bot_config['bot_mode']
                    
                    if bot_mode == 'static':
                        bot_class = StaticMarketBot
                        symbol = bot_config['symbol']
                    else:
                        if bot_config['dynamic_strategy'] == 'volume':
                            bot_class = CompoundProfitBot
                        else:
                            bot_class = BalanceProtectionBot
                        symbol = None
                    
                    bot = bot_class(
                        symbol=symbol,
                        lev=bot_config['leverage'],
                        percent=bot_config['percent'],
                        tp=bot_config['tp'],
                        sl=bot_config['sl'],
                        roi_trigger=bot_config['roi_trigger'],
                        ws_manager=self.ws_manager,
                        api_key=bot_config['api_key'] or self.api_key,
                        api_secret=bot_config['api_secret'] or self.api_secret,
                        telegram_bot_token=bot_config.get('telegram_chat_id') or self.telegram_bot_token,
                        telegram_chat_id=bot_config.get('telegram_chat_id') or self.telegram_chat_id,
                        bot_id=bot_id,
                        coin_manager=self.coin_manager,
                        symbol_locks=self.symbol_locks,
                        bot_coordinator=self.bot_coordinator,
                        pyramiding_n=bot_config['pyramiding_n'],
                        pyramiding_x=bot_config['pyramiding_x'],
                        dynamic_strategy=bot_config['dynamic_strategy'],
                        static_entry_mode=bot_config['static_entry_mode'],
                        reverse_on_stop=bot_config['reverse_on_stop']
                    )
                    
                    self.bots[bot_id] = bot
                    self.log(f"✅ Đã khôi phục bot {bot_id} từ database")
                    
                except Exception as e:
                    self.log(f"❌ Lỗi khôi phục bot {bot_config.get('bot_id', 'unknown')}: {str(e)}")
            
            self.log(f"✅ Đã khôi phục {len(bots_config)} bot từ database")
            
        except Exception as e:
            self.log(f"❌ Lỗi khôi phục bot từ database: {str(e)}")
    
    def _verify_api_connection(self):
        """Xác minh kết nối API Binance"""
        try:
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                self.log("❌ LỖI: Không thể kết nối đến API Binance. Kiểm tra:")
                self.log("   - API Key và Secret")
                self.log("   - Chặn IP (lỗi 451), thử VPN")
                self.log("   - Kết nối internet")
                return False
            else:
                self.log(f"✅ Kết nối Binance thành công! Số dư: {balance:.2f} USDT")
                return True
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra kết nối: {str(e)}")
            return False

    def get_position_summary(self):
        """Lấy tổng hợp thống kê chi tiết từ database"""
        try:
            all_bots = db_manager.get_all_bots()
            open_positions = db_manager.get_open_positions()
            statistics = db_manager.get_statistics()
            
            summary = "📊 **THỐNG KÊ CHI TIẾT**\n\n"
            
            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ**: {balance:.2f} USDT\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n"
            
            if statistics:
                summary += f"📈 **Tổng PnL**: {statistics.get('total_pnl', 0):.2f} USDT\n"
                summary += f"🎯 **Tổng giao dịch**: {statistics.get('total_trades', 0)}\n"
                summary += f"✅ **Thắng**: {statistics.get('winning_trades', 0)} | ❌ **Thua**: {statistics.get('losing_trades', 0)}\n\n"
            
            static_bots = [b for b in all_bots if b['bot_mode'] == 'static']
            dynamic_bots = [b for b in all_bots if b['bot_mode'] == 'dynamic']
            
            summary += f"🤖 **TỔNG SỐ BOT**: {len(all_bots)} bot\n"
            summary += f"🔧 **PHÂN LOẠI**: Tĩnh: {len(static_bots)} | Động: {len(dynamic_bots)}\n\n"
            
            if open_positions:
                summary += f"📈 **VỊ THẾ ĐANG MỞ**: {len(open_positions)}\n"
                
                for pos in open_positions[:5]:
                    symbol = pos['symbol']
                    side = pos['side']
                    entry = pos['entry_price']
                    current_price = pos['current_price'] or get_current_price(symbol)
                    roi = pos['roi'] or 0
                    
                    summary += f"🔹 {symbol} | {side} | Entry: {entry:.4f} | ROI: {roi:.2f}%\n"
                
                if len(open_positions) > 5:
                    summary += f"... và {len(open_positions) - 5} vị thế khác\n"
                summary += "\n"
            else:
                summary += "📭 **Không có vị thế đang mở**\n\n"
            
            queue_info = self.bot_coordinator.get_queue_info()
            summary += f"🎪 **THÔNG TIN HÀNG ĐỢI (FIFO)**\n"
            summary += f"• Bot đang tìm coin: {queue_info['current_finding'] or 'Không có'}\n"
            summary += f"• Bot trong hàng đợi: {queue_info['queue_size']}\n"
            summary += f"• Bot có coin: {len(queue_info['bots_with_coins'])}\n\n"
            
            if all_bots:
                summary += "📋 **CHI TIẾT BOT**:\n"
                
                for bot in all_bots[:10]:
                    bot_id = bot['bot_id']
                    mode = "🤖" if bot['bot_mode'] == 'static' else "🔄"
                    status = "🟢" if bot['status'] == 'running' else "🔴"
                    
                    bot_positions = [p for p in open_positions if p['bot_id'] == bot_id]
                    
                    summary += f"{status} {mode} **{bot_id}**\n"
                    summary += f"   📊 Đòn bẩy: {bot['leverage']}x | Vốn: {bot['percent']}%\n"
                    summary += f"   🔄 Nhồi lệnh: {bot['pyramiding_n']}/{bot['pyramiding_x']}%\n"
                    summary += f"   📈 Vị thế: {len(bot_positions)} coin\n"
                    
                    if bot_positions:
                        for pos in bot_positions[:2]:
                            summary += f"   🔗 {pos['symbol']} | {pos['side']} | ROI: {pos.get('roi', 0):.2f}%\n"
                    
                    summary += "\n"
                
                if len(all_bots) > 10:
                    summary += f"... và {len(all_bots) - 10} bot khác\n"
            
            return summary
                    
        except Exception as e:
            return f"❌ Lỗi thống kê: {str(e)}"

    def log(self, message):
        """Ghi log hệ thống"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫', '🔄']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[HỆ THỐNG] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>HỆ THỐNG</b>: {message}", 
                             chat_id=self.telegram_chat_id,
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

    def send_main_menu(self, chat_id):
        """Gửi menu chính"""
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES - HỆ THỐNG THÔNG MINH</b>\n\n"
            
            "🎯 <b>2 LOẠI BOT CHÍNH:</b>\n"
            "1. 🤖 <b>BOT TĨNH</b> - Coin cố định\n"
            "   • Giao dịch trên coin chỉ định\n"
            "   • 3 chế độ vào lệnh linh hoạt\n"
            "   • Kiểm soát chặt chẽ từng coin\n\n"
            
            "2. 🔄 <b>BOT ĐỘNG</b> - Tự tìm coin\n"
            "   • Tự động quét và chọn coin tốt nhất\n"
            "   • 2 chiến lược tìm kiếm:\n"
            "     📊 <b>Khối lượng</b>: Ưu tiên volume cao\n"
            "     📈 <b>Biến động</b>: Ưu tiên biến động mạnh\n\n"
            
            "🔄 <b>CƠ CHẾ HÀNG ĐỢI (FIFO):</b>\n"
            "• Chỉ 1 bot tìm coin tại một thời điểm\n"
            "• Bot vào lệnh → bot tiếp theo tìm NGAY\n"
            "• Bot có coin không thể vào hàng đợi\n"
            "• Bot đóng lệnh có thể vào lại hàng đợi\n\n"
            
            "⚡ <b>TÍCH HỢP DATABASE:</b>\n"
            "• Lưu trữ cấu hình bot vào PostgreSQL\n"
            "• Khôi phục bot khi khởi động lại\n"
            "• Lưu lịch sử giao dịch đầy đủ\n"
            "• Tự động dọn dẹp dữ liệu cũ\n\n"
            
            "🎯 <b>TÍN HIỆU THÔNG MINH:</b>\n"
            "• Phân tích RSI + Volume thời gian thực\n"
            "• 6 điều kiện vào/thoát lệnh\n"
            "• Kết hợp ROI + tín hiệu để thoát tối ưu"
        )
        send_telegram(welcome, chat_id=chat_id, reply_markup=create_main_menu(),
                     bot_token=self.telegram_bot_token, 
                     default_chat_id=self.telegram_chat_id)

    # ========== HÀM TẠO BÀN PHÍM MỚI ==========
    def create_static_entry_mode_keyboard(self):
        """Tạo bàn phím chọn chế độ vào lệnh cho bot tĩnh"""
        return {
            "keyboard": [
                [{"text": "🎯 Nghe tín hiệu"}, {"text": "🔄 Đảo ngược"}],
                [{"text": "⏳ Đợi hướng chuẩn"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_dynamic_strategy_keyboard(self):
        """Tạo bàn phím chọn chiến lược cho bot động"""
        return {
            "keyboard": [
                [{"text": "📊 Khối lượng"}, {"text": "📈 Biến động"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_volume_strategy_keyboard(self):
        """Tạo bàn phím cho chiến lược khối lượng"""
        return {
            "keyboard": [
                [{"text": "1000"}, {"text": "2000"}, {"text": "3000"}],
                [{"text": "5000"}, {"text": "10000"}],
                [{"text": "❌ Tắt SL"}, {"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_volatility_strategy_keyboard(self):
        """Tạo bàn phím cho chiến lược biến động"""
        return {
            "keyboard": [
                [{"text": "50"}, {"text": "100"}, {"text": "150"}],
                [{"text": "200"}, {"text": "300"}],
                [{"text": "500"}, {"text": "1000"}],
                [{"text": "✅ Bật đảo chiều"}, {"text": "❌ Tắt đảo chiều"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    # ========== HÀM THÊM BOT ==========
    def add_bot(self, bot_mode, bot_type, lev, percent, tp, sl, roi_trigger, 
                symbol=None, bot_count=1, **kwargs):
        """Thêm bot mới với cấu hình chi tiết và lưu vào database"""
        if sl == 0: sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ API Key chưa được cài đặt trong BotManager")
            return False
        
        if not self._verify_api_connection():
            self.log("❌ KHÔNG THỂ KẾT NỐI VỚI BINANCE - KHÔNG THỂ TẠO BOT")
            return False
        
        static_entry_mode = kwargs.get('static_entry_mode', 'signal')
        dynamic_strategy = kwargs.get('dynamic_strategy', 'volume')
        pyramiding_n = kwargs.get('pyramiding_n', 0)
        pyramiding_x = kwargs.get('pyramiding_x', 0)
        reverse_on_stop = kwargs.get('reverse_on_stop', False)
        
        created_count = 0
        
        try:
            for i in range(bot_count):
                if bot_mode == 'static' and symbol:
                    bot_id = f"STATIC_{symbol}_{int(time.time())}_{i}"
                else:
                    bot_id = f"DYNAMIC_{dynamic_strategy}_{int(time.time())}_{i}"
                
                if bot_id in self.bots: continue
                
                if bot_mode == 'static':
                    bot_class = StaticMarketBot
                    extra_params = {
                        'static_entry_mode': static_entry_mode,
                        'reverse_on_stop': reverse_on_stop
                    }
                else:
                    if dynamic_strategy == 'volume':
                        bot_class = CompoundProfitBot
                        if sl is None: sl = 0
                        if tp < 500: tp = 500
                    else:
                        bot_class = BalanceProtectionBot
                        if sl < 50: sl = 50
                        if tp < 200: tp = 200
                    
                    extra_params = {
                        'dynamic_strategy': dynamic_strategy,
                        'reverse_on_stop': reverse_on_stop
                    }
                
                bot = bot_class(
                symbol if bot_mode == 'static' else None,
                lev, percent, tp, sl, roi_trigger, self.ws_manager,
                self.api_key, self.api_secret, self.telegram_bot_token, self.telegram_chat_id,
                coin_manager=self.coin_manager, symbol_locks=self.symbol_locks,
                bot_coordinator=self.bot_coordinator, bot_id=bot_id, max_coins=1,
                pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                **extra_params
            )

                
                self.bots[bot_id] = bot
                created_count += 1
                
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False
        
        if created_count > 0:
            bot_data = {
                'bot_id': bot_id,
                'bot_mode': bot_mode,
                'bot_type': bot_class.__name__,
                'symbol': symbol,
                'leverage': lev,
                'percent': percent,
                'tp': tp,
                'sl': sl,
                'roi_trigger': roi_trigger,
                'pyramiding_n': pyramiding_n,
                'pyramiding_x': pyramiding_x,
                'dynamic_strategy': dynamic_strategy,
                'static_entry_mode': static_entry_mode,
                'reverse_on_stop': reverse_on_stop,
                'telegram_chat_id': self.telegram_chat_id,
                'api_key': self.api_key,
                'api_secret': self.api_secret,
                'status': 'running'
            }
            
            db_manager.save_bot_config(bot_data)
            
            mode_info = "🤖 BOT TĨNH" if bot_mode == 'static' else "🔄 BOT ĐỘNG"
            strategy_info = ""
            
            if bot_mode == 'static':
                if static_entry_mode == 'signal':
                    strategy_info = "🎯 Chế độ: Nghe tín hiệu\n• Chỉ vào lệnh khi có tín hiệu đúng hướng\n• Sau khi đóng, đợi tín hiệu mới"
                elif static_entry_mode == 'reverse':
                    strategy_info = "🔄 Chế độ: Đảo ngược\n• Sau khi đóng vị thế, mở ngay lệnh đảo ngược"
                else:
                    strategy_info = "⏳ Chế độ: Đợi hướng chuẩn\n• Sau khi đóng, đợi hướng chuẩn rồi mới vào"
            else:
                if dynamic_strategy == 'volume':
                    strategy_info = "📊 Chiến lược: Khối lượng\n• Ưu tiên coin volume cao\n• TP lớn, không SL\n• Nhồi lệnh tích cực"
                else:
                    strategy_info = "📈 Chiến lược: Biến động\n• Ưu tiên coin biến động mạnh\n• SL nhỏ, TP lớn\n• Có đảo chiều khi cắt lỗ"
            
            roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else ""
            pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if pyramiding_n > 0 and pyramiding_x > 0 else ""
            reverse_info = f" | 🔀 Đảo chiều: {'Có' if reverse_on_stop else 'Không'}" if bot_mode == 'static' or dynamic_strategy == 'volatility' else ""
            
            success_msg = (f"✅ <b>ĐÃ TẠO {created_count} BOT</b>\n\n"
                          f"{mode_info}\n{strategy_info}\n\n"
                          f"📋 THÔNG TIN CẤU HÌNH:\n"
                          f"🔢 Số bot: {created_count}\n"
                          f"💰 Đòn bẩy: {lev}x\n📊 % Số dư: {percent}%\n"
                          f"🎯 TP: {tp}%\n🛡️ SL: {sl if sl is not None else 'Tắt'}%"
                          f"{roi_info}{pyramiding_info}{reverse_info}\n")
            
            if bot_mode == 'static' and symbol:
                success_msg += f"🔗 Coin: {symbol}\n"
            else:
                success_msg += f"🔗 Coin: Tự động tìm ({dynamic_strategy})\n"
            
            success_msg += (f"\n🔄 <b>HỆ THỐNG HÀNG ĐỢI ĐƯỢC KÍCH HOẠT</b>\n"
                          f"• Bot đầu tiên trong hàng đợi tìm coin trước\n"
                          f"• Bot vào lệnh → bot tiếp theo tìm NGAY LẬP TỨC\n"
                          f"• Bot có coin không thể vào hàng đợi\n"
                          f"• Bot đóng lệnh có thể vào lại hàng đợi")
            
            if pyramiding_n > 0:
                success_msg += (f"\n\n🔄 <b>NHỒI LỆNH ĐƯỢC KÍCH HOẠT</b>\n"
                              f"• Nhồi {pyramiding_n} lần khi đạt mỗi mốc {pyramiding_x}% ROI\n"
                              f"• Mỗi lần nhồi dùng {percent}% vốn ban đầu\n"
                              f"• Tự động cập nhật giá trung bình")
            
            self.log(success_msg)
            return True
        else:
            self.log("❌ Không thể tạo bot")
            return False

    # ========== HÀM QUẢN LÝ COIN ==========
    def stop_coin(self, symbol):
        """Dừng coin trong tất cả bot"""
        stopped_count = 0
        symbol = symbol.upper()
        
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_symbol') and symbol in bot.active_symbols:
                if bot.stop_symbol(symbol): stopped_count += 1
                    
        if stopped_count > 0:
            self.log(f"✅ Đã dừng coin {symbol} trong {stopped_count} bot")
            return True
        else:
            self.log(f"❌ Không tìm thấy coin {symbol} trong bot nào")
            return False

    def get_coin_management_keyboard(self):
        """Tạo bàn phím quản lý coin từ database"""
        all_coins = set()
        for bot in self.bots.values():
            if hasattr(bot, 'active_symbols'):
                all_coins.update(bot.active_symbols)
        
        if not all_coins: return None
            
        keyboard = []
        row = []
        for coin in sorted(list(all_coins))[:12]:
            row.append({"text": f"⛔ Coin: {coin}"})
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row: keyboard.append(row)
        
        keyboard.append([{"text": "⛔ DỪNG TẤT CẢ COIN"}])
        keyboard.append([{"text": "❌ Hủy bỏ"}])
        
        return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}

    def stop_all_coins(self):
        """Dừng tất cả coin trong tất cả bot"""
        self.log("⛔ Đang dừng tất cả coin trong tất cả bot...")
        total_stopped = 0
        for bot_id, bot in self.bots.items():
            if hasattr(bot, 'stop_all_symbols'):
                stopped_count = bot.stop_all_symbols()
                total_stopped += stopped_count
                self.log(f"⛔ Đã dừng {stopped_count} coin trong bot {bot_id}")
        
        self.log(f"✅ Đã dừng tổng cộng {total_stopped} coin, hệ thống vẫn chạy")
        return total_stopped

    def stop_bot(self, bot_id, delete_config: bool = False, hard_delete: bool = False):
        """
        Dừng một bot.
    
        - delete_config=False: chỉ stop bot + update status='stopped'
        - delete_config=True : stop bot + XÓA config (soft/hard) để DB chạy lại không dựng bot nữa
        """
        bot = self.bots.get(bot_id)
    
        # 1) Stop runtime nếu bot đang chạy trong RAM
        if bot:
            try:
                bot.stop()  # BaseBot.stop() đã stop symbols + update status stopped :contentReference[oaicite:4]{index=4}
            except Exception as e:
                self.log(f"⚠️ Lỗi stop runtime bot {bot_id}: {e}")
    
            try:
                del self.bots[bot_id]
            except Exception:
                pass
    
        # 2) Update DB
        if delete_config:
            ok = db_manager.delete_bot_config(bot_id, hard=hard_delete)
            if ok:
                self.log(f"🗑️ Đã xóa {'CỨNG' if hard_delete else 'MỀM'} bot_config {bot_id}")
            else:
                self.log(f"❌ Không thể xóa bot_config {bot_id}")
            return ok
        else:
            db_manager.update_bot_status(bot_id, "stopped")
            self.log(f"🔴 Đã dừng bot {bot_id}")
            return True

    def stop_all(self, delete_config: bool = False, hard_delete: bool = False):
        """
        Dừng tất cả bot.
    
        - delete_config=False: chỉ dừng
        - delete_config=True : dừng + xóa config tất cả bot
        """
        self.log("🔴 Đang dừng tất cả bot.")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id, delete_config=delete_config, hard_delete=hard_delete)
        self.log("🔴 Đã dừng tất cả bot, hệ thống vẫn chạy")

    

    # ========== LISTENER TELEGRAM ==========
    def _telegram_listener(self):
        """Lắng nghe tin nhắn từ Telegram"""
        last_update_id = 0
        
        while self.running and self.telegram_bot_token:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates?offset={last_update_id+1}&timeout=5"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok'):
                        for update in data['result']:
                            update_id = update['update_id']
                            message = update.get('message', {})
                            chat_id = str(message.get('chat', {}).get('id'))
                            text = message.get('text', '').strip()
                            
                            if chat_id != self.telegram_chat_id: continue
                            
                            if update_id > last_update_id:
                                last_update_id = update_id
                                self._handle_telegram_message(chat_id, text)
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Lỗi nghe Telegram: {str(e)}")
                time.sleep(1)

    def _handle_telegram_message(self, chat_id, text):
        """Xử lý tin nhắn Telegram"""
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # ========== LUỒNG TẠO BOT MỚI ==========
        if current_step == 'waiting_bot_count':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    bot_count = int(text)
                    if bot_count <= 0 or bot_count > 20:
                        send_telegram("⚠️ Số bot phải từ 1-20. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['bot_count'] = bot_count
                    user_state['step'] = 'waiting_bot_mode'
                    
                    send_telegram(f"🤖 Số bot: {bot_count}\n\n<b>CHỌN LOẠI BOT:</b>",
                                chat_id=chat_id, reply_markup=create_bot_mode_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số bot:",
                                chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_bot_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "🤖 Bot Tĩnh - Coin cụ thể":
                user_state['bot_mode'] = 'static'
                user_state['step'] = 'waiting_static_entry_mode'
                
                send_telegram("🎯 <b>ĐÃ CHỌN: BOT TĨNH</b>\n\nChọn chế độ vào lệnh:",
                            chat_id=chat_id, reply_markup=self.create_static_entry_mode_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "🔄 Bot Động - Tự tìm coin":
                user_state['bot_mode'] = 'dynamic'
                user_state['step'] = 'waiting_dynamic_strategy'
                
                send_telegram("🔄 <b>ĐÃ CHỌN: BOT ĐỘNG</b>\n\nChọn chiến lược tìm coin:",
                            chat_id=chat_id, reply_markup=self.create_dynamic_strategy_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== BOT TĨNH: CHỌN CHẾ ĐỘ VÀO LỆNH ==========
        elif current_step == 'waiting_static_entry_mode':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ["🎯 Nghe tín hiệu", "🔄 Đảo ngược", "⏳ Đợi hướng chuẩn"]:
                if text == "🎯 Nghe tín hiệu":
                    user_state['static_entry_mode'] = 'signal'
                    mode_desc = "• Chỉ vào lệnh khi có tín hiệu đúng hướng\n• Sau khi đóng, đợi tín hiệu mới"
                elif text == "🔄 Đảo ngược":
                    user_state['static_entry_mode'] = 'reverse'
                    mode_desc = "• Sau khi đóng vị thế, mở ngay lệnh đảo ngược"
                else:
                    user_state['static_entry_mode'] = 'wait'
                    mode_desc = "• Sau khi đóng, đợi hướng chuẩn rồi mới vào"
                
                user_state['step'] = 'waiting_symbol'
                
                send_telegram(f"✅ Chế độ: {text}\n{mode_desc}\n\nChọn coin:",
                            chat_id=chat_id, reply_markup=create_symbols_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== BOT ĐỘNG: CHỌN CHIẾN LƯỢC ==========
        elif current_step == 'waiting_dynamic_strategy':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "📊 Khối lượng":
                user_state['dynamic_strategy'] = 'volume'
                user_state['step'] = 'waiting_volume_tp'
                
                send_telegram("📊 <b>CHIẾN LƯỢC KHỐI LƯỢNG</b>\n\n"
                            "🎯 <b>GỢI Ý CẤU HÌNH:</b>\n"
                            "• Take Profit lớn (1000-10000%)\n"
                            "• Không Stop Loss\n"
                            "• Nhồi lệnh tích cực\n\n"
                            "Chọn Take Profit (%):",
                            chat_id=chat_id, reply_markup=self.create_volume_strategy_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "📈 Biến động":
                user_state['dynamic_strategy'] = 'volatility'
                user_state['step'] = 'waiting_volatility_tp'
                
                send_telegram("📈 <b>CHIẾN LƯỢC BIẾN ĐỘNG</b>\n\n"
                            "🎯 <b>GỢI Ý CẤU HÌNH:</b>\n"
                            "• Stop Loss nhỏ (50-100%)\n"
                            "• Take Profit lớn (200-1000%)\n"
                            "• Có đảo chiều khi cắt lỗ\n\n"
                            "Chọn Take Profit (%):",
                            chat_id=chat_id, reply_markup=self.create_volatility_strategy_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== BOT ĐỘNG KHỐI LƯỢNG: CHỌN TP ==========
        elif current_step == 'waiting_volume_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt SL':
                user_state['tp'] = 1000
                user_state['sl'] = None
                user_state['step'] = 'waiting_leverage'
                
                send_telegram(f"📊 Take Profit: {user_state['tp']}%\n🛡️ Stop Loss: Tắt\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp < 100:
                        send_telegram("⚠️ TP phải ≥100% cho chiến lược khối lượng. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=self.create_volume_strategy_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['tp'] = tp
                    user_state['sl'] = None
                    user_state['step'] = 'waiting_leverage'
                    
                    send_telegram(f"📊 Take Profit: {tp}%\n🛡️ Stop Loss: Tắt\n\nChọn đòn bẩy:",
                                chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=self.create_volume_strategy_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== BOT ĐỘNG BIẾN ĐỘNG: CHỌN TP ==========
        elif current_step == 'waiting_volatility_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ["✅ Bật đảo chiều", "❌ Tắt đảo chiều"]:
                user_state['reverse_on_stop'] = (text == "✅ Bật đảo chiều")
                user_state['step'] = 'waiting_volatility_sl'
                
                send_telegram(f"🔀 Đảo chiều khi cắt lỗ: {'Bật' if user_state['reverse_on_stop'] else 'Tắt'}\n\nChọn Stop Loss (%):",
                            chat_id=chat_id, reply_markup=create_sl_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp < 50:
                        send_telegram("⚠️ TP phải ≥50% cho chiến lược biến động. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=self.create_volatility_strategy_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_volatility_sl'
                    
                    send_telegram(f"🎯 Take Profit: {tp}%\n\nChọn Stop Loss (%):",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=self.create_volatility_strategy_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_volatility_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    sl = float(text)
                    if sl < 0:
                        send_telegram("⚠️ SL phải ≥0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_volatility_reverse'
                    
                    if 'reverse_on_stop' not in user_state:
                        user_state['reverse_on_stop'] = True
                    
                    send_telegram(f"🛡️ Stop Loss: {sl}%\n\nBật đảo chiều khi cắt lỗ?",
                                chat_id=chat_id, reply_markup={
                                    "keyboard": [
                                        [{"text": "✅ Bật đảo chiều"}, {"text": "❌ Tắt đảo chiều"}],
                                        [{"text": "❌ Hủy bỏ"}]
                                    ],
                                    "resize_keyboard": True,
                                    "one_time_keyboard": True
                                },
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_volatility_reverse':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text in ["✅ Bật đảo chiều", "❌ Tắt đảo chiều"]:
                user_state['reverse_on_stop'] = (text == "✅ Bật đảo chiều")
                user_state['step'] = 'waiting_leverage'
                
                send_telegram(f"🔀 Đảo chiều: {'Bật' if user_state['reverse_on_stop'] else 'Tắt'}\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== BOT TĨNH: CHỌN SYMBOL ==========
        elif current_step == 'waiting_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                user_state['symbol'] = text
                user_state['step'] = 'waiting_leverage'
                send_telegram(f"🔗 Coin: {text}\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== CÁC BƯỚC CHUNG ==========
        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                lev_text = text[:-1] if text.endswith('x') else text
                try:
                    leverage = int(lev_text)
                    if leverage <= 0 or leverage > 100:
                        send_telegram("⚠️ Đòn bẩy phải từ 1-100. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['leverage'] = leverage
                    user_state['step'] = 'waiting_percent'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    balance_info = f"\n💰 Số dư hiện tại: {balance:.2f} USDT" if balance else ""
                    
                    send_telegram(f"💰 Đòn bẩy: {leverage}x{balance_info}\n\nChọn % số dư mỗi lệnh:",
                                chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho đòn bẩy:",
                                chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_percent':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    percent = float(text)
                    if percent <= 0 or percent > 100:
                        send_telegram("⚠️ % số dư phải từ 0.1-100. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['percent'] = percent
                    user_state['step'] = 'waiting_pyramiding_n'
                    
                    balance = get_balance(self.api_key, self.api_secret)
                    actual_amount = balance * (percent / 100) if balance else 0
                    
                    if 'tp' not in user_state:
                        user_state['step'] = 'waiting_tp'
                        send_telegram(f"📊 % Số dư: {percent}%\n💵 Số tiền mỗi lệnh: ~{actual_amount:.2f} USDT\n\nChọn Take Profit (%):",
                                    chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    else:
                        send_telegram(f"📊 % Số dư: {percent}%\n💵 Số tiền mỗi lệnh: ~{actual_amount:.2f} USDT\n\n🔄 <b>CẤU HÌNH NHỒI LỆNH</b>\n\nNhập số lần nhồi lệnh (0 để tắt):",
                                    chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho % số dư:",
                                chat_id=chat_id, reply_markup=create_percent_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp <= 0:
                        send_telegram("⚠️ Take Profit phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_sl'
                    
                    send_telegram(f"🎯 Take Profit: {tp}%\n\nChọn Stop Loss (%):",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=create_tp_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    sl = float(text)
                    if sl < 0:
                        send_telegram("⚠️ Stop Loss phải >=0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_pyramiding_n'
                    
                    send_telegram(f"🛡️ Stop Loss: {sl}%\n\n🔄 <b>CẤU HÌNH NHỒI LỆNH</b>\n\nNhập số lần nhồi lệnh (0 để tắt):",
                                chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id=chat_id, reply_markup=create_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_pyramiding_n':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt tính năng':
                user_state['pyramiding_n'] = 0
                user_state['pyramiding_x'] = 0
                user_state['step'] = 'waiting_roi_trigger'
                send_telegram(f"🔄 Nhồi lệnh: TẮT\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                            chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    pyramiding_n = int(text)
                    if pyramiding_n < 0 or pyramiding_n > 5:
                        send_telegram("⚠️ Số lần nhồi lệnh phải từ 0-5. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['pyramiding_n'] = pyramiding_n
                    
                    if pyramiding_n > 0:
                        user_state['step'] = 'waiting_pyramiding_x'
                        send_telegram(f"🔄 Số lần nhồi: {pyramiding_n}\n\nNhập mốc ROI để nhồi lệnh (%):",
                                    chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    else:
                        user_state['pyramiding_x'] = 0
                        user_state['step'] = 'waiting_roi_trigger'
                        send_telegram(f"🔄 Nhồi lệnh: TẮT\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                                    chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số nguyên cho số lần nhồi lệnh:",
                                chat_id=chat_id, reply_markup=create_pyramiding_n_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif current_step == 'waiting_pyramiding_x':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    pyramiding_x = float(text)
                    if pyramiding_x <= 0:
                        send_telegram("⚠️ Mốc ROI nhồi lệnh phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['pyramiding_x'] = pyramiding_x
                    user_state['step'] = 'waiting_roi_trigger'
                    
                    send_telegram(f"🔄 Nhồi lệnh: {user_state['pyramiding_n']} lần tại {pyramiding_x}% ROI\n\n🎯 <b>CHỌN NGƯỠNG ROI CHO THOÁT THÔNG MINH</b>\n\nChọn ngưỡng kích hoạt ROI (%):",
                                chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số cho mốc ROI nhồi lệnh:",
                                chat_id=chat_id, reply_markup=create_pyramiding_x_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        elif current_step == 'waiting_roi_trigger':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt tính năng':
                user_state['roi_trigger'] = None
                self._finish_bot_creation(chat_id, user_state)
            else:
                try:
                    roi_trigger = float(text)
                    if roi_trigger <= 0:
                        send_telegram("⚠️ Ngưỡng ROI phải >0. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
    
                    user_state['roi_trigger'] = roi_trigger
                    self._finish_bot_creation(chat_id, user_state)
                    
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Ngưỡng ROI:",
                                chat_id=chat_id, reply_markup=create_roi_trigger_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
    
        # ========== CÁC LỆNH KHÁC ==========
        elif text == "⛔ Quản lý Coin":
            keyboard = self.get_coin_management_keyboard()
            if not keyboard:
                send_telegram("📭 Không có coin nào đang được quản lý", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("⛔ <b>QUẢN LÝ COIN</b>\n\nChọn coin để dừng:",
                            chat_id=chat_id, reply_markup=keyboard,
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text.startswith("⛔ Coin: "):
            symbol = text.replace("⛔ Coin: ", "").strip()
            if self.stop_coin(symbol):
                send_telegram(f"✅ Đã dừng coin {symbol}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram(f"❌ Không thể dừng coin {symbol}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ DỪNG TẤT CẢ COIN":
            stopped_count = self.stop_all_coins()
            send_telegram(f"✅ Đã dừng {stopped_count} coin, hệ thống vẫn chạy", chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_count'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nKiểm tra API Key và mạng!", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            
            send_telegram(f"🎯 <b>CHỌN SỐ LƯỢNG BOT</b>\n\n💰 Số dư hiện tại: <b>{balance:.2f} USDT</b>\n\nChọn số lượng bot (mỗi bot quản lý 1 coin):",
                         chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📊 Danh sách Bot":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                message = "⛔ <b>CHỌN BOT ĐỂ DỪNG</b>\n\n"
                bot_keyboard = []
                
                for bot_id, bot in self.bots.items():
                    bot_keyboard.append([{"text": f"⛔ Bot: {bot_id}"}])
                
                keyboard = []
                if bot_keyboard: keyboard.extend(bot_keyboard)
                keyboard.append([{"text": "⛔ DỪNG TẤT CẢ BOT"}])
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(message, chat_id=chat_id, 
                            reply_markup={"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text.startswith("⛔ Bot: "):
            bot_id = text.replace("⛔ Bot: ", "").strip()
            if self.stop_bot(bot_id):
                send_telegram(f"✅ Đã dừng bot {bot_id}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram(f"❌ Không tìm thấy bot {bot_id}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⛔ DỪNG TẤT CẢ BOT":
            stopped_count = len(self.bots)
            self.stop_all()
            send_telegram(f"✅ Đã dừng {stopped_count} bot, hệ thống vẫn chạy", chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📊 Thống kê":
            summary = self.get_position_summary()
            send_telegram(summary, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "💰 Số dư":
            try:
                balance = get_balance(self.api_key, self.api_secret)
                if balance is None:
                    send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nKiểm tra API Key và mạng!", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                else:
                    total_balance, available_balance = get_total_and_available_balance(self.api_key, self.api_secret)
                    margin_balance, maint_margin, ratio = get_margin_safety_info(self.api_key, self.api_secret)
                    
                    message = f"💰 <b>THÔNG TIN SỐ DƯ</b>\n\n"
                    message += f"💳 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT\n"
                    
                    if total_balance is not None:
                        message += f"📊 <b>TỔNG SỐ DƯ (USDT+USDC)</b>: {total_balance:.2f} USDT\n"
                        message += f"💵 <b>KHẢ DỤNG (USDT+USDC)</b>: {available_balance:.2f} USDT\n"
                    
                    if margin_balance is not None and ratio is not None:
                        message += f"\n🛡️ <b>AN TOÀN KÝ QUỸ</b>\n"
                        message += f"• Margin Balance: {margin_balance:.2f}\n"
                        message += f"• Maint Margin: {maint_margin:.2f}\n"
                        message += f"• Tỷ lệ: {ratio:.2f}x"
                    
                    send_telegram(message, chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi số dư: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                open_positions = db_manager.get_open_positions()
                
                if not open_positions:
                    send_telegram("📭 Không có vị thế mở", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
                
                message = "📈 <b>VỊ THẾ ĐANG MỞ</b>\n\n"
                total_pnl = 0
                
                for pos in open_positions:
                    symbol = pos['symbol']
                    entry = pos['entry_price']
                    side = pos['side']
                    quantity = pos['quantity']
                    roi = pos.get('roi', 0)
                    current_price = pos.get('current_price') or get_current_price(symbol)
                    
                    if current_price > 0:
                        if side == "BUY":
                            pnl = (current_price - entry) * quantity
                        else:
                            pnl = (entry - current_price) * quantity
                        
                        total_pnl += pnl
                    
                    message += (f"🔹 {symbol} | {side}\n"
                              f"📊 Khối lượng: {quantity:.4f}\n"
                              f"🏷️ Entry: {entry:.4f}\n"
                              f"💰 ROI: {roi:.2f}%\n\n")
                
                message += f"📊 <b>TỔNG PnL: {total_pnl:.2f} USDT</b>"
                
                send_telegram(message, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi vị thế: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>HỆ THỐNG BOT THÔNG MINH</b>\n\n"
                
                "🤖 <b>BOT TĨNH - COIN CỐ ĐỊNH</b>\n"
                "1. 🎯 <b>Nghe tín hiệu</b>\n"
                "   • Chỉ vào lệnh khi có tín hiệu đúng hướng\n"
                "   • Sau khi đóng, đợi tín hiệu mới\n\n"
                
                "2. 🔄 <b>Đảo ngược</b>\n"
                "   • Sau khi đóng vị thế, mở ngay lệnh đảo ngược\n"
                "   • Tận dụng biến động liên tục\n\n"
                
                "3. ⏳ <b>Đợi hướng chuẩn</b>\n"
                "   • Sau khi đóng, đợi hướng chuẩn rồi mới vào\n"
                "   • Tránh vào lệnh sai thời điểm\n\n"
                
                "🔄 <b>BOT ĐỘNG - TỰ TÌM COIN</b>\n"
                "1. 📊 <b>Chiến lược Khối lượng</b>\n"
                "   • Ưu tiên coin có volume giao dịch cao\n"
                "   • Take Profit lớn (1000-10000%)\n"
                "   • Không Stop Loss\n"
                "   • Nhồi lệnh tích cực\n\n"
                
                "2. 📈 <b>Chiến lược Biến động</b>\n"
                "   • Ưu tiên coin biến động mạnh\n"
                "   • Stop Loss nhỏ (50-100%)\n"
                "   • Take Profit lớn (200-1000%)\n"
                "   • Có đảo chiều khi cắt lỗ\n\n"
                
                "🔄 <b>NHỒI LỆNH THÔNG MINH</b>\n"
                "• Chỉ nhồi khi ROI âm đạt mốc cài đặt\n"
                "• Mỗi lần nhồi dùng % vốn ban đầu\n"
                "• Tự động cập nhật giá trung bình\n"
                "• Tối đa 5 lần nhồi mỗi vị thế\n\n"
                
                "🎯 <b>TÍN HIỆU RSI + VOLUME</b>\n"
                "• Phân tích 6 điều kiện vào/thoát lệnh\n"
                "• Kết hợp ROI + tín hiệu để thoát tối ưu\n"
                "• Tự động quét coin tốt nhất thị trường"
            )
            send_telegram(strategy_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            all_bots = db_manager.get_all_bots()
            open_positions = db_manager.get_open_positions()
            
            static_bots = [b for b in all_bots if b['bot_mode'] == 'static']
            dynamic_bots = [b for b in all_bots if b['bot_mode'] == 'dynamic']
            volume_bots = [b for b in dynamic_bots if b['dynamic_strategy'] == 'volume']
            volatility_bots = [b for b in dynamic_bots if b['dynamic_strategy'] == 'volatility']
            
            trading_bots = len(open_positions)
            pyramiding_bots = len([b for b in all_bots if b['pyramiding_n'] > 0])
            
            config_info = (f"⚙️ <b>CẤU HÌNH HỆ THỐNG</b>\n\n"
                          f"🔑 Binance API: {api_status}\n\n"
                          f"🎯 <b>PHÂN BỐ BOT:</b>\n"
                          f"🤖 Bot tĩnh: {len(static_bots)}\n"
                          f"🔄 Bot động: {len(dynamic_bots)}\n"
                          f"   📊 Khối lượng: {len(volume_bots)}\n"
                          f"   📈 Biến động: {len(volatility_bots)}\n\n"
                          f"🤖 <b>TỔNG SỐ BOT:</b> {len(all_bots)}\n"
                          f"📊 Bot đang giao dịch: {trading_bots}\n"
                          f"🔄 Bot có nhồi lệnh: {pyramiding_bots}\n\n"
                          f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                          f"🗄️ Database: PostgreSQL (Railway)\n"
                          f"📋 Hàng đợi: {self.bot_coordinator.get_queue_info()['queue_size']} bot")
            send_telegram(config_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text:
            self.send_main_menu(chat_id)

    def _finish_bot_creation(self, chat_id, user_state):
        """Hoàn thành quá trình tạo bot"""
        try:
            bot_mode = user_state.get('bot_mode', 'static')
            leverage = user_state.get('leverage')
            percent = user_state.get('percent')
            tp = user_state.get('tp')
            sl = user_state.get('sl')
            roi_trigger = user_state.get('roi_trigger')
            symbol = user_state.get('symbol')
            bot_count = user_state.get('bot_count', 1)
            pyramiding_n = user_state.get('pyramiding_n', 0)
            pyramiding_x = user_state.get('pyramiding_x', 0)
            
            extra_params = {}
            
            if bot_mode == 'static':
                static_entry_mode = user_state.get('static_entry_mode', 'signal')
                extra_params['static_entry_mode'] = static_entry_mode
                extra_params['reverse_on_stop'] = user_state.get('reverse_on_stop', False)
            else:
                dynamic_strategy = user_state.get('dynamic_strategy', 'volume')
                extra_params['dynamic_strategy'] = dynamic_strategy
                extra_params['reverse_on_stop'] = user_state.get('reverse_on_stop', False)
            
            success = self.add_bot(
                bot_mode=bot_mode,
                bot_type="custom",
                symbol=symbol, lev=leverage, percent=percent, tp=tp, sl=sl,
                roi_trigger=roi_trigger, bot_count=bot_count,
                pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                **extra_params
            )
            
            if success:
                mode_info = "🤖 BOT TĨNH" if bot_mode == 'static' else "🔄 BOT ĐỘNG"
                strategy_info = ""
                
                if bot_mode == 'static':
                    entry_mode = user_state.get('static_entry_mode', 'signal')
                    if entry_mode == 'signal':
                        strategy_info = "🎯 Chế độ: Nghe tín hiệu"
                    elif entry_mode == 'reverse':
                        strategy_info = "🔄 Chế độ: Đảo ngược"
                    else:
                        strategy_info = "⏳ Chế độ: Đợi hướng chuẩn"
                else:
                    dynamic_strategy = user_state.get('dynamic_strategy', 'volume')
                    if dynamic_strategy == 'volume':
                        strategy_info = "📊 Chiến lược: Khối lượng"
                    else:
                        strategy_info = "📈 Chiến lược: Biến động"
                
                roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else ""
                pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if pyramiding_n > 0 and pyramiding_x > 0 else ""
                reverse_info = f" | 🔀 Đảo chiều: {'Có' if user_state.get('reverse_on_stop') else 'Không'}" if bot_mode == 'static' or user_state.get('dynamic_strategy') == 'volatility' else ""
                
                success_msg = (f"✅ <b>ĐÃ TẠO BOT THÀNH CÔNG</b>\n\n"
                              f"{mode_info}\n{strategy_info}\n\n"
                              f"📋 THÔNG TIN CẤU HÌNH:\n"
                              f"🔢 Số bot: {bot_count}\n"
                              f"💰 Đòn bẩy: {leverage}x\n📊 % Số dư: {percent}%\n"
                              f"🎯 TP: {tp}%\n🛡️ SL: {sl if sl is not None else 'Tắt'}%"
                              f"{roi_info}{pyramiding_info}{reverse_info}")
                if bot_mode == 'static' and symbol: 
                    success_msg += f"\n🔗 Coin: {symbol}"
                else:
                    dyn_strat = user_state.get('dynamic_strategy', 'volume')
                    success_msg += f"\n🔗 Coin: Tự động tìm ({dyn_strat})"
                
                success_msg += (f"\n\n🔄 <b>HỆ THỐNG HÀNG ĐỢI ĐƯỢC KÍCH HOẠT</b>\n"
                              f"• Bot đầu tiên trong hàng đợi tìm coin trước\n"
                              f"• Bot vào lệnh → bot tiếp theo tìm NGAY LẬP TỨC\n"
                              f"• Bot có coin không thể vào hàng đợi\n"
                              f"• Bot đóng lệnh có thể vào lại hàng đợi")
                
                if pyramiding_n > 0:
                    success_msg += (f"\n\n🔄 <b>NHỒI LỆNH ĐƯỢC KÍCH HOẠT</b>\n"
                                  f"• Nhồi {pyramiding_n} lần khi đạt mỗi mốc {pyramiding_x}% ROI\n"
                                  f"• Mỗi lần nhồi dùng {percent}% vốn ban đầu\n"
                                  f"• Tự động cập nhật giá trung bình")
                
                send_telegram(success_msg, chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                send_telegram("❌ Lỗi tạo bot. Vui lòng thử lại.",
                            chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            
            self.user_states[chat_id] = {}
            
        except Exception as e:
            send_telegram(f"❌ Lỗi tạo bot: {str(e)}", chat_id=chat_id, reply_markup=create_main_menu(),
                        bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            self.user_states[chat_id] = {}

# ========== HÀM CHẠY CHÍNH ==========
def create_bot_manager(api_key=None, api_secret=None, telegram_bot_token=None, telegram_chat_id=None):
    """Hàm tạo BotManager để sử dụng từ bên ngoài"""
    return BotManager(
        api_key=api_key,
        api_secret=api_secret,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id
    )

# ========== HÀM CHẠY ỨNG DỤNG ==========
def run_bot_manager():
    """Chạy ứng dụng BotManager với cấu hình từ biến môi trường"""
    
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_SECRET_KEY')
    telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not api_key or not api_secret:
        logger.error("❌ Thiếu cấu hình Binance API Key/Secret")
        logger.info("ℹ️ Thiết lập biến môi trường:")
        logger.info("  - BINANCE_API_KEY: Your Binance API Key")
        logger.info("  - BINANCE_SECRET_KEY: Your Binance API Secret")
        logger.info("  - TELEGRAM_BOT_TOKEN: Your Telegram Bot Token")
        logger.info("  - TELEGRAM_CHAT_ID: Your Telegram Chat ID")
        logger.info("  - DATABASE_URL: PostgreSQL connection URL (từ Railway)")
        return None
    
    logger.info("🟢 Đang khởi động BotManager...")
    logger.info(f"📊 API Key: {api_key[:10]}...")
    logger.info(f"🤖 Telegram Chat ID: {telegram_chat_id}")
    
    bot_manager = create_bot_manager(
        api_key=api_key,
        api_secret=api_secret,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id
    )
    
    return bot_manager

# Chạy ứng dụng nếu được gọi trực tiếp
if __name__ == "__main__":
    bot_manager = run_bot_manager()
    
    if bot_manager:
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            logger.info("🛑 Đang dừng hệ thống...")
            bot_manager.stop_all()
            logger.info("🔴 Hệ thống đã dừng")



