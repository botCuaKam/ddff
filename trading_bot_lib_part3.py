# trading_bot_lib_part3.py - PHIÊN BẢN MỚI
# PHẦN 3: BOTMANAGER VỚI LUỒNG TẠO BOT MỚI - SỬ DỤNG API THỰC BINANCE
from trading_bot_lib_part1 import (
    logger, get_all_usdt_pairs, get_max_leverage, get_step_size,
    set_leverage, get_total_and_available_balance, get_margin_safety_info,
    place_order, cancel_all_orders, get_current_price, get_positions,
    CoinManager, BotExecutionCoordinator, SmartCoinFinder, WebSocketManager,
    send_telegram, create_main_menu, create_cancel_keyboard,
    create_bot_count_keyboard, create_bot_mode_keyboard, create_symbols_keyboard,
    create_leverage_keyboard, create_percent_keyboard, create_tp_keyboard,
    create_sl_keyboard, create_roi_trigger_keyboard, create_pyramiding_n_keyboard,
    create_pyramiding_x_keyboard, get_balance, get_top_volume_symbols, 
    get_high_volatility_symbols, get_symbol_metrics
)

from trading_bot_lib_part2 import BalanceProtectionBot, CompoundProfitBot, StaticMarketBot

import time
import threading
import requests
import json
from collections import defaultdict

# ========== LỚP QUẢN LÝ BOT MỚI ==========
class BotManager:
    """Quản lý toàn bộ hệ thống bot với luồng tạo bot mới hoàn chỉnh"""
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

        if api_key and api_secret:
            self._verify_api_connection()
            self.log("🟢 HỆ THỐNG BOT THÔNG MINH ĐÃ KHỞI ĐỘNG")
            self.log("📊 Sử dụng dữ liệu thực từ Binance API")

            self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
            self.telegram_thread.start()

            if self.telegram_chat_id:
                self.send_main_menu(self.telegram_chat_id)
        else:
            self.log("⚡ BotManager đã khởi động ở chế độ không cấu hình")

    # ========== HÀM TIỆN ÍCH ==========
    def _verify_api_connection(self):
        """Xác minh kết nối API Binance với dữ liệu thực"""
        try:
            # Kiểm tra kết nối bằng cách lấy top volume symbols
            top_symbols = get_top_volume_symbols(limit=5)
            if top_symbols and len(top_symbols) > 0:
                self.log(f"✅ Kết nối Binance thành công! Top coin: {', '.join(top_symbols[:3])}")
                
                # Lấy số dư thực tế
                balance = get_balance(self.api_key, self.api_secret)
                if balance is not None:
                    self.log(f"💰 Số dư thực tế: {balance:.2f} USDT")
                    return True
                else:
                    self.log("❌ Không thể lấy số dư, kiểm tra API Key/Secret")
                    return False
            else:
                self.log("❌ Không thể kết nối đến API Binance thực")
                return False
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra kết nối: {str(e)}")
            return False

    def get_position_summary(self):
        """Lấy tổng hợp thống kê chi tiết từ dữ liệu thực"""
        try:
            all_positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
            
            total_long_count, total_short_count = 0, 0
            total_long_pnl, total_short_pnl, total_unrealized_pnl = 0, 0, 0
            
            for pos in all_positions:
                position_amt = float(pos.get('positionAmt', 0))
                if position_amt != 0:
                    unrealized_pnl = float(pos.get('unRealizedProfit', 0))
                    total_unrealized_pnl += unrealized_pnl
                    
                    if position_amt > 0:
                        total_long_count += 1
                        total_long_pnl += unrealized_pnl
                    else:
                        total_short_count += 1
                        total_short_pnl += unrealized_pnl
            
            # Phân loại bot
            static_bots = 0
            dynamic_bots = 0
            volume_bots = 0
            volatility_bots = 0
            
            bot_details = []
            total_bots_with_coins, trading_bots = 0, 0
            
            for bot_id, bot in self.bots.items():
                # Phân loại theo chế độ
                if hasattr(bot, 'symbol') and bot.symbol:
                    static_bots += 1
                else:
                    dynamic_bots += 1
                    if hasattr(bot, 'dynamic_strategy'):
                        if bot.dynamic_strategy == 'volume':
                            volume_bots += 1
                        else:
                            volatility_bots += 1
                
                has_coin = len(bot.active_symbols) > 0 if hasattr(bot, 'active_symbols') else False
                is_trading = False
                
                if has_coin and hasattr(bot, 'symbol_data'):
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False):
                            is_trading = True
                            break
                
                if has_coin: total_bots_with_coins += 1
                if is_trading: trading_bots += 1
                
                bot_details.append({
                    'bot_id': bot_id, 
                    'has_coin': has_coin, 
                    'is_trading': is_trading,
                    'symbols': bot.active_symbols if hasattr(bot, 'active_symbols') else [],
                    'symbol_data': bot.symbol_data if hasattr(bot, 'symbol_data') else {},
                    'status': bot.status, 
                    'leverage': bot.lev, 
                    'percent': bot.percent,
                    'pyramiding': f"{bot.pyramiding_n}/{bot.pyramiding_x}%" if hasattr(bot, 'pyramiding_enabled') and bot.pyramiding_enabled else "Tắt",
                    'strategy': getattr(bot, 'dynamic_strategy', 'static')
                })
            
            summary = "📊 **THỐNG KÊ CHI TIẾT**\n\n"
            
            balance = get_balance(self.api_key, self.api_secret)
            if balance is not None:
                summary += f"💰 **SỐ DƯ THỰC**: {balance:.2f} USDT\n"
                summary += f"📈 **Tổng PnL**: {total_unrealized_pnl:.2f} USDT\n\n"
            else:
                summary += f"💰 **SỐ DƯ**: ❌ Lỗi kết nối\n\n"
            
            summary += f"🤖 **TỔNG SỐ BOT**: {len(self.bots)} bot | {total_bots_with_coins} bot có coin | {trading_bots} bot đang giao dịch\n"
            summary += f"🔧 **PHÂN LOẠI**: Tĩnh: {static_bots} | Động: {dynamic_bots} (Khối lượng: {volume_bots} | Biến động: {volatility_bots})\n\n"
            
            if total_long_count > 0 or total_short_count > 0:
                summary += f"📈 **VỊ THẾ THỰC TRÊN BINANCE**:\n"
                summary += f"   📊 Số lượng: LONG={total_long_count} | SHORT={total_short_count}\n"
                summary += f"   💰 PnL: LONG={total_long_pnl:.2f} USDT | SHORT={total_short_pnl:.2f} USDT\n"
                summary += f"   ⚖️ Chênh lệch: {abs(total_long_pnl - total_short_pnl):.2f} USDT\n\n"
            
            queue_info = self.bot_coordinator.get_queue_info()
            summary += f"🎪 **HỆ THỐNG HÀNG ĐỢI (FIFO)**:\n"
            summary += f"• Bot đang tìm coin: {queue_info['current_finding'] or 'Không có'}\n"
            summary += f"• Bot trong hàng đợi: {queue_info['queue_size']}\n"
            summary += f"• Bot có coin: {len(queue_info['bots_with_coins'])}\n"
            summary += f"• Coin đã phân phối: {queue_info['found_coins_count']}\n\n"
            
            if bot_details:
                summary += "📋 **CHI TIẾT BOT**:\n"
                for bot in bot_details:
                    status_emoji = "🟢" if bot['is_trading'] else "🟡" if bot['has_coin'] else "🔴"
                    strategy_emoji = "🤖" if bot['strategy'] == 'static' else "📊" if bot['strategy'] == 'volume' else "📈"
                    
                    summary += f"{status_emoji}{strategy_emoji} **{bot['bot_id']}**\n"
                    summary += f"   📊 Đòn bẩy: {bot['leverage']}x | Vốn: {bot['percent']}%\n"
                    summary += f"   🔄 Nhồi lệnh: {bot['pyramiding']}\n"
                    
                    if bot['symbols']:
                        for symbol in bot['symbols']:
                            symbol_info = bot['symbol_data'].get(symbol, {})
                            status = "🟢 Đang giao dịch" if symbol_info.get('position_open') else "🟡 Chờ tín hiệu"
                            side = symbol_info.get('side', '')
                            qty = symbol_info.get('qty', 0)
                            
                            summary += f"   🔗 {symbol} | {status}"
                            if side: summary += f" | {side} {abs(qty):.4f}"
                            
                            if symbol_info.get('pyramiding_count', 0) > 0:
                                summary += f" | 🔄 {symbol_info['pyramiding_count']} lần"
                                
                            summary += "\n"
                    else:
                        strategy_text = "Tĩnh" if bot['strategy'] == 'static' else f"Động ({bot['strategy']})"
                        summary += f"   🔍 Đang tìm coin ({strategy_text})...\n"
                    summary += "\n"
            
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
            
            "🎯 <b>LUỒNG TẠO BOT MỚI:</b>\n"
            "1. Chọn loại bot (Tĩnh/Động)\n"
            "2. Tĩnh: Chọn coin cố định\n"
            "3. Động: Chọn chiến lược tìm coin\n"
            "4. Cấu hình tham số giao dịch\n\n"
            
            "📊 <b>DỮ LIỆU THỰC TỪ BINANCE:</b>\n"
            "• Top coin theo khối lượng\n"
            "• Top coin theo biến động\n"
            "• Tín hiệu RSI + Volume thực\n"
            "• Số dư và giá thực\n\n"
            
            "⚡ <b>CHỌN '➕ Thêm Bot' ĐỂ BẮT ĐẦU</b>"
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
                [{"text": "⏳ Đợi hướng chuẩn"}, {"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_dynamic_strategy_keyboard(self):
        """Tạo bàn phím chọn chiến lược cho bot động"""
        return {
            "keyboard": [
                [{"text": "📊 Khối lượng (Volume)"}, {"text": "📈 Biến động (Volatility)"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_volume_strategy_tp_keyboard(self):
        """Tạo bàn phím TP cho chiến lược khối lượng"""
        return {
            "keyboard": [
                [{"text": "500"}, {"text": "1000"}, {"text": "2000"}],
                [{"text": "3000"}, {"text": "5000"}, {"text": "10000"}],
                [{"text": "❌ Tắt SL"}, {"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_volatility_strategy_tp_keyboard(self):
        """Tạo bàn phím TP cho chiến lược biến động"""
        return {
            "keyboard": [
                [{"text": "100"}, {"text": "200"}, {"text": "300"}],
                [{"text": "500"}, {"text": "1000"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_volatility_strategy_sl_keyboard(self):
        """Tạo bàn phím SL cho chiến lược biến động"""
        return {
            "keyboard": [
                [{"text": "30"}, {"text": "50"}, {"text": "100"}],
                [{"text": "150"}, {"text": "200"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    def create_reverse_choice_keyboard(self):
        """Tạo bàn phím chọn đảo chiều"""
        return {
            "keyboard": [
                [{"text": "✅ Bật đảo chiều"}, {"text": "❌ Tắt đảo chiều"}],
                [{"text": "❌ Hủy bỏ"}]
            ],
            "resize_keyboard": True,
            "one_time_keyboard": True
        }

    # ========== HÀM THÊM BOT MỚI ==========
    def add_bot(self, bot_mode, lev, percent, tp, sl, roi_trigger, 
                symbol=None, bot_count=1, **kwargs):
        """Thêm bot mới với cấu hình chi tiết"""
        if sl == 0: sl = None
            
        if not self.api_key or not self.api_secret:
            self.log("❌ API Key chưa được cài đặt trong BotManager")
            return False
        
        if not self._verify_api_connection():
            self.log("❌ KHÔNG THỂ KẾT NỐI VỚI BINANCE - KHÔNG THỂ TẠO BOT")
            return False
        
        # Lấy các tham số mới
        static_entry_mode = kwargs.get('static_entry_mode', 'signal')
        dynamic_strategy = kwargs.get('dynamic_strategy', 'volume')
        pyramiding_n = kwargs.get('pyramiding_n', 0)
        pyramiding_x = kwargs.get('pyramiding_x', 0)
        reverse_on_stop = kwargs.get('reverse_on_stop', False)
        
        created_count = 0
        
        try:
            for i in range(bot_count):
                # Tạo bot ID
                if bot_mode == 'static' and symbol:
                    bot_id = f"STATIC_{symbol}_{int(time.time())}_{i}"
                else:
                    bot_id = f"DYNAMIC_{dynamic_strategy}_{int(time.time())}_{i}"
                
                if bot_id in self.bots: continue
                
                # CHỌN LỚP BOT THEO CHIẾN LƯỢC
                if bot_mode == 'static':
                    bot_class = StaticMarketBot
                    # Thêm thông tin chế độ vào lệnh
                    extra_params = {
                        'static_entry_mode': static_entry_mode,
                        'reverse_on_stop': reverse_on_stop
                    }
                else:
                    # Bot động: chọn chiến lược
                    if dynamic_strategy == 'volume':
                        bot_class = CompoundProfitBot
                        # Gợi ý: TP lớn, không SL, có nhồi lệnh
                        if sl is None: sl = 0  # Không SL
                        if tp < 500: tp = 500  # TP tối thiểu 500%
                    else:  # volatility
                        bot_class = BalanceProtectionBot
                        # Gợi ý: SL nhỏ, TP lớn, có đảo chiều
                        if sl < 30: sl = 30  # SL tối thiểu 30%
                        if tp < 100: tp = 100  # TP tối thiểu 100%
                    
                    extra_params = {
                        'dynamic_strategy': dynamic_strategy,
                        'reverse_on_stop': reverse_on_stop
                    }
                
                # Tạo bot
                bot = bot_class(
                    symbol if bot_mode == 'static' else None,
                    lev, percent, tp, sl, roi_trigger, self.ws_manager,
                    self.api_key, self.api_secret, self.telegram_bot_token, self.telegram_chat_id,
                    coin_manager=self.coin_manager, symbol_locks=self.symbol_locks,
                    bot_coordinator=self.bot_coordinator, bot_id=bot_id, max_coins=1,
                    pyramiding_n=pyramiding_n, pyramiding_x=pyramiding_x,
                    **extra_params
                )
                
                bot._bot_manager = self
                self.bots[bot_id] = bot
                created_count += 1
                
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot: {str(e)}")
            return False
        
        if created_count > 0:
            # Thông tin chi tiết
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
        """Tạo bàn phím quản lý coin"""
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

    def stop_bot(self, bot_id):
        """Dừng một bot cụ thể"""
        bot = self.bots.get(bot_id)
        if bot:
            bot.stop()
            del self.bots[bot_id]
            self.log(f"🔴 Đã dừng bot {bot_id}")
            return True
        return False

    def stop_all(self):
        """Dừng tất cả bot"""
        self.log("🔴 Đang dừng tất cả bot...")
        for bot_id in list(self.bots.keys()):
            self.stop_bot(bot_id)
        self.log("🔴 Đã dừng tất cả bot, hệ thống vẫn chạy")

    # ========== LISTENER TELEGRAM MỚI ==========
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
        """Xử lý tin nhắn Telegram - LUỒNG TẠO BOT MỚI"""
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # ========== BẮT ĐẦU LUỒNG TẠO BOT MỚI ==========
        if text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_bot_type'}
            balance = get_balance(self.api_key, self.api_secret)
            if balance is None:
                send_telegram("❌ <b>LỖI KẾT NỐI BINANCE</b>\nKiểm tra API Key và mạng!", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                return
            
            send_telegram(f"🎯 <b>CHỌN LOẠI BOT</b>\n\n💰 Số dư hiện tại: <b>{balance:.2f} USDT</b>\n\nChọn loại bot:",
                         chat_id=chat_id, reply_markup=create_bot_mode_keyboard(),
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== CHỌN LOẠI BOT ==========
        elif current_step == 'waiting_bot_type':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "🤖 Bot Tĩnh - Coin cụ thể":
                user_state['bot_mode'] = 'static'
                user_state['step'] = 'waiting_static_symbol'
                
                # Lấy danh sách coin thực từ Binance
                try:
                    symbols = get_all_usdt_pairs(limit=15) or ["BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT"]
                    symbol_keyboard = []
                    row = []
                    for symbol in symbols:
                        row.append({"text": symbol})
                        if len(row) == 3:
                            symbol_keyboard.append(row)
                            row = []
                    if row: symbol_keyboard.append(row)
                    symbol_keyboard.append([{"text": "❌ Hủy bỏ"}])
                    
                    send_telegram("🤖 <b>BOT TĨNH</b>\n\nChọn coin cố định để giao dịch:",
                                chat_id=chat_id, reply_markup={"keyboard": symbol_keyboard, "resize_keyboard": True, "one_time_keyboard": True},
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except Exception as e:
                    send_telegram(f"⚠️ Lỗi lấy danh sách coin: {str(e)}\n\nNhập tên coin (ví dụ: BNBUSDT):",
                                chat_id=chat_id, reply_markup={"keyboard": [[{"text": "❌ Hủy bỏ"}]], "resize_keyboard": True, "one_time_keyboard": True},
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                
            elif text == "🔄 Bot Động - Tự tìm coin":
                user_state['bot_mode'] = 'dynamic'
                user_state['step'] = 'waiting_bot_count'
                
                send_telegram("🔄 <b>BOT ĐỘNG</b>\n\nChọn số lượng bot (mỗi bot tìm 1 coin):",
                            chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT TĨNH: CHỌN SYMBOL ==========
        elif current_step == 'waiting_static_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                # Kiểm tra symbol có hợp lệ không
                symbol = text.upper()
                if not symbol.endswith('USDT'):
                    symbol += 'USDT'
                
                user_state['symbol'] = symbol
                user_state['step'] = 'waiting_static_entry_mode'
                
                send_telegram(f"🔗 Coin: {symbol}\n\n<b>CHỌN CHẾ ĐỘ VÀO LỆNH:</b>\n\n"
                            "🎯 <b>Nghe tín hiệu</b>: Chỉ vào khi có tín hiệu RSI đúng hướng\n"
                            "🔄 <b>Đảo ngược</b>: Sau khi đóng, mở ngay lệnh đảo ngược\n"
                            "⏳ <b>Đợi hướng chuẩn</b>: Sau khi đóng, đợi hướng chuẩn rồi vào",
                            chat_id=chat_id, reply_markup=self.create_static_entry_mode_keyboard(),
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
                elif text == "🔄 Đảo ngược":
                    user_state['static_entry_mode'] = 'reverse'
                else:
                    user_state['static_entry_mode'] = 'wait'
                
                user_state['step'] = 'waiting_leverage'
                send_telegram(f"✅ Chế độ: {text}\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG: CHỌN SỐ LƯỢNG ==========
        elif current_step == 'waiting_bot_count':
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
                    user_state['step'] = 'waiting_dynamic_strategy'
                    
                    send_telegram(f"🤖 Số bot: {bot_count}\n\n<b>CHỌN CHIẾN LƯỢC TÌM COIN:</b>\n\n"
                                "📊 <b>Khối lượng</b>: Ưu tiên coin volume cao, TP lớn, không SL\n"
                                "📈 <b>Biến động</b>: Ưu tiên coin biến động mạnh, SL nhỏ, TP lớn",
                                chat_id=chat_id, reply_markup=self.create_dynamic_strategy_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho số bot:",
                                chat_id=chat_id, reply_markup=create_bot_count_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG: CHỌN CHIẾN LƯỢC ==========
        elif current_step == 'waiting_dynamic_strategy':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "📊 Khối lượng (Volume)":
                user_state['dynamic_strategy'] = 'volume'
                user_state['step'] = 'waiting_volume_tp'
                
                send_telegram("📊 <b>CHIẾN LƯỢC KHỐI LƯỢNG</b>\n\n"
                            "🎯 <b>GỢI Ý CẤU HÌNH:</b>\n"
                            "• Take Profit lớn (500-10000%)\n"
                            "• Không Stop Loss\n"
                            "• Nhồi lệnh tích cực\n\n"
                            "Chọn Take Profit (%):",
                            chat_id=chat_id, reply_markup=self.create_volume_strategy_tp_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == "📈 Biến động (Volatility)":
                user_state['dynamic_strategy'] = 'volatility'
                user_state['step'] = 'waiting_volatility_tp'
                
                send_telegram("📈 <b>CHIẾN LƯỢC BIẾN ĐỘNG</b>\n\n"
                            "🎯 <b>GỢI Ý CẤU HÌNH:</b>\n"
                            "• Stop Loss nhỏ (30-200%)\n"
                            "• Take Profit lớn (100-1000%)\n"
                            "• Có đảo chiều khi cắt lỗ\n\n"
                            "Chọn Take Profit (%):",
                            chat_id=chat_id, reply_markup=self.create_volatility_strategy_tp_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG KHỐI LƯỢNG: CHỌN TP ==========
        elif current_step == 'waiting_volume_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            elif text == '❌ Tắt SL':
                user_state['tp'] = 1000  # Mặc định
                user_state['sl'] = None  # Không SL
                user_state['step'] = 'waiting_leverage'
                
                send_telegram(f"📊 Take Profit: {user_state['tp']}%\n🛡️ Stop Loss: Tắt\n\nChọn đòn bẩy:",
                            chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp < 100:
                        send_telegram("⚠️ TP phải ≥100% cho chiến lược khối lượng. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=self.create_volume_strategy_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['tp'] = tp
                    user_state['sl'] = None  # Không SL cho chiến lược volume
                    user_state['step'] = 'waiting_leverage'
                    
                    send_telegram(f"📊 Take Profit: {tp}%\n🛡️ Stop Loss: Tắt\n\nChọn đòn bẩy:",
                                chat_id=chat_id, reply_markup=create_leverage_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=self.create_volume_strategy_tp_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG BIẾN ĐỘNG: CHỌN TP ==========
        elif current_step == 'waiting_volatility_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id=chat_id, reply_markup=create_main_menu(),
                            bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            else:
                try:
                    tp = float(text)
                    if tp < 50:
                        send_telegram("⚠️ TP phải ≥50% cho chiến lược biến động. Vui lòng chọn:",
                                    chat_id=chat_id, reply_markup=self.create_volatility_strategy_tp_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['tp'] = tp
                    user_state['step'] = 'waiting_volatility_sl'
                    
                    send_telegram(f"🎯 Take Profit: {tp}%\n\nChọn Stop Loss (%):",
                                chat_id=chat_id, reply_markup=self.create_volatility_strategy_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Take Profit:",
                                chat_id=chat_id, reply_markup=self.create_volatility_strategy_tp_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG BIẾN ĐỘNG: CHỌN SL ==========
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
                                    chat_id=chat_id, reply_markup=self.create_volatility_strategy_sl_keyboard(),
                                    bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                        return
                    
                    user_state['sl'] = sl
                    user_state['step'] = 'waiting_volatility_reverse'
                    
                    send_telegram(f"🛡️ Stop Loss: {sl}%\n\nBật đảo chiều khi cắt lỗ?",
                                chat_id=chat_id, reply_markup=self.create_reverse_choice_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                except ValueError:
                    send_telegram("⚠️ Vui lòng nhập số hợp lệ cho Stop Loss:",
                                chat_id=chat_id, reply_markup=self.create_volatility_strategy_sl_keyboard(),
                                bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        # ========== BOT ĐỘNG BIẾN ĐỘNG: CHỌN ĐẢO CHIỀU ==========
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
        
        # ========== CÁC BƯỚC CHUNG: ĐÒN BẨY ==========
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
        
        # ========== CÁC BƯỚC CHUNG: % SỐ DƯ ==========
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
                    
                    # Kiểm tra nếu đã có TP/SL từ chiến lược
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
        
        # ========== BOT TĨNH: CHỌN TP ==========
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
        
        # ========== BOT TĨNH: CHỌN SL ==========
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
        
        # ========== CHỌN SỐ LẦN NHỒI LỆNH ==========
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
        
        # ========== CHỌN MỐC ROI NHỒI LỆNH ==========
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
        
        # ========== CHỌN NGƯỠNG ROI THOÁT THÔNG MINH ==========
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
        
        # ========== CÁC LỆNH QUẢN LÝ KHÁC ==========
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
                    # Lấy thêm thông tin tổng số dư
                    total_balance, available_balance = get_total_and_available_balance(self.api_key, self.api_secret)
                    margin_balance, maint_margin, ratio = get_margin_safety_info(self.api_key, self.api_secret)
                    
                    message = f"💰 <b>THÔNG TIN SỐ DƯ THỰC</b>\n\n"
                    message += f"💳 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT\n"
                    
                    if total_balance is not None:
                        message += f"📊 <b>TỔNG SỐ DƯ (USDT+USDC)</b>: {total_balance:.2f} USDT\n"
                        message += f"💵 <b>KHẢ DỤNG (USDT+USDC)</b>: {available_balance:.2f} USDT\n"
                    
                    if margin_balance is not None and ratio is not None:
                        message += f"\n🛡️ <b>AN TOÀN KÝ QUỸ THỰC</b>\n"
                        message += f"• Margin Balance: {margin_balance:.2f}\n"
                        message += f"• Maint Margin: {maint_margin:.2f}\n"
                        message += f"• Tỷ lệ an toàn: {ratio:.2f}x\n"
                        message += f"• Ngưỡng cảnh báo: {1.15}x"
                    
                    send_telegram(message, chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi số dư: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "📈 Vị thế":
            try:
                positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)
                if not positions:
                    send_telegram("📭 Không có vị thế mở trên Binance", chat_id=chat_id,
                                 bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
                    return
                
                message = "📈 <b>VỊ THẾ THỰC TRÊN BINANCE</b>\n\n"
                total_pnl = 0
                for pos in positions:
                    position_amt = float(pos.get('positionAmt', 0))
                    if position_amt != 0:
                        symbol = pos.get('symbol', 'UNKNOWN')
                        entry = float(pos.get('entryPrice', 0))
                        side = "LONG" if position_amt > 0 else "SHORT"
                        pnl = float(pos.get('unRealizedProfit', 0))
                        leverage = float(pos.get('leverage', 1))
                        total_pnl += pnl
                        
                        message += (f"🔹 {symbol} | {side} | {leverage}x\n"
                                  f"📊 Khối lượng: {abs(position_amt):.4f}\n"
                                  f"🏷️ Entry: {entry:.4f}\n"
                                  f"💰 PnL: {pnl:.2f} USDT\n\n")
                
                message += f"📊 <b>TỔNG PnL THỰC: {total_pnl:.2f} USDT</b>"
                
                send_telegram(message, chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi vị thế: {str(e)}", chat_id=chat_id,
                             bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "🎯 Chiến lược":
            strategy_info = (
                "🎯 <b>HỆ THỐNG BOT THÔNG MINH - DỮ LIỆU THỰC</b>\n\n"
                
                "🤖 <b>LUỒNG TẠO BOT MỚI:</b>\n"
                "1. Chọn loại bot (Tĩnh/Động)\n"
                "2. Tĩnh: Chọn coin cố định\n"
                "3. Động: Chọn chiến lược tìm coin\n"
                "4. Cấu hình tham số giao dịch\n\n"
                
                "📊 <b>DỮ LIỆU THỰC TỪ BINANCE:</b>\n"
                "• Top coin theo khối lượng thực\n"
                "• Top coin theo biến động thực\n"
                "• Tín hiệu RSI + Volume thực\n"
                "• Số dư và giá thực\n\n"
                
                "🔄 <b>CƠ CHẾ HÀNG ĐỢI (FIFO):</b>\n"
                "• Chỉ 1 bot tìm coin tại một thời điểm\n"
                "• Bot vào lệnh → bot tiếp theo tìm NGAY\n"
                "• Bot có coin không thể vào hàng đợi\n"
                "• Bot đóng lệnh có thể vào lại hàng đợi\n\n"
                
                "⚡ <b>TÍNH NĂNG NHỒI LỆNH THỰC:</b>\n"
                "• Nhồi lệnh cùng chiều khi đạt mốc ROI âm\n"
                "• Số lần nhồi và mốc ROI tùy chỉnh\n"
                "• Tự động cập nhật giá trung bình\n\n"
                
                "🎯 <b>TÍN HIỆU THÔNG MINH THỰC:</b>\n"
                "• Phân tích RSI + Volume thời gian thực\n"
                "• 6 điều kiện vào/thoát lệnh\n"
                "• Kết hợp ROI + tín hiệu để thoát tối ưu"
            )
            send_telegram(strategy_info, chat_id=chat_id,
                         bot_token=self.telegram_bot_token, default_chat_id=self.telegram_chat_id)
        
        elif text == "⚙️ Cấu hình":
            balance = get_balance(self.api_key, self.api_secret)
            api_status = "✅ Đã kết nối" if balance is not None else "❌ Lỗi kết nối"
            
            total_bots_with_coins, trading_bots = 0, 0
            static_bots, dynamic_bots = 0, 0
            volume_bots, volatility_bots = 0, 0
            pyramiding_bots = 0
            
            for bot in self.bots.values():
                if hasattr(bot, 'symbol') and bot.symbol:
                    static_bots += 1
                else:
                    dynamic_bots += 1
                    
                    if hasattr(bot, 'dynamic_strategy'):
                        if bot.dynamic_strategy == 'volume':
                            volume_bots += 1
                        else:
                            volatility_bots += 1
                
                if hasattr(bot, 'active_symbols'):
                    if len(bot.active_symbols) > 0: total_bots_with_coins += 1
                    for symbol, data in bot.symbol_data.items():
                        if data.get('position_open', False): trading_bots += 1
                
                if hasattr(bot, 'pyramiding_enabled') and bot.pyramiding_enabled:
                    pyramiding_bots += 1
            
            config_info = (f"⚙️ <b>CẤU HÌNH HỆ THỐNG THỰC</b>\n\n"
                          f"🔑 Binance API: {api_status}\n\n"
                          f"🎯 <b>PHÂN BỐ BOT THỰC:</b>\n"
                          f"🤖 Bot tĩnh: {static_bots}\n"
                          f"🔄 Bot động: {dynamic_bots}\n"
                          f"   📊 Khối lượng: {volume_bots}\n"
                          f"   📈 Biến động: {volatility_bots}\n\n"
                          f"🤖 <b>TỔNG SỐ BOT:</b> {len(self.bots)}\n"
                          f"📊 Bot có coin: {total_bots_with_coins}\n"
                          f"🟢 Bot đang giao dịch: {trading_bots}\n"
                          f"🔄 Bot có nhồi lệnh: {pyramiding_bots}\n\n"
                          f"🌐 WebSocket: {len(self.ws_manager.connections)} kết nối\n"
                          f"🔄 Cooldown: 1s\n📋 Hàng đợi: {self.bot_coordinator.get_queue_info()['queue_size']} bot")
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
            
            # Thêm các tham số mới
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
                # Thông tin chi tiết
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
