# trading_bot_lib_part2.py
# PHẦN 2: THƯ VIỆN BASEBOT VỚI DATABASE TÍCH HỢP
from trading_bot_lib_part1 import (
    logger, get_all_usdt_pairs, get_max_leverage, get_step_size,
    set_leverage, get_total_and_available_balance, get_margin_safety_info,
    place_order, cancel_all_orders, get_current_price, get_positions,
    CoinManager, BotExecutionCoordinator, SmartCoinFinder, WebSocketManager,
    send_telegram, get_top_volume_symbols, get_high_volatility_symbols,
    db_manager
)

import time
import threading
import math
import random
from datetime import datetime
from collections import defaultdict

# ========== LỚP BOT CƠ SỞ VỚI DATABASE ==========
class BaseBot:
    """Lớp cơ sở cho tất cả các bot với tích hợp database"""
    
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager, api_key, api_secret,
                 telegram_bot_token, telegram_chat_id, strategy_name, config_key=None, bot_id=None,
                 coin_manager=None, symbol_locks=None, max_coins=1, bot_coordinator=None,
                 pyramiding_n=0, pyramiding_x=0, bot_type="balance_protection",  
                 dynamic_strategy="volume", reverse_on_stop=False, static_entry_mode="signal"):
        
        self.bot_type = bot_type
        self.dynamic_strategy = dynamic_strategy  # "volume" hoặc "volatility"
        self.reverse_on_stop = reverse_on_stop
        self.static_entry_mode = static_entry_mode  # "signal", "reverse", "wait"
        
        self.max_coins = 1
        self.active_symbols = []
        self.symbol_data = {}
        self.symbol = symbol.upper() if symbol else None

        self.lev = lev
        self.percent = percent
        self.tp = tp
        self.sl = sl
        self.roi_trigger = roi_trigger
        self.ws_manager = ws_manager
        self.api_key = api_key
        self.api_secret = api_secret
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.strategy_name = strategy_name
        self.config_key = config_key
        self.bot_id = bot_id or f"{bot_type}_{dynamic_strategy}_{int(time.time())}_{random.randint(1000, 9999)}"

        # Thông tin nhồi lệnh
        self.pyramiding_n = int(pyramiding_n) if pyramiding_n else 0
        self.pyramiding_x = float(pyramiding_x) if pyramiding_x else 0
        self.pyramiding_enabled = self.pyramiding_n > 0 and self.pyramiding_x > 0

        self.status = "searching" if not symbol else "waiting"
        self._stop = False

        self.current_processing_symbol = None
        self.last_trade_completion_time = 0
        self.trade_cooldown = 30

        self.last_global_position_check = 0
        self.last_error_log_time = 0
        self.global_position_check_interval = 30

        self.global_long_count = 0
        self.global_short_count = 0
        self.global_long_pnl = 0
        self.global_short_pnl = 0
        self.global_long_volume = 0.0
        self.global_short_volume = 0.0
        self.next_global_side = None

        self.margin_safety_threshold = 1.15
        self.margin_safety_interval = 10
        self.last_margin_safety_check = 0

        # Ngưỡng cân bằng
        self.volume_imbalance_threshold = 0.1

        self.coin_manager = coin_manager or CoinManager()
        self.symbol_locks = symbol_locks
        self.coin_finder = SmartCoinFinder(api_key, api_secret)

        self.find_new_bot_after_close = True
        self.bot_creation_time = time.time()

        self.execution_lock = threading.Lock()
        self.last_execution_time = 0
        self.execution_cooldown = 1

        self.bot_coordinator = bot_coordinator or BotExecutionCoordinator()

        # Lưu cấu hình bot vào database
        self._save_bot_config_to_db()
        
        # Khôi phục vị thế từ database khi khởi động
        self._restore_positions_from_db()

        if symbol and not self.coin_finder.has_existing_position(symbol):
            self._add_symbol(symbol)
        
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

        # Log khởi động
        roi_info = f" | 🎯 ROI Kích hoạt: {roi_trigger}%" if roi_trigger else " | 🎯 ROI Kích hoạt: Tắt"
        pyramiding_info = f" | 🔄 Nhồi lệnh: {pyramiding_n} lần tại {pyramiding_x}%" if self.pyramiding_enabled else " | 🔄 Nhồi lệnh: Tắt"
        strategy_info = f" | 📊 Chiến lược: {dynamic_strategy}" if dynamic_strategy else ""
        
        if symbol:
            self.log(f"🟢 Bot {strategy_name} đã khởi động | 🤖 Tĩnh | {strategy_info} | Coin: {symbol} | Đòn bẩy: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}{pyramiding_info}")
        else:
            self.log(f"🟢 Bot {strategy_name} đã khởi động | 🔄 Động | {strategy_info} | 1 coin | Đòn bẩy: {lev}x | Vốn: {percent}% | TP/SL: {tp}%/{sl}%{roi_info}{pyramiding_info}")

    # ========== DATABASE METHODS ==========
    
    def _save_bot_config_to_db(self):
        """Lưu cấu hình bot vào database"""
        bot_data = {
            'bot_id': self.bot_id,
            'bot_mode': 'static' if self.symbol else 'dynamic',
            'bot_type': self.strategy_name,
            'symbol': self.symbol,
            'leverage': self.lev,
            'percent': self.percent,
            'tp': self.tp,
            'sl': self.sl,
            'roi_trigger': self.roi_trigger,
            'pyramiding_n': self.pyramiding_n,
            'pyramiding_x': self.pyramiding_x,
            'dynamic_strategy': self.dynamic_strategy,
            'static_entry_mode': self.static_entry_mode,
            'reverse_on_stop': self.reverse_on_stop,
            'telegram_chat_id': self.telegram_chat_id,
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'status': 'running'
        }
        
        if db_manager.save_bot_config(bot_data):
            logger.info(f"✅ Đã lưu cấu hình bot {self.bot_id} vào database")
        else:
            logger.error(f"❌ Lỗi lưu cấu hình bot {self.bot_id} vào database")
    
    def _restore_positions_from_db(self):
        """Khôi phục vị thế từ database khi khởi động lại"""
        try:
            positions = db_manager.get_open_positions(self.bot_id)
            
            for pos in positions:
                symbol = pos['symbol']
                if symbol not in self.active_symbols:
                    self._add_symbol(symbol)
                
                # Cập nhật thông tin vị thế từ database
                self.symbol_data[symbol] = {
                    'status': 'open',
                    'side': pos['side'],
                    'qty': pos['quantity'] * (1 if pos['side'] == 'BUY' else -1),
                    'entry': pos['entry_price'],
                    'current_price': pos['current_price'] or get_current_price(symbol),
                    'position_open': True,
                    'last_trade_time': time.time(),
                    'last_close_time': 0,
                    'entry_base': pos['entry_price'],
                    'average_down_count': 0,
                    'last_average_down_time': 0,
                    'high_water_mark_roi': pos['roi'] or 0,
                    'roi_check_activated': pos['roi'] and pos['roi'] >= self.roi_trigger if self.roi_trigger else False,
                    'close_attempted': False,
                    'last_close_attempt': 0,
                    'last_position_check': time.time(),
                    'pyramiding_count': pos['pyramiding_count'],
                    'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
                    'last_pyramiding_time': 0,
                    'pyramiding_base_roi': 0.0,
                }
                
                self.coin_manager.register_coin(symbol, self.bot_id)
                self.bot_coordinator.bot_has_coin(self.bot_id)
                
                self.log(f"✅ Đã khôi phục vị thế {symbol} từ database")
                
        except Exception as e:
            self.log(f"❌ Lỗi khôi phục vị thế từ database: {str(e)}")
    
    def _save_position_to_db(self, symbol, action="open"):
        """Lưu vị thế vào database"""
        if symbol not in self.symbol_data:
            return
        
        symbol_info = self.symbol_data[symbol]
        
        if action == "open" and symbol_info['position_open']:
            position_data = {
                'bot_id': self.bot_id,
                'symbol': symbol,
                'side': symbol_info['side'],
                'entry_price': symbol_info['entry'],
                'quantity': abs(symbol_info['qty']),
                'current_price': symbol_info.get('current_price', 0),
                'roi': 0,
                'tp_price': symbol_info['entry'] * (1 + self.tp/100) if symbol_info['side'] == 'BUY' else symbol_info['entry'] * (1 - self.tp/100),
                'sl_price': symbol_info['entry'] * (1 - self.sl/100) if symbol_info['side'] == 'BUY' else symbol_info['entry'] * (1 + self.sl/100),
                'pyramiding_count': symbol_info.get('pyramiding_count', 0),
                'status': 'open'
            }
            
            if db_manager.save_position(position_data):
                logger.debug(f"✅ Đã lưu vị thế {symbol} vào database")
    
    def _save_trade_history(self, symbol, side, price, quantity, pnl=None, roi=None, reason=""):
        """Lưu lịch sử giao dịch vào database"""
        trade_data = {
            'bot_id': self.bot_id,
            'symbol': symbol,
            'side': side,
            'price': price,
            'quantity': quantity,
            'pnl': pnl,
            'roi': roi,
            'reason': reason
        }
        
        if db_manager.save_trade_history(trade_data):
            # Cập nhật thống kê
            if pnl is not None:
                is_win = pnl > 0
                db_manager.update_statistics(self.bot_id, pnl, is_win)
            
            logger.debug(f"✅ Đã lưu lịch sử giao dịch {symbol} vào database")
    
    def _update_position_in_db(self, symbol, updates):
        """Cập nhật thông tin vị thế trong database"""
        try:
            if symbol not in self.symbol_data or not self.symbol_data[symbol]['position_open']:
                return
            
            symbol_info = self.symbol_data[symbol]
            
            # Tính ROI hiện tại
            current_price = self.get_current_price(symbol)
            if current_price > 0:
                if symbol_info['side'] == "BUY":
                    profit = (current_price - symbol_info['entry']) * abs(symbol_info['qty'])
                else:
                    profit = (symbol_info['entry'] - current_price) * abs(symbol_info['qty'])
                    
                invested = symbol_info['entry'] * abs(symbol_info['qty']) / self.lev
                if invested > 0:
                    current_roi = (profit / invested) * 100
                else:
                    current_roi = 0
            else:
                current_roi = 0
            
            query = """
            UPDATE bot_positions 
            SET current_price = %s, 
                roi = %s,
                pyramiding_count = %s,
                last_update = CURRENT_TIMESTAMP
            WHERE bot_id = %s AND symbol = %s AND status = 'open'
            """
            
            db_manager.execute_query(query, (
                current_price,
                current_roi,
                symbol_info.get('pyramiding_count', 0),
                self.bot_id,
                symbol
            ))
            
        except Exception as e:
            logger.error(f"❌ Lỗi cập nhật vị thế {symbol} trong database: {str(e)}")
    
    # ========== CÁC HÀM CHUNG ==========
    
    def _run(self):
        """Vòng lặp chính với database"""
        while not self._stop:
            try:
                current_time = time.time()

                # KIỂM TRA AN TOÀN KÝ QUỸ
                if current_time - self.last_margin_safety_check > self.margin_safety_interval:
                    self.last_margin_safety_check = current_time
                    if self._check_margin_safety():
                        time.sleep(5)
                        continue
                
                # KIỂM TRA VỊ THẾ TOÀN TÀI KHOẢN
                if current_time - self.last_global_position_check > 30:
                    self.check_global_positions()
                    self.last_global_position_check = current_time
                
                # NẾU BOT KHÔNG CÓ COIN NÀO - YÊU CẦU TÌM COIN
                if not self.active_symbols:
                    if self.symbol:
                        # Bot tĩnh có symbol cố định
                        if self.symbol not in self.active_symbols:
                            if not self.coin_finder.has_existing_position(self.symbol):
                                self._add_symbol(self.symbol)
                        time.sleep(5)
                        continue
                    
                    # BOT ĐỘNG: YÊU CẦU TÌM COIN
                    search_permission = self.bot_coordinator.request_coin_search(self.bot_id)
                    
                    if search_permission:
                        queue_info = self.bot_coordinator.get_queue_info()
                        self.log(f"🔍 Đang tìm coin (vị trí: 1/{queue_info['queue_size'] + 1})...")
                        
                        found_coin = self._find_and_add_new_coin()
                        
                        if found_coin:
                            self.bot_coordinator.bot_has_coin(self.bot_id)
                            self.log(f"✅ Đã tìm thấy coin: {found_coin}, đang chờ vào lệnh...")
                        else:
                            self.bot_coordinator.finish_coin_search(self.bot_id)
                            self.log(f"❌ Không tìm thấy coin phù hợp")
                    else:
                        queue_pos = self.bot_coordinator.get_queue_position(self.bot_id)
                        if queue_pos > 0:
                            queue_info = self.bot_coordinator.get_queue_info()
                            current_finder = queue_info['current_finding']
                            self.log(f"⏳ Đang chờ tìm coin (vị trí: {queue_pos}/{queue_info['queue_size'] + 1}) - Bot đang tìm: {current_finder}")
                        time.sleep(2)
                
                # XỬ LÝ COIN HIỆN TẠI VÀ CẬP NHẬT DATABASE
                for symbol in self.active_symbols.copy():
                    position_opened = self._process_single_symbol(symbol)
                    
                    if position_opened:
                        self.log(f"🎯 Đã vào lệnh thành công {symbol}, chuyển quyền tìm coin...")
                        next_bot = self.bot_coordinator.finish_coin_search(self.bot_id)
                        if next_bot:
                            self.log(f"🔄 Đã chuyển quyền tìm coin cho bot: {next_bot}")
                        break
                
                time.sleep(1)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"❌ Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(5)

    def _process_single_symbol(self, symbol):
        """Xử lý một symbol với cập nhật database"""
        try:
            symbol_info = self.symbol_data[symbol]
            current_time = time.time()
            
            # Kiểm tra vị thế định kỳ và cập nhật database
            if current_time - symbol_info.get('last_position_check', 0) > 30:
                self._check_symbol_position(symbol)
                symbol_info['last_position_check'] = current_time
                
                # Cập nhật thông tin vị thế vào database
                self._update_position_in_db(symbol, {})
            
            # Xử lý theo trạng thái
            if symbol_info['position_open']:
                # BOT TĨNH
                if self.symbol:
                    self._check_symbol_tp_sl(symbol)
                    
                    if self.pyramiding_enabled:
                        self._check_pyramiding(symbol)
                    
                    return False
                
                # BOT ĐỘNG
                exit_triggered = False
                
                if self.dynamic_strategy == "volume":
                    exit_triggered = self._check_smart_exit_condition(symbol)
                else:
                    exit_triggered = self._check_early_reversal(symbol)
                
                if exit_triggered:
                    return False
                
                self._check_symbol_tp_sl(symbol)
                
                if self.pyramiding_enabled:
                    self._check_pyramiding(symbol)
                    
                return False
            else:
                # Tìm cơ hội vào lệnh
                if (current_time - symbol_info['last_trade_time'] > 30 and 
                    current_time - symbol_info['last_close_time'] > 30):
                    
                    # BOT TĨNH
                    if self.symbol:
                        return self._process_static_entry(symbol)
                    
                    # BOT ĐỘNG
                    return self._process_dynamic_entry(symbol)
                
                return False
                
        except Exception as e:
            self.log(f"❌ Lỗi xử lý {symbol}: {str(e)}")
            return False

    def _process_static_entry(self, symbol):
        """Xử lý vào lệnh cho bot tĩnh"""
        try:
            entry_signal = self.coin_finder.get_entry_signal(symbol)
            
            if not entry_signal:
                return False
            
            if self.static_entry_mode == "signal":
                target_side = entry_signal
            elif self.static_entry_mode == "reverse":
                self.check_global_positions()
                target_side = self._get_reverse_side()
            else:  # "wait"
                target_side = entry_signal
            
            if target_side in ["BUY", "SELL"]:
                if not self.coin_finder.has_existing_position(symbol):
                    if self._open_symbol_position(symbol, target_side):
                        self.symbol_data[symbol]['last_trade_time'] = time.time()
                        return True
            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi xử lý bot tĩnh {symbol}: {str(e)}")
            return False

    def _process_dynamic_entry(self, symbol):
        """Xử lý vào lệnh cho bot động"""
        try:
            entry_signal = self.coin_finder.get_entry_signal(symbol)
            
            if not entry_signal:
                return False
            
            if self.dynamic_strategy == "volume":
                target_side = self._get_side_for_volume_strategy()
            else:
                target_side = self._get_side_for_volatility_strategy()
            
            if entry_signal == target_side:
                if not self.coin_finder.has_existing_position(symbol):
                    if self._open_symbol_position(symbol, target_side):
                        self.symbol_data[symbol]['last_trade_time'] = time.time()
                        return True
            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi xử lý bot động {symbol}: {str(e)}")
            return False

    def _get_reverse_side(self):
        """Lấy hướng đảo ngược"""
        if self.next_global_side:
            return "SELL" if self.next_global_side == "BUY" else "BUY"
        return random.choice(["BUY", "SELL"])

    def _get_side_for_volume_strategy(self):
        """Lấy hướng cho chiến lược khối lượng"""
        self.check_global_positions()
        
        if self.next_global_side in ["BUY", "SELL"]:
            return self.next_global_side
        
        if self.global_long_volume > 0 or self.global_short_volume > 0:
            diff = abs(self.global_long_volume - self.global_short_volume)
            total = self.global_long_volume + self.global_short_volume
            
            if total > 0:
                imbalance = diff / total
                
                if imbalance > self.volume_imbalance_threshold:
                    if self.global_long_volume > self.global_short_volume:
                        return "SELL"
                    else:
                        return "BUY"
        
        return random.choice(["BUY", "SELL"])

    def _get_side_for_volatility_strategy(self):
        """Lấy hướng cho chiến lược biến động"""
        self.check_global_positions()
        
        if self.next_global_side in ["BUY", "SELL"]:
            return self.next_global_side
        
        return random.choice(["SELL", "BUY"])

    def _check_early_reversal(self, symbol):
        """Kiểm tra điều kiện đảo chiều sớm"""
        try:
            if not self.symbol_data[symbol]['position_open']:
                return False
            
            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                return False
            
            entry = float(self.symbol_data[symbol]['entry'])
            side = self.symbol_data[symbol]['side']
            
            if side == "BUY":
                profit = (current_price - entry) * abs(self.symbol_data[symbol]['qty'])
            else:
                profit = (entry - current_price) * abs(self.symbol_data[symbol]['qty'])
            
            invested = entry * abs(self.symbol_data[symbol]['qty']) / self.lev
            if invested <= 0:
                return False
            
            current_roi = (profit / invested) * 100
            
            if current_roi <= -50 and self.reverse_on_stop:
                reversal_signal = self.coin_finder.get_rsi_signal(symbol, volume_threshold=20)
                
                if reversal_signal:
                    if (side == "BUY" and reversal_signal == "SELL") or \
                       (side == "SELL" and reversal_signal == "BUY"):
                        
                        reason = f"🔄 Đảo chiều sớm (ROI: {current_roi:.2f}% + Tín hiệu đảo chiều)"
                        self.log(f"⚠️ {symbol} - Kích hoạt đảo chiều: {reason}")
                        
                        self._close_symbol_position(symbol, reason)
                        
                        time.sleep(2)
                        new_side = "SELL" if side == "BUY" else "BUY"
                        self._open_symbol_position(symbol, new_side)
                        
                        return True
            
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra đảo chiều {symbol}: {str(e)}")
            return False

    def _check_smart_exit_condition(self, symbol):
        """Kiểm tra điều kiện thoát thông minh"""
        try:
            if not self.symbol_data[symbol]['position_open'] or not self.symbol_data[symbol]['roi_check_activated']:
                return False
            
            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                return False
            
            if self.symbol_data[symbol]['side'] == "BUY":
                profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
            else:
                profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                
            invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
            if invested <= 0:
                return False
                
            current_roi = (profit / invested) * 100
            
            if current_roi >= self.roi_trigger:
                exit_signal = self.coin_finder.get_exit_signal(symbol)
                if exit_signal:
                    reason = f"🎯 Đạt ROI {self.roi_trigger}% + Tín hiệu thoát (ROI: {current_roi:.2f}%)"
                    self._close_symbol_position(symbol, reason)
                    return True
            return False
            
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra thoát thông minh {symbol}: {str(e)}")
            return False

    def _find_and_add_new_coin(self):
        """Tìm và thêm coin mới"""
        try:
            active_coins = self.coin_manager.get_active_coins()
            
            if self.dynamic_strategy == "volume":
                new_symbol = self.coin_finder.find_best_coin_by_volume(active_coins, self.lev)
            else:
                new_symbol = self.coin_finder.find_best_coin_by_volatility(active_coins, self.lev)
            
            if new_symbol and self.bot_coordinator.is_coin_available(new_symbol):
                if self.coin_finder.has_existing_position(new_symbol):
                    return None
                    
                success = self._add_symbol(new_symbol)
                if success:
                    time.sleep(1)
                    if self.coin_finder.has_existing_position(new_symbol):
                        self.log(f"🚫 {new_symbol} - PHÁT HIỆN CÓ VỊ THẾ SAU KHI THÊM, DỪNG THEO DÕI NGAY")
                        self.stop_symbol(new_symbol)
                        return None
                        
                    return new_symbol
            
            return None
                
        except Exception as e:
            self.log(f"❌ Lỗi tìm coin mới: {str(e)}")
            return None

    # ========== CÁC HÀM HỖ TRỢ CHUNG ==========

    def _add_symbol(self, symbol):
        """Thêm symbol vào quản lý với database"""
        if symbol in self.active_symbols or len(self.active_symbols) >= self.max_coins:
            return False
        if self.coin_finder.has_existing_position(symbol):
            return False
        
        self.symbol_data[symbol] = {
            'status': 'waiting', 'side': '', 'qty': 0, 'entry': 0, 'current_price': 0,
            'position_open': False, 'last_trade_time': 0, 'last_close_time': 0,
            'entry_base': 0, 'average_down_count': 0, 'last_average_down_time': 0,
            'high_water_mark_roi': 0, 'roi_check_activated': False,
            'close_attempted': False, 'last_close_attempt': 0, 'last_position_check': 0,
            'pyramiding_count': 0,
            'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
            'last_pyramiding_time': 0,
            'pyramiding_base_roi': 0.0,
        }
        
        self.active_symbols.append(symbol)
        self.coin_manager.register_coin(symbol, self.bot_id)
        self.ws_manager.add_symbol(symbol, lambda price, sym=symbol: self._handle_price_update(price, sym))
        
        self._check_symbol_position(symbol)
        if self.symbol_data[symbol]['position_open']:
            self.stop_symbol(symbol)
            return False
        return True

    def _handle_price_update(self, price, symbol):
        """Xử lý cập nhật giá từ WebSocket"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol]['current_price'] = price

    def get_current_price(self, symbol):
        """Lấy giá hiện tại"""
        if (symbol in self.ws_manager.price_cache and 
            time.time() - self.ws_manager.last_price_update.get(symbol, 0) < 5):
            return self.ws_manager.price_cache[symbol]
        return get_current_price(symbol)

    def _check_symbol_position(self, symbol):
        """Kiểm tra và cập nhật thông tin vị thế"""
        try:
            # Kiểm tra trong database trước
            db_positions = db_manager.get_open_positions(self.bot_id)
            position_found_in_db = False
            
            for pos in db_positions:
                if pos['symbol'] == symbol:
                    position_found_in_db = True
                    if pos['status'] == 'open':
                        self.symbol_data[symbol]['position_open'] = True
                        self.symbol_data[symbol]['status'] = "open"
                        self.symbol_data[symbol]['side'] = pos['side']
                        self.symbol_data[symbol]['qty'] = pos['quantity'] * (1 if pos['side'] == 'BUY' else -1)
                        self.symbol_data[symbol]['entry'] = pos['entry_price']
                        
                        if self.pyramiding_enabled:
                            self.symbol_data[symbol]['pyramiding_count'] = pos['pyramiding_count']
                            self.symbol_data[symbol]['next_pyramiding_roi'] = self.pyramiding_x
                        
                        current_price = self.get_current_price(symbol)
                        if current_price > 0:
                            if self.symbol_data[symbol]['side'] == "BUY":
                                profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
                            else:
                                profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                                
                            invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
                            if invested > 0:
                                current_roi = (profit / invested) * 100
                                if current_roi >= self.roi_trigger:
                                    self.symbol_data[symbol]['roi_check_activated'] = True
                        break
                    else:
                        self._reset_symbol_position(symbol)
                        break
            
            if not position_found_in_db:
                # Kiểm tra trên Binance
                positions = get_positions(symbol, self.api_key, self.api_secret)
                if not positions:
                    self._reset_symbol_position(symbol)
                    return
                
                for pos in positions:
                    if pos['symbol'] == symbol:
                        position_amt = float(pos.get('positionAmt', 0))
                        if abs(position_amt) > 0:
                            self.symbol_data[symbol]['position_open'] = True
                            self.symbol_data[symbol]['status'] = "open"
                            self.symbol_data[symbol]['side'] = "BUY" if position_amt > 0 else "SELL"
                            self.symbol_data[symbol]['qty'] = position_amt
                            self.symbol_data[symbol]['entry'] = float(pos.get('entryPrice', 0))
                            
                            # Lưu vào database
                            self._save_position_to_db(symbol, "open")
                            
                            if self.pyramiding_enabled:
                                self.symbol_data[symbol]['pyramiding_count'] = 0
                                self.symbol_data[symbol]['next_pyramiding_roi'] = self.pyramiding_x
                            
                            current_price = self.get_current_price(symbol)
                            if current_price > 0:
                                if self.symbol_data[symbol]['side'] == "BUY":
                                    profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
                                else:
                                    profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                                    
                                invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
                                if invested > 0:
                                    current_roi = (profit / invested) * 100
                                    if current_roi >= self.roi_trigger:
                                        self.symbol_data[symbol]['roi_check_activated'] = True
                            break
                        else:
                            self._reset_symbol_position(symbol)
                            break
            
        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra vị thế {symbol}: {str(e)}")

    def _reset_symbol_position(self, symbol):
        """Reset thông tin vị thế"""
        if symbol in self.symbol_data:
            self.symbol_data[symbol].update({
                'position_open': False, 'status': "waiting", 'side': "", 'qty': 0, 'entry': 0,
                'close_attempted': False, 'last_close_attempt': 0, 'entry_base': 0,
                'average_down_count': 0, 'high_water_mark_roi': 0, 'roi_check_activated': False,
                'pyramiding_count': 0,
                'next_pyramiding_roi': self.pyramiding_x if self.pyramiding_enabled else 0,
                'last_pyramiding_time': 0,
                'pyramiding_base_roi': 0.0,   
            })

    def _open_symbol_position(self, symbol, side):
        """Mở vị thế mới với database"""
        try:
            if self.coin_finder.has_existing_position(symbol):
                self.log(f"⚠️ {symbol} - CÓ VỊ THẾ TRÊN BINANCE, BỎ QUA")
                self.stop_symbol(symbol)
                return False

            self._check_symbol_position(symbol)
            if self.symbol_data[symbol]['position_open']:
                return False

            current_leverage = self.coin_finder.get_symbol_leverage(symbol)
            if current_leverage < self.lev:
                self.log(f"❌ {symbol} - Đòn bẩy không đủ: {current_leverage}x < {self.lev}x")
                self.stop_symbol(symbol)
                return False

            if not set_leverage(symbol, self.lev, self.api_key, self.api_secret):
                self.log(f"❌ {symbol} - Không thể cài đặt đòn bẩy")
                self.stop_symbol(symbol)
                return False

            balance, available_balance = get_total_and_available_balance(self.api_key, self.api_secret)
            
            if balance is None or balance <= 0:
                self.log(f"❌ {symbol} - Không đủ số dư")
                return False
    
            required_usd = balance * (self.percent / 100)
    
            if available_balance is None or available_balance <= 0 or required_usd > available_balance:
                self.log(f"❌ {symbol} - Không đủ số dư khả dụng: cần {required_usd:.2f}, khả dụng {available_balance or 0:.2f}")
                return False

            current_price = self.get_current_price(symbol)
            if current_price <= 0:
                self.log(f"❌ {symbol} - Lỗi giá")
                self.stop_symbol(symbol)
                return False

            step_size = get_step_size(symbol, self.api_key, self.api_secret)
            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)

            if qty <= 0 or qty < step_size:
                self.log(f"❌ {symbol} - Khối lượng không hợp lệ")
                self.stop_symbol(symbol)
                return False

            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                if executed_qty >= 0:
                    time.sleep(1)
                    self._check_symbol_position(symbol)
                    
                    if not self.symbol_data[symbol]['position_open']:
                        self.log(f"❌ {symbol} - Lệnh đã khớp nhưng không tạo vị thế")
                        self.stop_symbol(symbol)
                        return False
                    
                    pyramiding_info = {}
                    if self.pyramiding_enabled:
                        pyramiding_info = {
                            'pyramiding_count': 0,
                            'next_pyramiding_roi': self.pyramiding_x,
                            'last_pyramiding_time': 0,
                            'pyramiding_base_roi': 0.0,
                        }
                    
                    self.symbol_data[symbol].update({
                        'entry': avg_price, 'entry_base': avg_price, 'average_down_count': 0,
                        'side': side, 'qty': executed_qty if side == "BUY" else -executed_qty,
                        'position_open': True, 'status': "open", 'high_water_mark_roi': 0,
                        'roi_check_activated': False,
                        **pyramiding_info
                    })

                    self.bot_coordinator.bot_has_coin(self.bot_id)
                    
                    # Lưu vào database
                    self._save_position_to_db(symbol, "open")
                    
                    # Lưu lịch sử giao dịch
                    self._save_trade_history(
                        symbol, 
                        f"OPEN_{side}", 
                        avg_price, 
                        executed_qty,
                        reason=f"Mở vị thế {side} - Đòn bẩy {self.lev}x"
                    )

                    strategy_info = "📊 Khối lượng" if self.dynamic_strategy == "volume" else "📈 Biến động"
                    
                    message = (f"✅ <b>ĐÃ MỞ VỊ THẾ {symbol}</b>\n"
                              f"🤖 Bot: {self.bot_id} ({strategy_info})\n📌 Hướng: {side}\n"
                              f"🏷️ Entry: {avg_price:.4f}\n📊 Khối lượng: {executed_qty:.4f}\n"
                              f"💰 Đòn bẩy: {self.lev}x\n🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%")
                    if self.roi_trigger:
                        message += f" | 🎯 ROI Kích hoạt: {self.roi_trigger}%"
                    if self.pyramiding_enabled:
                        message += f" | 🔄 Nhồi lệnh: {self.pyramiding_n} lần tại {self.pyramiding_x}%"
                    
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Lệnh chưa khớp")
                    self.stop_symbol(symbol)
                    return False
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi lệnh: {error_msg}")
                self.stop_symbol(symbol)
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi mở vị thế: {str(e)}")
            self.stop_symbol(symbol)
            return False

    def _check_pyramiding(self, symbol):
        """Nhồi lệnh khi đang lỗ với database"""
        try:
            if not self.pyramiding_enabled:
                return False

            info = self.symbol_data.get(symbol)
            if not info or not info.get('position_open', False):
                return False

            current_count = int(info.get('pyramiding_count', 0))
            if current_count >= self.pyramiding_n:
                return False

            current_time = time.time()
            if current_time - info.get('last_pyramiding_time', 0) < 60:
                return False

            current_price = self.get_current_price(symbol)
            if current_price is None or current_price <= 0:
                return False

            entry = float(info.get('entry', 0))
            qty   = abs(float(info.get('qty', 0)))
            if entry <= 0 or qty <= 0:
                return False

            if info.get('side') == "BUY":
                profit = (current_price - entry) * qty
            else:
                profit = (entry - current_price) * qty

            invested = entry * qty / self.lev
            if invested <= 0:
                return False

            roi = (profit / invested) * 100

            if roi >= 0:
                return False

            step = float(self.pyramiding_x or 0)
            if step <= 0:
                return False

            base_roi = float(info.get('pyramiding_base_roi', 0.0))
            target_roi = base_roi - step

            if roi > target_roi:
                return False

            self.log(f"📉 {symbol} - ROI hiện tại {roi:.2f}% <= mốc nhồi {target_roi:.2f}% → THỬ NHỒI...")

            if self._pyramid_order(symbol):
                new_count = current_count + 1
                info['pyramiding_count'] = new_count
                info['pyramiding_base_roi'] = roi
                info['last_pyramiding_time'] = current_time
                
                # Cập nhật database
                self._update_position_in_db(symbol, {'pyramiding_count': new_count})

                self.log(f"🔄 {symbol} - ĐÃ NHỒI LẦN {new_count}/{self.pyramiding_n} tại ROI {roi:.2f}%")
                return True

            return False

        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra nhồi lệnh {symbol}: {str(e)}")
            return False

    def _pyramid_order(self, symbol):
        """Thực hiện lệnh nhồi với database"""
        try:
            symbol_info = self.symbol_data[symbol]
            if not symbol_info['position_open']:
                return False
            
            side = symbol_info['side']
            
            balance, available_balance = get_total_and_available_balance(self.api_key, self.api_secret)
            if balance is None or balance <= 0:
                self.log(f"❌ {symbol} - Không đủ tổng số dư để nhồi lệnh")
                return False
    
            required_usd = balance * (self.percent / 100)
    
            if available_balance is None or available_balance <= 0 or required_usd > available_balance:
                self.log(f"❌ {symbol} - Không đủ số dư khả dụng để nhồi lệnh: cần {required_usd:.2f}, khả dụng {available_balance or 0:.2f}")
                return False

            current_price = self.get_current_price(symbol)
            if current_price < 0:
                self.log(f"❌ {symbol} - Lỗi giá khi nhồi lệnh")
                return False

            step_size = get_step_size(symbol, self.api_key, self.api_secret)
            usd_amount = balance * (self.percent / 100)
            qty = (usd_amount * self.lev) / current_price
            if step_size > 0:
                qty = math.floor(qty / step_size) * step_size
                qty = round(qty, 8)

            if qty <= 0 or qty < step_size:
                self.log(f"❌ {symbol} - Khối lượng không hợp lệ khi nhồi lệnh")
                return False

            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)

            result = place_order(symbol, side, qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                executed_qty = float(result.get('executedQty', 0))
                avg_price = float(result.get('avgPrice', current_price))

                if executed_qty >= 0:
                    old_qty = symbol_info['qty']
                    old_entry = symbol_info['entry']
                    
                    total_qty = abs(old_qty) + executed_qty
                    if side == "BUY":
                        new_qty = old_qty + executed_qty
                        new_entry = (old_entry * abs(old_qty) + avg_price * executed_qty) / total_qty
                    else:
                        new_qty = old_qty - executed_qty
                        new_entry = (old_entry * abs(old_qty) + avg_price * executed_qty) / total_qty
                    
                    symbol_info['qty'] = new_qty
                    symbol_info['entry'] = new_entry
                    
                    # Cập nhật database
                    self._update_position_in_db(symbol, {})
                    
                    # Lưu lịch sử giao dịch
                    self._save_trade_history(
                        symbol,
                        f"PYRAMID_{side}",
                        avg_price,
                        executed_qty,
                        reason=f"Nhồi lệnh lần {symbol_info.get('pyramiding_count', 0) + 1}"
                    )
                    
                    message = (f"🔄 <b>NHỒI LỆNH {symbol}</b>\n"
                              f"🤖 Bot: {self.bot_id}\n📌 Hướng: {side}\n"
                              f"🏷️ Entry: {avg_price:.4f} (Trung bình: {new_entry:.4f})\n"
                              f"📊 Khối lượng: {executed_qty:.4f} (Tổng: {abs(new_qty):.4f})\n"
                              f"💰 Đòn bẩy: {self.lev}x\n🎯 Lần nhồi: {symbol_info.get('pyramiding_count', 0) + 1}/{self.pyramiding_n}")
                    
                    self.log(message)
                    return True
                else:
                    self.log(f"❌ {symbol} - Nhồi lệnh không thành công")
                    return False
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi nhồi lệnh: {error_msg}")
                return False

        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi nhồi lệnh: {str(e)}")
            return False

    def _close_symbol_position(self, symbol, reason=""):
        """Đóng vị thế với database"""
        try:
            self._check_symbol_position(symbol)
            if not self.symbol_data[symbol]['position_open'] or abs(self.symbol_data[symbol]['qty']) <= 0:
                return True

            current_time = time.time()
            if (self.symbol_data[symbol]['close_attempted'] and 
                current_time - self.symbol_data[symbol]['last_close_attempt'] < 30):
                return False
            
            self.symbol_data[symbol]['close_attempted'] = True
            self.symbol_data[symbol]['last_close_attempt'] = current_time

            close_side = "SELL" if self.symbol_data[symbol]['side'] == "BUY" else "BUY"
            close_qty = abs(self.symbol_data[symbol]['qty'])
            
            cancel_all_orders(symbol, self.api_key, self.api_secret)
            time.sleep(1)
            
            result = place_order(symbol, close_side, close_qty, self.api_key, self.api_secret)
            if result and 'orderId' in result:
                current_price = self.get_current_price(symbol)
                pnl = 0
                roi = 0
                
                if self.symbol_data[symbol]['entry'] > 0:
                    if self.symbol_data[symbol]['side'] == "BUY":
                        pnl = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
                    else:
                        pnl = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
                    
                    invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
                    if invested > 0:
                        roi = (pnl / invested) * 100
                
                # Cập nhật database
                db_manager.close_position(self.bot_id, symbol, pnl, roi)
                
                # Lưu lịch sử giao dịch
                self._save_trade_history(
                    symbol,
                    f"CLOSE_{close_side}",
                    current_price,
                    close_qty,
                    pnl,
                    roi,
                    reason
                )
                
                pyramiding_info = ""
                if self.pyramiding_enabled:
                    pyramiding_count = self.symbol_data[symbol].get('pyramiding_count', 0)
                    pyramiding_info = f"\n🔄 Số lần đã nhồi: {pyramiding_count}/{self.pyramiding_n}"
                
                message = (f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {symbol}</b>\n"
                          f"🤖 Bot: {self.bot_id}\n📌 Lý do: {reason}\n"
                          f"🏷️ Exit: {current_price:.4f}\n📊 Khối lượng: {close_qty:.4f}\n"
                          f"💰 PnL: {pnl:.2f} USDT | ROI: {roi:.2f}%\n"
                          f"📈 Lần hạ giá trung bình: {self.symbol_data[symbol]['average_down_count']}"
                          f"{pyramiding_info}")
                self.log(message)
                
                self.symbol_data[symbol]['last_close_time'] = time.time()
                self._reset_symbol_position(symbol)
                self.bot_coordinator.bot_lost_coin(self.bot_id)
                return True
            else:
                error_msg = result.get('msg', 'Lỗi không xác định') if result else 'Không có phản hồi'
                self.log(f"❌ {symbol} - Lỗi lệnh đóng: {error_msg}")
                self.symbol_data[symbol]['close_attempted'] = False
                return False
                
        except Exception as e:
            self.log(f"❌ {symbol} - Lỗi đóng vị thế: {str(e)}")
            self.symbol_data[symbol]['close_attempted'] = False
            return False

    def _check_margin_safety(self):
        """Kiểm tra an toàn ký quỹ toàn tài khoản"""
        try:
            margin_balance, maint_margin, ratio = get_margin_safety_info(
                self.api_key, self.api_secret
            )

            if margin_balance is None or maint_margin is None or ratio is None:
                return False

            if ratio <= self.margin_safety_threshold:
                msg = (f"🛑 BẢO VỆ KÝ QUỸ ĐƯỢC KÍCH HOẠT\n"
                      f"• Margin / Maint = {ratio:.2f}x ≤ {self.margin_safety_threshold:.2f}x\n"
                      f"• Đang đóng toàn bộ vị thế của bot để tránh thanh lý.")
                self.log(msg)

                send_telegram(
                    msg,
                    chat_id=self.telegram_chat_id,
                    bot_token=self.telegram_bot_token,
                    default_chat_id=self.telegram_chat_id,
                )

                self.stop_all_symbols()
                return True

            return False

        except Exception as e:
            self.log(f"❌ Lỗi kiểm tra an toàn ký quỹ: {str(e)}")
            return False

    def _check_symbol_tp_sl(self, symbol):
        """Kiểm tra Take Profit và Stop Loss"""
        if (not self.symbol_data[symbol]['position_open'] or 
            self.symbol_data[symbol]['entry'] <= 0 or 
            self.symbol_data[symbol]['close_attempted']):
            return

        current_price = self.get_current_price(symbol)
        if current_price <= 0: return

        if self.symbol_data[symbol]['side'] == "BUY":
            profit = (current_price - self.symbol_data[symbol]['entry']) * abs(self.symbol_data[symbol]['qty'])
        else:
            profit = (self.symbol_data[symbol]['entry'] - current_price) * abs(self.symbol_data[symbol]['qty'])
            
        invested = self.symbol_data[symbol]['entry'] * abs(self.symbol_data[symbol]['qty']) / self.lev
        if invested <= 0: return
            
        roi = (profit / invested) * 100

        if roi > self.symbol_data[symbol]['high_water_mark_roi']:
            self.symbol_data[symbol]['high_water_mark_roi'] = roi

        if (self.roi_trigger is not None and 
            self.symbol_data[symbol]['high_water_mark_roi'] >= self.roi_trigger and 
            not self.symbol_data[symbol]['roi_check_activated']):
            self.symbol_data[symbol]['roi_check_activated'] = True

        if self.tp is not None and roi >= self.tp:
            self._close_symbol_position(symbol, f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
        elif self.sl is not None and self.sl > 0 and roi <= -self.sl:
            self._close_symbol_position(symbol, f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")

    def check_global_positions(self):
        """
        Cập nhật tổng khối lượng LONG/SHORT toàn tài khoản
        """
        try:
            positions = get_positions(api_key=self.api_key, api_secret=self.api_secret)

            if not positions:
                self.global_long_count = 0
                self.global_short_count = 0
                self.global_long_pnl = 0
                self.global_short_pnl = 0
                self.global_long_volume = 0.0
                self.global_short_volume = 0.0

                self.next_global_side = random.choice(["BUY", "SELL"])
                return

            long_count, short_count = 0, 0
            long_volume, short_volume = 0.0, 0.0

            for pos in positions:
                position_amt = float(pos.get("positionAmt", 0.0))
                if position_amt == 0:
                    continue

                if position_amt > 0:
                    long_count += 1
                elif position_amt < 0:
                    short_count += 1

                try:
                    lev = float(pos.get("leverage", 1.0))
                except Exception:
                    lev = 1.0

                price = 0.0
                try:
                    price = float(pos.get("markPrice") or 0.0)
                except Exception:
                    price = 0.0

                if price <= 0:
                    try:
                        price = float(pos.get("entryPrice") or 0.0)
                    except Exception:
                        price = 0.0

                if price <= 0:
                    continue

                notional = abs(position_amt) * price
                effective_volume = notional * lev

                if position_amt > 0:
                    long_volume += effective_volume
                elif position_amt < 0:
                    short_volume += effective_volume

            self.global_long_count = long_count
            self.global_short_count = short_count
            self.global_long_pnl = 0
            self.global_short_pnl = 0
            self.global_long_volume = long_volume
            self.global_short_volume = short_volume

            if long_volume > 0 or short_volume > 0:
                diff = abs(long_volume - short_volume)
                total = long_volume + short_volume
                if total > 0:
                    imbalance = diff / total
                else:
                    imbalance = 0

                if imbalance < 0.01:
                    self.next_global_side = random.choice(["BUY", "SELL"])
                else:
                    if long_volume > short_volume:
                        self.next_global_side = "SELL"
                    else:
                        self.next_global_side = "BUY"
            else:
                if long_count > short_count:
                    self.next_global_side = "SELL"
                elif short_count > long_count:
                    self.next_global_side = "BUY"
                else:
                    self.next_global_side = random.choice(["BUY", "SELL"])

        except Exception as e:
            if time.time() - self.last_error_log_time > 30:
                self.log(f"❌ Lỗi vị thế toàn cục: {str(e)}")
                self.last_error_log_time = time.time()

    # ========== CÁC HÀM QUẢN LÝ BOT ==========

    def stop_symbol(self, symbol):
        """Dừng theo dõi một symbol với database"""
        if symbol not in self.active_symbols: return False
        
        self.log(f"⛔ Đang dừng coin {symbol}...")
        
        if self.current_processing_symbol == symbol:
            timeout = time.time() + 10
            while self.current_processing_symbol == symbol and time.time() < timeout:
                time.sleep(1)
        
        if self.symbol_data[symbol]['position_open']:
            self._close_symbol_position(symbol, "Dừng coin theo lệnh")
        
        self.ws_manager.remove_symbol(symbol)
        self.coin_manager.unregister_coin(symbol, self.bot_id)
        
        # Xóa khỏi database
        try:
            query = "DELETE FROM bot_positions WHERE bot_id = %s AND symbol = %s"
            db_manager.execute_query(query, (self.bot_id, symbol))
        except Exception as e:
            logger.error(f"Lỗi xóa vị thế {symbol} từ database: {str(e)}")
        
        if symbol in self.symbol_data: del self.symbol_data[symbol]
        if symbol in self.active_symbols: self.active_symbols.remove(symbol)
        
        self.bot_coordinator.bot_lost_coin(self.bot_id)
        self.log(f"✅ Đã dừng coin {symbol}")
        return True

    def stop_all_symbols(self):
        """Dừng tất cả symbol"""
        self.log("⛔ Đang dừng tất cả coin...")
        symbols_to_stop = self.active_symbols.copy()
        stopped_count = 0
        
        for symbol in symbols_to_stop:
            if self.stop_symbol(symbol):
                stopped_count += 1
                time.sleep(1)
        
        self.log(f"✅ Đã dừng {stopped_count} coin, bot vẫn chạy")
        return stopped_count

    def stop(self):
        """Dừng bot hoàn toàn và cập nhật database"""
        self._stop = True
        
        # Cập nhật trạng thái bot trong database
        db_manager.update_bot_status(self.bot_id, "stopped")
        
        stopped_count = self.stop_all_symbols()
        self.log(f"🔴 Bot đã dừng - Đã dừng {stopped_count} coin")

    def log(self, message):
        """Ghi log và gửi thông báo Telegram"""
        important_keywords = ['❌', '✅', '⛔', '💰', '📈', '📊', '🎯', '🛡️', '🔴', '🟢', '⚠️', '🚫', '🔄']
        if any(keyword in message for keyword in important_keywords):
            logger.warning(f"[{self.bot_id}] {message}")
            if self.telegram_bot_token and self.telegram_chat_id:
                send_telegram(f"<b>{self.bot_id}</b>: {message}", 
                             bot_token=self.telegram_bot_token, 
                             default_chat_id=self.telegram_chat_id)

# ========== CÁC LỚP BOT CỤ THỂ ==========

class BalanceProtectionBot(BaseBot):
    """Bot bảo vệ vốn - Chiến lược biến động"""
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Bot-Biến-Động", bot_id=bot_id, 
                         dynamic_strategy="volatility",
                         **kwargs)

class CompoundProfitBot(BaseBot):
    """Bot lãi kép - Chiến lược khối lượng"""
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Bot-Khối-Lượng", bot_id=bot_id, 
                         dynamic_strategy="volume",
                         **kwargs)

class StaticMarketBot(BaseBot):
    """Bot tĩnh - Coin cố định"""
    def __init__(self, symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                 api_key, api_secret, telegram_bot_token, telegram_chat_id, bot_id=None, **kwargs):
        
        static_entry_mode = kwargs.pop('static_entry_mode', 'signal')
        
        super().__init__(symbol, lev, percent, tp, sl, roi_trigger, ws_manager,
                         api_key, api_secret, telegram_bot_token, telegram_chat_id,
                         "Bot-Tĩnh", bot_id=bot_id, 
                         static_entry_mode=static_entry_mode,
                         **kwargs)
