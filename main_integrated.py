# main_integrated.py
# FILE CHÍNH TÍCH HỢP TẤT CẢ HỆ THỐNG

import os
import sys
import logging
from datetime import datetime

# Thêm path cho imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import tất cả các phần
from trading_bot_lib_part1 import (
    logger, DatabaseManager, CoinManager, 
    BotExecutionCoordinator, SmartCoinFinder, WebSocketManager
)
from trading_bot_lib_part3 import BotManager
from trading_bot_lib_part4 import run_api_server, start_web_in_thread
from trading_bot_auth_system import initialize_auth_system

import threading
import time
from flask import Flask

def setup_global_logging():
    """Thiết lập logging toàn hệ thống"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('trading_bot_system.log'),
            logging.FileHandler('trading_bot_errors.log')
        ]
    )
    return logging.getLogger(__name__)

class TradingBotSystem:
    """Lớp quản lý toàn bộ hệ thống Trading Bot"""
    
    def __init__(self):
        self.logger = setup_global_logging()
        self.db_manager = DatabaseManager.get_instance()
        self.bot_manager = None
        self.app = None
        self.is_running = False
        
        # Khởi tạo các components
        self.coin_manager = CoinManager()
        self.bot_coordinator = BotExecutionCoordinator()
        self.ws_manager = WebSocketManager()
        
        self.logger.info("=" * 60)
        self.logger.info("🚀 HỆ THỐNG TRADING BOT ĐA NGƯỜI DÙNG")
        self.logger.info("=" * 60)
    
    def initialize(self):
        """Khởi tạo hệ thống"""
        try:
            self.logger.info("🔄 Đang khởi tạo hệ thống...")
            
            # Kiểm tra biến môi trường
            required_envs = ['DATABASE_URL']
            missing_envs = [env for env in required_envs if not os.getenv(env)]
            
            if missing_envs:
                self.logger.error(f"❌ Thiếu biến môi trường: {missing_envs}")
                return False
            
            # Khởi tạo database
            if not self._init_database():
                return False
            
            # Khởi tạo Flask app cho API server
            self.app = self._create_flask_app()
            
            # Khởi tạo hệ thống auth
            initialize_auth_system(self.app)
            
            # Khởi tạo BotManager (chỉ dùng cho admin/system)
            self._init_bot_manager()
            
            # Khởi tạo WebSocket manager
            self._init_websocket_manager()
            
            self.is_running = True
            self.logger.info("✅ Hệ thống đã khởi tạo thành công")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo hệ thống: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _init_database(self):
        """Khởi tạo database"""
        try:
            # Kiểm tra kết nối
            conn = self.db_manager.get_connection()
            if conn:
                self.db_manager.return_connection(conn)
                self.logger.info("✅ Kết nối database thành công")
                return True
            else:
                self.logger.error("❌ Không thể kết nối database")
                return False
        except Exception as e:
            self.logger.error(f"❌ Lỗi kết nối database: {str(e)}")
            return False
    
    def _create_flask_app(self):
        """Tạo Flask app"""
        from flask import Flask
        from flask_cors import CORS
        
        app = Flask(__name__)
        CORS(app, supports_credentials=True)
        
        # Cấu hình
        app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'trading-bot-secret-key-2024')
        app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV') == 'production'
        app.config['SESSION_COOKIE_HTTPONLY'] = True
        app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
        
        # Route chào mừng
        @app.route('/')
        def home():
            return {
                "service": "Trading Bot System",
                "version": "1.0.0",
                "status": "running" if self.is_running else "stopped",
                "timestamp": datetime.now().isoformat(),
                "endpoints": {
                    "auth": "/api/auth/*",
                    "user": "/api/user/*",
                    "admin": "/api/admin/*",
                    "bots": "/api/bots/*",
                    "system": "/api/system/*"
                }
            }
        
        return app
    
    def _init_bot_manager(self):
        """Khởi tạo BotManager (cho admin)"""
        try:
            # Chỉ khởi tạo nếu có API keys trong env (cho admin)
            admin_api_key = os.getenv('BINANCE_API_KEY')
            admin_api_secret = os.getenv('BINANCE_API_SECRET')
            
            if admin_api_key and admin_api_secret:
                telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
                telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
                
                self.bot_manager = BotManager(
                    api_key=admin_api_key,
                    api_secret=admin_api_secret,
                    telegram_bot_token=telegram_bot_token,
                    telegram_chat_id=telegram_chat_id,
                    coin_manager=self.coin_manager,
                    bot_coordinator=self.bot_coordinator,
                    ws_manager=self.ws_manager
                )
                
                self.logger.info("✅ BotManager (admin) đã khởi tạo")
            else:
                self.logger.warning("⚠️ Không có API keys admin trong env")
                self.bot_manager = None
                
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo BotManager: {str(e)}")
            self.bot_manager = None
    
    def _init_websocket_manager(self):
        """Khởi tạo WebSocket manager"""
        try:
            # WebSocket manager đã được khởi tạo trong __init__
            self.logger.info("✅ WebSocket Manager đã khởi tạo")
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi tạo WebSocket: {str(e)}")
    
    def start_api_server(self, host='0.0.0.0', port=5000, debug=False):
        """Khởi động API server"""
        try:
            self.logger.info(f"🌐 Đang khởi động API server trên {host}:{port}")
            
            # Chạy server trong thread riêng
            server_thread = threading.Thread(
                target=self.app.run,
                kwargs={
                    'host': host,
                    'port': port,
                    'debug': debug,
                    'use_reloader': False
                },
                daemon=True
            )
            server_thread.start()
            
            self.logger.info("✅ API server đã khởi động")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Lỗi khởi động API server: {str(e)}")
            return False
    
    def start_system(self):
        """Khởi động toàn bộ hệ thống"""
        if not self.initialize():
            self.logger.error("❌ Không thể khởi tạo hệ thống")
            return False
        
        # Khởi động API server
        port = int(os.getenv('PORT', '5000'))
        if not self.start_api_server(port=port):
            return False
        
        # Thông báo hệ thống đã sẵn sàng
        self.logger.info("=" * 60)
        self.logger.info("🎉 HỆ THỐNG ĐÃ SẴN SÀNG!")
        self.logger.info(f"📊 Truy cập: http://localhost:{port}")
        self.logger.info("=" * 60)
        
        # Giữ chương trình chạy
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("👋 Nhận tín hiệu dừng...")
            self.stop()
        
        return True
    
    def stop(self):
        """Dừng hệ thống"""
        self.logger.info("🛑 Đang dừng hệ thống...")
        
        if self.bot_manager:
            self.bot_manager.stop_all()
        
        if self.ws_manager:
            self.ws_manager.stop()
        
        self.is_running = False
        self.logger.info("✅ Hệ thống đã dừng")

def main():
    """Hàm main chính"""
    system = TradingBotSystem()
    
    try:
        system.start_system()
    except Exception as e:
        system.logger.error(f"❌ Lỗi hệ thống: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
