# trading_bot_auth_system.py
# PHẦN 5: HỆ THỐNG ĐĂNG NHẬP ĐA NGƯỜI DÙNG VỚI JWT

from trading_bot_lib_part1 import db_manager, logger
from trading_bot_lib_part4 import get_database_connection, send_telegram

import os
import time
import hashlib
import secrets
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

import jwt
from flask import request, jsonify, session
from functools import wraps

# ================== CẤU HÌNH ==================
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24

# ================== INIT DATABASE ==================
def init_auth_tables():
    """Khởi tạo bảng người dùng trong database"""
    init_queries = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            password_salt VARCHAR(50) NOT NULL,
            binance_api_key VARCHAR(255),
            binance_api_secret VARCHAR(255),
            telegram_bot_token VARCHAR(255),
            telegram_chat_id VARCHAR(100),
            is_active BOOLEAN DEFAULT TRUE,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )
        """,
        
        """
        CREATE TABLE IF NOT EXISTS user_sessions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            session_token VARCHAR(255) UNIQUE NOT NULL,
            ip_address VARCHAR(45),
            user_agent TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            is_valid BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,
        
        """
        CREATE TABLE IF NOT EXISTS user_balance_logs (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total_balance DECIMAL(20, 8) DEFAULT 0,
            available_balance DECIMAL(20, 8) DEFAULT 0,
            margin_balance DECIMAL(20, 8) DEFAULT 0,
            maint_margin DECIMAL(20, 8) DEFAULT 0,
            margin_ratio DECIMAL(10, 4) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """,
        
        "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
        "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)",
        "CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(session_token)",
        "CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id)"
    ]
    
    conn = None
    try:
        conn = get_database_connection()
        if not conn:
            logger.error("❌ Không thể kết nối database để tạo bảng users")
            return False
        
        cursor = conn.cursor()
        for query in init_queries:
            cursor.execute(query)
        
        conn.commit()
        logger.info("✅ Đã khởi tạo bảng người dùng")
        
        # Tạo tài khoản admin mặc định nếu không có user nào
        cursor.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            create_default_admin()
            logger.info("✅ Đã tạo tài khoản admin mặc định")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi khởi tạo bảng auth: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def create_default_admin():
    """Tạo tài khoản admin mặc định"""
    default_password = os.getenv("ADMIN_DEFAULT_PASSWORD", "admin123")
    salt, password_hash = hash_password(default_password)
    
    query = """
    INSERT INTO users (username, email, password_hash, password_salt, is_admin, is_active)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (username) DO NOTHING
    """
    
    params = ("admin", "admin@tradingbot.com", password_hash, salt, True, True)
    db_manager.execute_query(query, params)

# ================== HÀM BẢO MẬT ==================
def hash_password(password: str) -> Tuple[str, str]:
    """Hash mật khẩu với salt"""
    salt = secrets.token_hex(16)
    combined = password + salt
    hash_obj = hashlib.sha256(combined.encode())
    return salt, hash_obj.hexdigest()

def verify_password(password: str, salt: str, stored_hash: str) -> bool:
    """Xác thực mật khẩu"""
    combined = password + salt
    hash_obj = hashlib.sha256(combined.encode())
    return hash_obj.hexdigest() == stored_hash

def generate_jwt_token(user_id: int, username: str, is_admin: bool) -> str:
    """Tạo JWT token"""
    payload = {
        "user_id": user_id,
        "username": username,
        "is_admin": is_admin,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

def verify_jwt_token(token: str) -> Optional[Dict]:
    """Xác thực JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token đã hết hạn")
        return None
    except jwt.InvalidTokenError:
        logger.warning("Token không hợp lệ")
        return None

# ================== DECORATOR XÁC THỰC ==================
def login_required(f):
    """Decorator yêu cầu đăng nhập"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        
        # Lấy token từ header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
        else:
            # Thử lấy từ cookie
            token = request.cookies.get('access_token')
        
        if not token:
            return jsonify({"error": "Token xác thực không tìm thấy"}), 401
        
        # Xác thực token
        payload = verify_jwt_token(token)
        if not payload:
            return jsonify({"error": "Token không hợp lệ hoặc đã hết hạn"}), 401
        
        # Lưu thông tin user vào request context
        request.user_id = payload['user_id']
        request.username = payload['username']
        request.is_admin = payload.get('is_admin', False)
        
        return f(*args, **kwargs)
    
    return decorated_function

def admin_required(f):
    """Decorator yêu cầu quyền admin"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Kiểm tra đăng nhập trước
        response = login_required(f)(*args, **kwargs)
        
        # Nếu không phải response tuple (tức là đã pass login_required)
        if not isinstance(response, tuple) and hasattr(request, 'is_admin'):
            if not request.is_admin:
                return jsonify({"error": "Yêu cầu quyền admin"}), 403
        
        return response
    
    return decorated_function

# ================== API ENDPOINTS - AUTH ==================
def register_auth_routes(app):
    """Đăng ký route xác thực vào Flask app"""
    
    @app.route('/api/auth/register', methods=['POST'])
    def register_user():
        """Đăng ký người dùng mới"""
        try:
            data = request.get_json()
            username = data.get('username')
            email = data.get('email')
            password = data.get('password')
            
            if not all([username, email, password]):
                return jsonify({"error": "Thiếu thông tin bắt buộc"}), 400
            
            if len(password) < 6:
                return jsonify({"error": "Mật khẩu phải có ít nhất 6 ký tự"}), 400
            
            # Kiểm tra username/email đã tồn tại
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT id FROM users WHERE username = %s OR email = %s", 
                          (username, email))
            if cursor.fetchone():
                conn.close()
                return jsonify({"error": "Username hoặc email đã tồn tại"}), 400
            
            # Hash mật khẩu
            salt, password_hash = hash_password(password)
            
            # Tạo user
            cursor.execute("""
                INSERT INTO users (username, email, password_hash, password_salt, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, username, email, created_at
            """, (username, email, password_hash, salt, True))
            
            user = cursor.fetchone()
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Đã đăng ký user mới: {username}")
            
            return jsonify({
                "success": True,
                "message": "Đăng ký thành công",
                "user": {
                    "id": user[0],
                    "username": user[1],
                    "email": user[2],
                    "created_at": user[3].isoformat()
                }
            }), 201
            
        except Exception as e:
            logger.error(f"❌ Lỗi đăng ký: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/auth/login', methods=['POST'])
    def login_user():
        """Đăng nhập"""
        try:
            data = request.get_json()
            username = data.get('username')
            password = data.get('password')
            
            if not username or not password:
                return jsonify({"error": "Thiếu username hoặc password"}), 400
            
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Lấy thông tin user
            cursor.execute("""
                SELECT id, username, email, password_hash, password_salt, is_admin, is_active
                FROM users WHERE username = %s OR email = %s
            """, (username, username))
            
            user = cursor.fetchone()
            
            if not user:
                conn.close()
                return jsonify({"error": "Tài khoản không tồn tại"}), 401
            
            user_id, username, email, stored_hash, salt, is_admin, is_active = user
            
            if not is_active:
                conn.close()
                return jsonify({"error": "Tài khoản đã bị vô hiệu hóa"}), 403
            
            # Xác thực mật khẩu
            if not verify_password(password, salt, stored_hash):
                conn.close()
                return jsonify({"error": "Mật khẩu không đúng"}), 401
            
            # Cập nhật last_login
            cursor.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s", (user_id,))
            
            # Tạo JWT token
            token = generate_jwt_token(user_id, username, is_admin)
            
            # Lưu session vào database
            session_token = secrets.token_hex(32)
            expires_at = datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
            
            cursor.execute("""
                INSERT INTO user_sessions (user_id, session_token, ip_address, user_agent, expires_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, session_token, request.remote_addr, request.user_agent.string, expires_at))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ User đăng nhập: {username}")
            
            response = jsonify({
                "success": True,
                "message": "Đăng nhập thành công",
                "token": token,
                "user": {
                    "id": user_id,
                    "username": username,
                    "email": email,
                    "is_admin": is_admin
                }
            })
            
            # Set cookie
            response.set_cookie(
                'access_token',
                token,
                httponly=True,
                secure=(os.getenv('FLASK_ENV') == 'production'),
                samesite='Strict',
                max_age=JWT_EXPIRE_HOURS * 3600
            )
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Lỗi đăng nhập: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/auth/logout', methods=['POST'])
    @login_required
    def logout_user():
        """Đăng xuất"""
        try:
            token = request.headers.get('Authorization', '').replace('Bearer ', '')
            
            # Vô hiệu hóa session trong database
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE user_sessions 
                SET is_valid = FALSE 
                WHERE session_token = %s AND is_valid = TRUE
            """, (token,))
            
            conn.commit()
            conn.close()
            
            response = jsonify({"success": True, "message": "Đã đăng xuất"})
            response.delete_cookie('access_token')
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Lỗi đăng xuất: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/auth/me', methods=['GET'])
    @login_required
    def get_current_user():
        """Lấy thông tin user hiện tại"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, username, email, binance_api_key, binance_api_secret,
                       telegram_bot_token, telegram_chat_id, is_admin, is_active,
                       created_at, last_login
                FROM users WHERE id = %s
            """, (request.user_id,))
            
            user = cursor.fetchone()
            conn.close()
            
            if not user:
                return jsonify({"error": "User không tồn tại"}), 404
            
            # Ẩn thông tin nhạy cảm
            user_dict = {
                "id": user[0],
                "username": user[1],
                "email": user[2],
                "binance_api_key_configured": bool(user[3]),
                "binance_api_secret_configured": bool(user[4]),
                "telegram_configured": bool(user[5] and user[6]),
                "is_admin": user[7],
                "is_active": user[8],
                "created_at": user[9].isoformat() if user[9] else None,
                "last_login": user[10].isoformat() if user[10] else None
            }
            
            return jsonify({"user": user_dict})
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy thông tin user: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/auth/update-api-keys', methods=['PUT'])
    @login_required
    def update_user_api_keys():
        """Cập nhật API keys của user"""
        try:
            data = request.get_json()
            
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Lấy user hiện tại để merge
            cursor.execute("""
                SELECT binance_api_key, binance_api_secret, telegram_bot_token, telegram_chat_id
                FROM users WHERE id = %s
            """, (request.user_id,))
            
            current = cursor.fetchone()
            current_data = {
                'binance_api_key': current[0] if current else None,
                'binance_api_secret': current[1] if current else None,
                'telegram_bot_token': current[2] if current else None,
                'telegram_chat_id': current[3] if current else None
            }
            
            # Merge với dữ liệu mới
            update_data = {**current_data, **data}
            
            # Cập nhật
            cursor.execute("""
                UPDATE users 
                SET binance_api_key = %s, binance_api_secret = %s,
                    telegram_bot_token = %s, telegram_chat_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                update_data.get('binance_api_key'),
                update_data.get('binance_api_secret'),
                update_data.get('telegram_bot_token'),
                update_data.get('telegram_chat_id'),
                request.user_id
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ User {request.username} đã cập nhật API keys")
            
            return jsonify({
                "success": True,
                "message": "Đã cập nhật API keys"
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi cập nhật API keys: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/auth/change-password', methods=['POST'])
    @login_required
    def change_password():
        """Đổi mật khẩu"""
        try:
            data = request.get_json()
            current_password = data.get('current_password')
            new_password = data.get('new_password')
            
            if len(new_password) < 6:
                return jsonify({"error": "Mật khẩu mới phải có ít nhất 6 ký tự"}), 400
            
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Lấy thông tin mật khẩu hiện tại
            cursor.execute("""
                SELECT password_hash, password_salt FROM users WHERE id = %s
            """, (request.user_id,))
            
            result = cursor.fetchone()
            if not result:
                conn.close()
                return jsonify({"error": "User không tồn tại"}), 404
            
            stored_hash, salt = result
            
            # Xác thực mật khẩu hiện tại
            if not verify_password(current_password, salt, stored_hash):
                conn.close()
                return jsonify({"error": "Mật khẩu hiện tại không đúng"}), 401
            
            # Hash mật khẩu mới
            new_salt, new_hash = hash_password(new_password)
            
            # Cập nhật
            cursor.execute("""
                UPDATE users 
                SET password_hash = %s, password_salt = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_hash, new_salt, request.user_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ User {request.username} đã đổi mật khẩu")
            
            return jsonify({
                "success": True,
                "message": "Đã đổi mật khẩu thành công"
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi đổi mật khẩu: {str(e)}")
            return jsonify({"error": str(e)}), 500

# ================== API ENDPOINTS - USER BOTS ==================
def register_user_bot_routes(app, bot_manager):
    """Đăng ký route quản lý bot theo user"""
    
    @app.route('/api/user/bots', methods=['GET'])
    @login_required
    def get_user_bots():
        """Lấy danh sách bot của user"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    bot_id, bot_mode, bot_type, symbol, leverage, percent,
                    tp, sl, roi_trigger, pyramiding_n, pyramiding_x,
                    dynamic_strategy, static_entry_mode, reverse_on_stop,
                    status, created_at, updated_at
                FROM bot_configs 
                WHERE user_id = %s AND deleted_at IS NULL
                ORDER BY created_at DESC
            """, (request.user_id,))
            
            columns = [desc[0] for desc in cursor.description]
            bots = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            
            return jsonify({"bots": bots})
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy bots của user: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/user/bots', methods=['POST'])
    @login_required
    def create_user_bot():
        """Tạo bot mới cho user"""
        try:
            data = request.get_json()
            
            # Lấy API keys của user
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT binance_api_key, binance_api_secret, telegram_bot_token, telegram_chat_id
                FROM users WHERE id = %s
            """, (request.user_id,))
            
            user_keys = cursor.fetchone()
            conn.close()
            
            if not user_keys or not user_keys[0] or not user_keys[1]:
                return jsonify({
                    "error": "Chưa cấu hình Binance API keys. Vui lòng cập nhật trong phần Profile."
                }), 400
            
            # Truyền API keys của user vào bot
            data['api_key'] = user_keys[0]
            data['api_secret'] = user_keys[1]
            data['telegram_bot_token'] = user_keys[2]
            data['telegram_chat_id'] = user_keys[3]
            data['user_id'] = request.user_id
            
            # Gọi đến endpoint tạo bot gốc
            from trading_bot_lib_part4 import create_bot
            return create_bot()
            
        except Exception as e:
            logger.error(f"❌ Lỗi tạo bot cho user: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/user/balance', methods=['GET'])
    @login_required
    def get_user_balance():
        """Lấy số dư của user"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Lấy API keys của user
            cursor.execute("""
                SELECT binance_api_key, binance_api_secret FROM users WHERE id = %s
            """, (request.user_id,))
            
            user_keys = cursor.fetchone()
            conn.close()
            
            if not user_keys or not user_keys[0] or not user_keys[1]:
                return jsonify({
                    "error": "Chưa cấu hình Binance API keys"
                }), 400
            
            # Lấy số dư từ Binance (sử dụng hàm từ part1)
            from trading_bot_lib_part1 import get_total_and_available_balance, get_margin_safety_info
            
            total, available = get_total_and_available_balance(user_keys[0], user_keys[1])
            margin_balance, maint_margin, ratio = get_margin_safety_info(user_keys[0], user_keys[1])
            
            if total is None:
                return jsonify({
                    "error": "Không thể lấy số dư từ Binance. Kiểm tra API keys."
                }), 400
            
            # Lưu log số dư
            log_query = """
                INSERT INTO user_balance_logs 
                (user_id, total_balance, available_balance, margin_balance, maint_margin, margin_ratio)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            db_manager.execute_query(log_query, (
                request.user_id, total, available, margin_balance, maint_margin, ratio
            ))
            
            return jsonify({
                "total_balance": total,
                "available_balance": available,
                "margin_balance": margin_balance,
                "maint_margin": maint_margin,
                "margin_ratio": ratio,
                "is_safe": ratio > 1.15 if ratio else False,
                "timestamp": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy số dư user: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/user/positions', methods=['GET'])
    @login_required
    def get_user_positions():
        """Lấy vị thế của user"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    bp.*,
                    bc.bot_mode, bc.bot_type, bc.leverage, bc.percent
                FROM bot_positions bp
                JOIN bot_configs bc ON bp.bot_id = bc.bot_id
                WHERE bc.user_id = %s AND bp.status = 'open'
                ORDER BY bp.last_update DESC
            """, (request.user_id,))
            
            columns = [desc[0] for desc in cursor.description]
            positions = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            
            return jsonify({"positions": positions})
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy positions của user: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/user/statistics', methods=['GET'])
    @login_required
    def get_user_statistics():
        """Lấy thống kê của user"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Thống kê từ bot_statistics
            cursor.execute("""
                SELECT 
                    SUM(bs.total_trades) as total_trades,
                    SUM(bs.winning_trades) as winning_trades,
                    SUM(bs.losing_trades) as losing_trades,
                    SUM(bs.total_pnl) as total_pnl,
                    COUNT(DISTINCT bc.bot_id) as total_bots
                FROM bot_statistics bs
                JOIN bot_configs bc ON bs.bot_id = bc.bot_id
                WHERE bc.user_id = %s
            """, (request.user_id,))
            
            stats = cursor.fetchone()
            
            # Thống kê ngày
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    SUM(pnl) as daily_pnl,
                    COUNT(*) as daily_trades
                FROM trade_history th
                JOIN bot_configs bc ON th.bot_id = bc.bot_id
                WHERE bc.user_id = %s AND created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """, (request.user_id,))
            
            daily_stats = cursor.fetchall()
            
            conn.close()
            
            return jsonify({
                "statistics": {
                    "total_trades": stats[0] or 0,
                    "winning_trades": stats[1] or 0,
                    "losing_trades": stats[2] or 0,
                    "total_pnl": float(stats[3] or 0),
                    "total_bots": stats[4] or 0,
                    "win_rate": (stats[1] / stats[0] * 100) if stats[0] else 0
                },
                "daily_stats": [
                    {
                        "date": row[0].isoformat(),
                        "daily_pnl": float(row[1] or 0),
                        "daily_trades": row[2] or 0
                    } for row in daily_stats
                ]
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy statistics của user: {str(e)}")
            return jsonify({"error": str(e)}), 500

# ================== ADMIN ENDPOINTS ==================
def register_admin_routes(app):
    """Đăng ký route admin"""
    
    @app.route('/api/admin/users', methods=['GET'])
    @admin_required
    def get_all_users():
        """Lấy danh sách tất cả users (admin only)"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    id, username, email, is_admin, is_active,
                    created_at, last_login,
                    (SELECT COUNT(*) FROM bot_configs WHERE user_id = users.id) as bot_count
                FROM users
                ORDER BY created_at DESC
            """)
            
            columns = [desc[0] for desc in cursor.description]
            users = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            conn.close()
            
            return jsonify({"users": users})
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy users (admin): {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/admin/users/<int:user_id>/toggle', methods=['PUT'])
    @admin_required
    def toggle_user_status(user_id):
        """Bật/tắt user (admin only)"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Lấy trạng thái hiện tại
            cursor.execute("SELECT is_active FROM users WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return jsonify({"error": "User không tồn tại"}), 404
            
            new_status = not result[0]
            
            # Cập nhật
            cursor.execute("""
                UPDATE users SET is_active = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (new_status, user_id))
            
            conn.commit()
            conn.close()
            
            status_text = "kích hoạt" if new_status else "vô hiệu hóa"
            logger.info(f"✅ Admin {request.username} đã {status_text} user #{user_id}")
            
            return jsonify({
                "success": True,
                "message": f"Đã {status_text} user",
                "new_status": new_status
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi toggle user status: {str(e)}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/admin/system-stats', methods=['GET'])
    @admin_required
    def get_system_stats():
        """Thống kê hệ thống (admin only)"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Tổng số users
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            
            # Users active
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = TRUE")
            active_users = cursor.fetchone()[0]
            
            # Tổng số bots
            cursor.execute("SELECT COUNT(*) FROM bot_configs WHERE deleted_at IS NULL")
            total_bots = cursor.fetchone()[0]
            
            # Bots running
            cursor.execute("SELECT COUNT(*) FROM bot_configs WHERE status = 'running' AND deleted_at IS NULL")
            running_bots = cursor.fetchone()[0]
            
            # Tổng PnL
            cursor.execute("SELECT SUM(total_pnl) FROM bot_statistics")
            total_pnl = cursor.fetchone()[0] or 0
            
            # Thống kê theo ngày
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(DISTINCT user_id) as active_users,
                    COUNT(DISTINCT bot_id) as active_bots,
                    SUM(pnl) as daily_pnl
                FROM trade_history th
                JOIN bot_configs bc ON th.bot_id = bc.bot_id
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """)
            
            daily_stats = cursor.fetchall()
            
            conn.close()
            
            return jsonify({
                "system_stats": {
                    "total_users": total_users,
                    "active_users": active_users,
                    "total_bots": total_bots,
                    "running_bots": running_bots,
                    "total_pnl": float(total_pnl)
                },
                "daily_stats": [
                    {
                        "date": row[0].isoformat(),
                        "active_users": row[1] or 0,
                        "active_bots": row[2] or 0,
                        "daily_pnl": float(row[3] or 0)
                    } for row in daily_stats
                ]
            })
            
        except Exception as e:
            logger.error(f"❌ Lỗi lấy system stats: {str(e)}")
            return jsonify({"error": str(e)}), 500

# ================== INITIALIZE ==================
def initialize_auth_system(app, bot_manager=None):
    """Khởi tạo hệ thống auth"""
    logger.info("🔄 Đang khởi tạo hệ thống đăng nhập...")
    
    # Khởi tạo bảng
    init_auth_tables()
    
    # Đăng ký routes
    register_auth_routes(app)
    
    if bot_manager:
        register_user_bot_routes(app, bot_manager)
    
    register_admin_routes(app)
    
    logger.info("✅ Hệ thống đăng nhập đã sẵn sàng")
