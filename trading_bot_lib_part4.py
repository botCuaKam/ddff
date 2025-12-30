# trading_bot_lib_part4.py
# PHẦN 4: REST API SERVER CHO REACT & TELEGRAM SONG SONG
from trading_bot_lib_part3 import BotManager, run_bot_manager
from trading_bot_lib_part1 import db_manager, logger
from trading_bot_lib_part2 import StaticMarketBot, CompoundProfitBot, BalanceProtectionBot

import os
import json
import time
import threading
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from datetime import datetime
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor

# ========== KHỞI TẠO FLASK APP ==========
app = Flask(__name__, 
           static_folder='../react-app/build/static',
           template_folder='../react-app/build')
CORS(app)  # Cho phép React kết nối
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Biến toàn cục
bot_manager = None
api_status = {
    'status': 'stopped',
    'start_time': None,
    'total_bots': 0,
    'running_bots': 0,
    'total_balance': 0,
    'total_pnl': 0
}

# ========== HÀM KHỞI TẠO BOT MANAGER ==========
def initialize_bot_manager():
    """Khởi tạo Bot Manager từ biến môi trường"""
    global bot_manager
    
    try:
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not api_key or not api_secret:
            logger.error("❌ Thiếu cấu hình API Binance")
            return False
            
        logger.info("🟢 Đang khởi động BotManager...")
        
        bot_manager = BotManager(
            api_key=api_key,
            api_secret=api_secret,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id
        )
        
        # Cập nhật trạng thái
        api_status.update({
            'status': 'running',
            'start_time': datetime.now().isoformat(),
            'total_bots': len(bot_manager.bots) if bot_manager else 0
        })
        
        # Bắt đầu thread broadcast thông tin real-time
        threading.Thread(target=broadcast_updates, daemon=True).start()
        
        logger.info("✅ REST API Server đã sẵn sàng!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi khởi tạo BotManager: {str(e)}")
        return False

# ========== HÀM BROADCAST REAL-TIME ==========
def broadcast_updates():
    """Broadcast thông tin real-time qua WebSocket"""
    while True:
        try:
            if bot_manager and api_status['status'] == 'running':
                # Lấy thông tin tổng hợp
                summary = get_system_summary()
                
                # Broadcast qua Socket.IO
                socketio.emit('system_update', {
                    'timestamp': datetime.now().isoformat(),
                    'summary': summary,
                    'queue_info': bot_manager.bot_coordinator.get_queue_info() if bot_manager else {},
                    'active_coins': bot_manager.coin_manager.get_active_coins() if bot_manager else []
                })
                
                # Broadcast vị thế đang mở
                positions = get_open_positions()
                if positions:
                    socketio.emit('positions_update', {
                        'positions': positions,
                        'count': len(positions)
                    })
                
                # Broadcast bot status
                bots_status = get_all_bots_status()
                socketio.emit('bots_update', {
                    'bots': bots_status,
                    'total': len(bots_status)
                })
                
            time.sleep(5)  # Update mỗi 5 giây
            
        except Exception as e:
            logger.error(f"❌ Lỗi broadcast: {str(e)}")
            time.sleep(10)

# ========== HÀM TIỆN ÍCH DATABASE ==========
def get_database_connection():
    """Lấy kết nối database với RealDictCursor"""
    try:
        database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/trading_bot')
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://")
        
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Lỗi kết nối database: {str(e)}")
        return None

# ========== API ROUTES ==========

@app.route('/')
def serve_react():
    """Phục vụ React app"""
    return render_template('index.html')

@app.route('/api/health', methods=['GET'])
def health_check():
    """Kiểm tra trạng thái hệ thống"""
    return jsonify({
        'status': 'healthy' if bot_manager else 'disconnected',
        'bot_manager_running': api_status['status'] == 'running',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if get_database_connection() else 'disconnected',
        'system_info': api_status
    })

@app.route('/api/system/start', methods=['POST'])
def start_system():
    """Khởi động hệ thống"""
    global bot_manager
    
    if api_status['status'] == 'running':
        return jsonify({'success': True, 'message': 'Hệ thống đã chạy'})
    
    try:
        success = initialize_bot_manager()
        if success:
            api_status['status'] = 'running'
            api_status['start_time'] = datetime.now().isoformat()
            return jsonify({
                'success': True,
                'message': '✅ Hệ thống đã khởi động thành công!'
            })
        else:
            return jsonify({
                'success': False,
                'message': '❌ Không thể khởi động hệ thống'
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Lỗi khởi động hệ thống: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Lỗi: {str(e)}'
        }), 500

@app.route('/api/system/stop', methods=['POST'])
def stop_system():
    """Dừng hệ thống"""
    global bot_manager
    
    if not bot_manager:
        return jsonify({'success': True, 'message': 'Hệ thống đã dừng'})
    
    try:
        bot_manager.stop_all()
        bot_manager = None
        api_status['status'] = 'stopped'
        
        return jsonify({
            'success': True,
            'message': '✅ Hệ thống đã dừng thành công!'
        })
        
    except Exception as e:
        logger.error(f"❌ Lỗi dừng hệ thống: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Lỗi: {str(e)}'
        }), 500

@app.route('/api/system/summary', methods=['GET'])
def get_system_summary():
    """Lấy thông tin tổng quan hệ thống"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            # Tổng số bot
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_bots,
                    COUNT(CASE WHEN status = 'running' THEN 1 END) as running_bots,
                    COUNT(CASE WHEN bot_mode = 'static' THEN 1 END) as static_bots,
                    COUNT(CASE WHEN bot_mode = 'dynamic' THEN 1 END) as dynamic_bots
                FROM bot_configs 
                WHERE deleted_at IS NULL
            """)
            bot_stats = cursor.fetchone()
            
            # Vị thế đang mở
            cursor.execute("""
                SELECT COUNT(*) as open_positions,
                       SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as long_positions,
                       SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as short_positions
                FROM bot_positions 
                WHERE status = 'open'
            """)
            position_stats = cursor.fetchone()
            
            # Thống kê PnL
            cursor.execute("""
                SELECT 
                    SUM(total_trades) as total_trades,
                    SUM(winning_trades) as winning_trades,
                    SUM(losing_trades) as losing_trades,
                    SUM(total_pnl) as total_pnl,
                    COALESCE(AVG(total_pnl), 0) as avg_pnl_per_trade
                FROM bot_statistics
            """)
            pnl_stats = cursor.fetchone()
            
            # Coin đang hoạt động
            cursor.execute("""
                SELECT DISTINCT symbol 
                FROM bot_positions 
                WHERE status = 'open' 
                ORDER BY symbol
            """)
            active_coins = [row['symbol'] for row in cursor.fetchall()]
            
            # Hàng đợi thông tin
            queue_info = bot_manager.bot_coordinator.get_queue_info() if bot_manager else {}
            
            conn.close()
            
            # Tổng hợp
            summary = {
                'bot_statistics': bot_stats,
                'position_statistics': position_stats,
                'pnl_statistics': pnl_stats,
                'active_coins': active_coins,
                'queue_info': queue_info,
                'system_status': api_status,
                'timestamp': datetime.now().isoformat()
            }
            
            return jsonify(summary)
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy system summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== BOT MANAGEMENT API ==========

@app.route('/api/bots', methods=['GET'])
def get_all_bots():
    """Lấy danh sách tất cả bot"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    bot_id, bot_mode, bot_type, symbol, leverage, percent,
                    tp, sl, roi_trigger, pyramiding_n, pyramiding_x,
                    dynamic_strategy, static_entry_mode, reverse_on_stop,
                    status, created_at, updated_at
                FROM bot_configs 
                WHERE deleted_at IS NULL
                ORDER BY created_at DESC
            """)
            bots = cursor.fetchall()
            
            # Thêm thông tin vị thế cho mỗi bot
            for bot in bots:
                cursor.execute("""
                    SELECT * FROM bot_positions 
                    WHERE bot_id = %s AND status = 'open'
                """, (bot['bot_id'],))
                positions = cursor.fetchall()
                bot['open_positions'] = positions
                bot['positions_count'] = len(positions)
                
                # Thống kê bot
                cursor.execute("""
                    SELECT * FROM bot_statistics 
                    WHERE bot_id = %s
                """, (bot['bot_id'],))
                stats = cursor.fetchone()
                bot['statistics'] = stats if stats else {}
            
            conn.close()
            return jsonify({'bots': bots})
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy danh sách bot: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bots/<bot_id>', methods=['GET'])
def get_bot_details(bot_id):
    """Lấy thông tin chi tiết bot"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            # Thông tin bot
            cursor.execute("""
                SELECT * FROM bot_configs 
                WHERE bot_id = %s AND deleted_at IS NULL
            """, (bot_id,))
            bot = cursor.fetchone()
            
            if not bot:
                conn.close()
                return jsonify({'error': 'Bot not found'}), 404
            
            # Vị thế đang mở
            cursor.execute("""
                SELECT * FROM bot_positions 
                WHERE bot_id = %s AND status = 'open'
                ORDER BY opened_at DESC
            """, (bot_id,))
            positions = cursor.fetchall()
            
            # Lịch sử giao dịch
            cursor.execute("""
                SELECT * FROM trade_history 
                WHERE bot_id = %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (bot_id,))
            trade_history = cursor.fetchall()
            
            # Thống kê
            cursor.execute("""
                SELECT * FROM bot_statistics 
                WHERE bot_id = %s
            """, (bot_id,))
            statistics = cursor.fetchone()
            
            conn.close()
            
            return jsonify({
                'bot': bot,
                'positions': positions,
                'trade_history': trade_history,
                'statistics': statistics if statistics else {}
            })
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy bot details: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bots', methods=['POST'])
def create_bot():
    """Tạo bot mới"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['bot_mode', 'leverage', 'percent', 'tp', 'sl']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing field: {field}'}), 400
        
        if not bot_manager:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        # Chuẩn bị parameters
        bot_params = {
            'bot_mode': data['bot_mode'],
            'bot_type': data.get('bot_type', 'custom'),
            'lev': int(data['leverage']),
            'percent': float(data['percent']),
            'tp': float(data['tp']),
            'sl': float(data.get('sl', 0)),
            'roi_trigger': float(data.get('roi_trigger', 0)) if data.get('roi_trigger') else None,
            'bot_count': int(data.get('bot_count', 1))
        }
        
        # Bot tĩnh cần symbol
        if data['bot_mode'] == 'static':
            if 'symbol' not in data:
                return jsonify({'error': 'Static bot requires symbol'}), 400
            bot_params['symbol'] = data['symbol']
            bot_params['static_entry_mode'] = data.get('static_entry_mode', 'signal')
        else:
            bot_params['dynamic_strategy'] = data.get('dynamic_strategy', 'volume')
        
        # Pyramiding
        bot_params['pyramiding_n'] = int(data.get('pyramiding_n', 0))
        bot_params['pyramiding_x'] = float(data.get('pyramiding_x', 0))
        
        # Reverse on stop
        bot_params['reverse_on_stop'] = data.get('reverse_on_stop', False)
        
        # Gọi BotManager để tạo bot
        success = bot_manager.add_bot(**bot_params)
        
        if success:
            # Emit socket event
            socketio.emit('bot_created', {
                'bot_id': f"{data['bot_mode']}_{datetime.now().timestamp()}",
                'params': bot_params,
                'timestamp': datetime.now().isoformat()
            })
            
            return jsonify({
                'success': True,
                'message': '✅ Bot created successfully!'
            })
        else:
            return jsonify({
                'success': False,
                'message': '❌ Failed to create bot'
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Lỗi tạo bot: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bots/<bot_id>/stop', methods=['POST'])
def stop_bot(bot_id):
    """Dừng bot"""
    try:
        if not bot_manager:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        success = bot_manager.stop_bot(bot_id)
        
        if success:
            # Emit socket event
            socketio.emit('bot_stopped', {
                'bot_id': bot_id,
                'timestamp': datetime.now().isoformat()
            })
            
            return jsonify({
                'success': True,
                'message': f'✅ Bot {bot_id} stopped successfully!'
            })
        else:
            return jsonify({
                'success': False,
                'message': f'❌ Bot {bot_id} not found'
            }), 404
            
    except Exception as e:
        logger.error(f"❌ Lỗi dừng bot: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/bots/stop-all', methods=['POST'])
def stop_all_bots():
    """Dừng tất cả bot"""
    try:
        if not bot_manager:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        stopped_count = len(bot_manager.bots)
        bot_manager.stop_all()
        
        # Emit socket event
        socketio.emit('all_bots_stopped', {
            'count': stopped_count,
            'timestamp': datetime.now().isoformat()
        })
        
        return jsonify({
            'success': True,
            'message': f'✅ Stopped {stopped_count} bots successfully!'
        })
            
    except Exception as e:
        logger.error(f"❌ Lỗi dừng tất cả bot: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== COIN MANAGEMENT API ==========

@app.route('/api/coins', methods=['GET'])
def get_active_coins():
    """Lấy danh sách coin đang hoạt động"""
    try:
        if not bot_manager:
            return jsonify({'coins': []})
            
        coins = bot_manager.coin_manager.get_active_coins()
        
        # Lấy thông tin chi tiết từ database
        conn = get_database_connection()
        if conn:
            with conn.cursor() as cursor:
                coin_details = []
                for coin in coins:
                    cursor.execute("""
                        SELECT 
                            bp.symbol, bp.side, bp.entry_price, bp.quantity,
                            bp.current_price, bp.roi, bp.opened_at,
                            bc.bot_id, bc.bot_mode, bc.leverage
                        FROM bot_positions bp
                        JOIN bot_configs bc ON bp.bot_id = bc.bot_id
                        WHERE bp.symbol = %s AND bp.status = 'open'
                    """, (coin,))
                    positions = cursor.fetchall()
                    
                    if positions:
                        coin_details.append({
                            'symbol': coin,
                            'positions': positions,
                            'total_positions': len(positions),
                            'total_quantity': sum(p['quantity'] for p in positions)
                        })
                
                conn.close()
                return jsonify({'coins': coin_details})
        
        return jsonify({'coins': []})
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy active coins: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/coins/<symbol>/stop', methods=['POST'])
def stop_coin(symbol):
    """Dừng coin trên tất cả bot"""
    try:
        if not bot_manager:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        success = bot_manager.stop_coin(symbol)
        
        if success:
            # Emit socket event
            socketio.emit('coin_stopped', {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat()
            })
            
            return jsonify({
                'success': True,
                'message': f'✅ Coin {symbol} stopped successfully!'
            })
        else:
            return jsonify({
                'success': False,
                'message': f'❌ Coin {symbol} not found'
            }), 404
            
    except Exception as e:
        logger.error(f"❌ Lỗi dừng coin: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/coins/stop-all', methods=['POST'])
def stop_all_coins():
    """Dừng tất cả coin"""
    try:
        if not bot_manager:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        stopped_count = bot_manager.stop_all_coins()
        
        # Emit socket event
        socketio.emit('all_coins_stopped', {
            'count': stopped_count,
            'timestamp': datetime.now().isoformat()
        })
        
        return jsonify({
            'success': True,
            'message': f'✅ Stopped {stopped_count} coins successfully!'
        })
            
    except Exception as e:
        logger.error(f"❌ Lỗi dừng tất cả coin: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== POSITIONS API ==========

@app.route('/api/positions', methods=['GET'])
def get_open_positions():
    """Lấy vị thế đang mở"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'positions': []})
            
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    bp.*,
                    bc.bot_mode, bc.bot_type, bc.leverage,
                    bc.percent, bc.tp, bc.sl, bc.roi_trigger
                FROM bot_positions bp
                JOIN bot_configs bc ON bp.bot_id = bc.bot_id
                WHERE bp.status = 'open'
                ORDER BY bp.last_update DESC
            """)
            positions = cursor.fetchall()
            
            # Tính toán PnL và ROI
            for pos in positions:
                if pos['entry_price'] and pos['current_price']:
                    quantity = pos['quantity']
                    entry = pos['entry_price']
                    current = pos['current_price']
                    
                    if pos['side'] == 'BUY':
                        pnl = (current - entry) * quantity
                    else:
                        pnl = (entry - current) * quantity
                    
                    invested = entry * quantity / pos['leverage']
                    if invested > 0:
                        roi = (pnl / invested) * 100
                    else:
                        roi = 0
                    
                    pos['pnl'] = pnl
                    pos['roi'] = roi
                    pos['unrealized_pnl'] = pnl
            
            conn.close()
            return jsonify({'positions': positions})
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy positions: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== TRADE HISTORY API ==========

@app.route('/api/trades', methods=['GET'])
def get_trade_history():
    """Lấy lịch sử giao dịch"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'trades': []})
            
        with conn.cursor() as cursor:
            # Lấy parameters từ query
            limit = request.args.get('limit', default=100, type=int)
            offset = request.args.get('offset', default=0, type=int)
            bot_id = request.args.get('bot_id')
            symbol = request.args.get('symbol')
            
            query = """
                SELECT 
                    th.*,
                    bc.bot_mode, bc.bot_type
                FROM trade_history th
                JOIN bot_configs bc ON th.bot_id = bc.bot_id
                WHERE 1=1
            """
            params = []
            
            if bot_id:
                query += " AND th.bot_id = %s"
                params.append(bot_id)
            
            if symbol:
                query += " AND th.symbol = %s"
                params.append(symbol)
            
            query += " ORDER BY th.created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            trades = cursor.fetchall()
            
            # Tổng số records
            count_query = """
                SELECT COUNT(*) as total 
                FROM trade_history th
                WHERE 1=1
            """
            count_params = []
            
            if bot_id:
                count_query += " AND th.bot_id = %s"
                count_params.append(bot_id)
            
            if symbol:
                count_query += " AND th.symbol = %s"
                count_params.append(symbol)
            
            cursor.execute(count_query, count_params)
            total = cursor.fetchone()['total']
            
            conn.close()
            
            return jsonify({
                'trades': trades,
                'pagination': {
                    'total': total,
                    'limit': limit,
                    'offset': offset,
                    'has_more': (offset + len(trades)) < total
                }
            })
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy trade history: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== STATISTICS API ==========

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Lấy thống kê hệ thống"""
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            # Thống kê tổng
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT bot_id) as total_bots,
                    SUM(total_trades) as total_trades,
                    SUM(winning_trades) as winning_trades,
                    SUM(losing_trades) as losing_trades,
                    SUM(total_pnl) as total_pnl,
                    COALESCE(AVG(total_pnl), 0) as avg_pnl_per_bot
                FROM bot_statistics
            """)
            stats = cursor.fetchone()
            
            # Thống kê theo ngày
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as trades_count,
                    SUM(pnl) as daily_pnl,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_days,
                    SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losing_days
                FROM trade_history
                WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            """)
            daily_stats = cursor.fetchall()
            
            # Thống kê theo bot
            cursor.execute("""
                SELECT 
                    bs.bot_id,
                    bc.bot_mode,
                    bc.bot_type,
                    bs.total_trades,
                    bs.winning_trades,
                    bs.losing_trades,
                    bs.total_pnl,
                    COALESCE(bs.total_pnl / NULLIF(bs.total_trades, 0), 0) as avg_pnl_per_trade,
                    COALESCE(bs.winning_trades * 100.0 / NULLIF(bs.total_trades, 0), 0) as win_rate
                FROM bot_statistics bs
                JOIN bot_configs bc ON bs.bot_id = bc.bot_id
                ORDER BY bs.total_pnl DESC
            """)
            bot_stats = cursor.fetchall()
            
            conn.close()
            
            return jsonify({
                'overall': stats,
                'daily_stats': daily_stats,
                'bot_stats': bot_stats,
                'timestamp': datetime.now().isoformat()
            })
            
    except Exception as e:
        logger.error(f"❌ Lỗi lấy statistics: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== BALANCE API ==========

@app.route('/api/balance', methods=['GET'])
def get_balance_info():
    """Lấy thông tin số dư"""
    try:
        if not bot_manager or not bot_manager.api_key:
            return jsonify({'error': 'Bot manager not initialized'}), 400
            
        from trading_bot_lib_part1 import get_total_and_available_balance, get_margin_safety_info
        
        total_balance, available_balance = get_total_and_available_balance(
            bot_manager.api_key, 
            bot_manager.api_secret
        )
        
        margin_balance, maint_margin, ratio = get_margin_safety_info(
            bot_manager.api_key,
            bot_manager.api_secret
        )
        
        return jsonify({
            'total_balance': total_balance or 0,
            'available_balance': available_balance or 0,
            'margin_balance': margin_balance or 0,
            'maint_margin': maint_margin or 0,
            'margin_ratio': ratio or 0,
            'is_safe': ratio and ratio > 1.15,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy balance: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== QUEUE API ==========

@app.route('/api/queue', methods=['GET'])
def get_queue_info():
    """Lấy thông tin hàng đợi"""
    try:
        if not bot_manager:
            return jsonify({'queue': {}})
            
        queue_info = bot_manager.bot_coordinator.get_queue_info()
        return jsonify({'queue': queue_info})
        
    except Exception as e:
        logger.error(f"❌ Lỗi lấy queue info: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ========== SOCKET.IO EVENTS ==========

@socketio.on('connect')
def handle_connect():
    """Xử lý khi client kết nối"""
    logger.info(f"Client connected: {request.sid}")
    emit('connected', {
        'message': 'Connected to Trading Bot API',
        'timestamp': datetime.now().isoformat()
    })

@socketio.on('disconnect')
def handle_disconnect():
    """Xử lý khi client ngắt kết nối"""
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('subscribe')
def handle_subscribe(data):
    """Subscribe to real-time updates"""
    channels = data.get('channels', [])
    logger.info(f"Client {request.sid} subscribed to: {channels}")
    emit('subscribed', {
        'channels': channels,
        'timestamp': datetime.now().isoformat()
    })

# ========== HÀM CHẠY SERVER ==========

def run_api_server(host='0.0.0.0', port=5000, debug=False):
    """Chạy REST API server"""
    # Khởi tạo BotManager
    if not initialize_bot_manager():
        logger.warning("⚠️ BotManager khởi tạo thất bại, API server vẫn chạy")
    
    logger.info(f"🚀 REST API Server đang chạy tại: http://{host}:{port}")
    logger.info("📡 WebSocket sẵn sàng tại: ws://localhost:5000")
    
    # Chạy server
    socketio.run(app, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)

if __name__ == '__main__':
    run_api_server()
