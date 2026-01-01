# trading_bot_lib_part4.py
# PHẦN 4: REST API SERVER CHO REACT & TELEGRAM SONG SONG (FIX APP CONTEXT + DATA LAYER)

from trading_bot_lib_part3 import BotManager
from trading_bot_lib_part1 import db_manager, logger

import os
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit

import psycopg2
from psycopg2.extras import RealDictCursor


# ================== FLASK APP ==================
# Nếu bạn có build React rồi, set đúng path build ở đây.
REACT_BUILD_DIR = os.getenv("REACT_BUILD_DIR", "../react-app/build")
REACT_STATIC_DIR = os.path.join(REACT_BUILD_DIR, "static")

app = Flask(
    __name__,
    static_folder=REACT_STATIC_DIR,
    template_folder=REACT_BUILD_DIR
)
CORS(app)

# SocketIO: ưu tiên eventlet nếu có, fallback threading
_socket_async_mode = os.getenv("SOCKETIO_ASYNC_MODE", "").strip().lower()
if not _socket_async_mode:
    try:
        import eventlet  # noqa
        _socket_async_mode = "eventlet"
    except Exception:
        _socket_async_mode = "threading"

socketio = SocketIO(app, cors_allowed_origins="*", async_mode=_socket_async_mode)

# ================== GLOBALS ==================
bot_manager: Optional[BotManager] = None

api_status: Dict[str, Any] = {
    "status": "stopped",
    "start_time": None,
    "total_bots": 0,
    "running_bots": 0,
    "total_balance": 0,
    "total_pnl": 0
}

_broadcast_thread_started = False
_broadcast_lock = threading.Lock()


# ================== DB CONNECTION ==================
def get_database_connection():
    """Lấy kết nối DB với RealDictCursor"""
    try:
        database_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/trading_bot")
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://")

        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"❌ Lỗi kết nối database: {str(e)}")
        return None


# ================== BOT MANAGER INIT ==================
def initialize_bot_manager() -> bool:
    """Khởi tạo BotManager từ biến môi trường"""
    global bot_manager, _broadcast_thread_started

    try:
        api_key = os.getenv("BINANCE_API_KEY")
        # fallback để khỏi dính nhầm tên biến
        api_secret = os.getenv("BINANCE_API_SECRET") or os.getenv("BINANCE_SECRET_KEY")
        telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

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

        api_status.update({
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "total_bots": len(bot_manager.bots) if bot_manager else 0
        })

        # Start broadcast thread chỉ 1 lần
        with _broadcast_lock:
            if not _broadcast_thread_started:
                threading.Thread(target=broadcast_updates, daemon=True).start()
                _broadcast_thread_started = True

        logger.info("✅ BotManager đã sẵn sàng!")
        return True

    except Exception as e:
        logger.error(f"❌ Lỗi khởi tạo BotManager: {str(e)}")
        return False


# ================== DATA LAYER (NO jsonify / NO request) ==================
def get_system_summary_data() -> Dict[str, Any]:
    """Lấy system summary (trả về dict thuần)"""
    conn = get_database_connection()
    if not conn:
        return {"error": "Database connection failed"}

    try:
        with conn.cursor() as cursor:
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

            cursor.execute("""
                SELECT COUNT(*) as open_positions,
                       SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as long_positions,
                       SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as short_positions
                FROM bot_positions 
                WHERE status = 'open'
            """)
            position_stats = cursor.fetchone()

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

            cursor.execute("""
                SELECT DISTINCT symbol 
                FROM bot_positions 
                WHERE status = 'open' 
                ORDER BY symbol
            """)
            active_coins = [row["symbol"] for row in cursor.fetchall()]

        queue_info = bot_manager.bot_coordinator.get_queue_info() if bot_manager else {}

        return {
            "bot_statistics": bot_stats,
            "position_statistics": position_stats,
            "pnl_statistics": pnl_stats,
            "active_coins": active_coins,
            "queue_info": queue_info,
            "system_status": api_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ Lỗi lấy system summary: {str(e)}")
        return {"error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_open_positions_data() -> List[Dict[str, Any]]:
    """Lấy vị thế đang mở (list dict)"""
    conn = get_database_connection()
    if not conn:
        return []

    try:
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

        # Tính PnL/ROI nếu thiếu (giữ logic nhẹ)
        for pos in positions:
            try:
                entry = pos.get("entry_price")
                current = pos.get("current_price")
                qty = pos.get("quantity") or 0
                side = (pos.get("side") or "").upper()

                if entry and current and qty:
                    if side == "BUY":
                        pnl = (current - entry) * qty
                    else:
                        pnl = (entry - current) * qty
                    pos["pnl_calc"] = pnl
            except Exception:
                pass

        return positions

    except Exception as e:
        logger.error(f"❌ Lỗi lấy open positions: {str(e)}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_all_bots_status_data() -> List[Dict[str, Any]]:
    """Lấy status bot (list dict)"""
    conn = get_database_connection()
    if not conn:
        return []

    try:
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

        # Bổ sung positions_count nhanh
        if bots:
            bot_ids = [b["bot_id"] for b in bots]
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT bot_id, COUNT(*) as positions_count
                    FROM bot_positions
                    WHERE status='open' AND bot_id = ANY(%s)
                    GROUP BY bot_id
                """, (bot_ids,))
                counts = {row["bot_id"]: row["positions_count"] for row in cursor.fetchall()}

            for b in bots:
                b["positions_count"] = counts.get(b["bot_id"], 0)

        return bots

    except Exception as e:
        logger.error(f"❌ Lỗi lấy bots status: {str(e)}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ================== BROADCAST THREAD ==================
def broadcast_updates():
    """Broadcast realtime qua Socket.IO (an toàn context)"""
    while True:
        try:
            with app.app_context():
                if bot_manager and api_status["status"] == "running":
                    summary = get_system_summary_data()
                    socketio.emit("system_update", {
                        "timestamp": datetime.now().isoformat(),
                        "summary": summary,
                        "queue_info": bot_manager.bot_coordinator.get_queue_info() if bot_manager else {},
                        "active_coins": bot_manager.coin_manager.get_active_coins() if bot_manager else []
                    })

                    positions = get_open_positions_data()
                    socketio.emit("positions_update", {
                        "positions": positions,
                        "count": len(positions)
                    })

                    bots_status = get_all_bots_status_data()
                    socketio.emit("bots_update", {
                        "bots": bots_status,
                        "total": len(bots_status)
                    })

            time.sleep(5)

        except Exception as e:
            logger.error(f"❌ Lỗi broadcast: {str(e)}")
            time.sleep(10)


# ================== REACT SERVE ==================
@app.route("/")
def serve_react():
    """Phục vụ React build nếu có, không có thì trả thông báo"""
    index_path = os.path.join(REACT_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return render_template("index.html")
    return jsonify({
        "message": "React build not found. Set REACT_BUILD_DIR or deploy build folder.",
        "timestamp": datetime.now().isoformat()
    })


@app.route("/static/<path:path>")
def serve_static(path):
    """Serve static for React if needed"""
    if os.path.exists(REACT_STATIC_DIR):
        return send_from_directory(REACT_STATIC_DIR, path)
    return jsonify({"error": "Static folder not found"}), 404


# ================== HEALTH ==================
@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy" if bot_manager else "disconnected",
        "bot_manager_running": api_status["status"] == "running",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if get_database_connection() else "disconnected",
        "system_info": api_status
    })


# ================== SYSTEM CONTROL ==================
@app.route("/api/system/start", methods=["POST"])
def start_system():
    global bot_manager

    if api_status["status"] == "running":
        return jsonify({"success": True, "message": "Hệ thống đã chạy"})

    try:
        success = initialize_bot_manager()
        if success:
            api_status["status"] = "running"
            api_status["start_time"] = datetime.now().isoformat()
            return jsonify({"success": True, "message": "✅ Hệ thống đã khởi động thành công!"})
        return jsonify({"success": False, "message": "❌ Không thể khởi động hệ thống"}), 500
    except Exception as e:
        logger.error(f"❌ Lỗi khởi động hệ thống: {str(e)}")
        return jsonify({"success": False, "message": f"Lỗi: {str(e)}"}), 500


@app.route("/api/system/stop", methods=["POST"])
def stop_system():
    global bot_manager

    if not bot_manager:
        api_status["status"] = "stopped"
        return jsonify({"success": True, "message": "Hệ thống đã dừng"})

    try:
        bot_manager.stop_all()
        bot_manager = None
        api_status["status"] = "stopped"
        return jsonify({"success": True, "message": "✅ Hệ thống đã dừng thành công!"})
    except Exception as e:
        logger.error(f"❌ Lỗi dừng hệ thống: {str(e)}")
        return jsonify({"success": False, "message": f"Lỗi: {str(e)}"}), 500

@app.route("/api/bots/<bot_id>/delete", methods=["POST"])
def delete_bot(bot_id):
    try:
        hard = request.args.get("hard", "0") == "1"

        # Nếu bot_manager đang chạy -> stop và xóa config
        if bot_manager:
            ok = bot_manager.stop_bot(bot_id, delete_config=True, hard_delete=hard)
        else:
            # Nếu hệ thống chưa khởi tạo bot_manager -> vẫn xóa config trực tiếp DB
            ok = db_manager.delete_bot_config(bot_id, hard=hard)

        if ok:
            socketio.emit("bot_deleted", {"bot_id": bot_id, "timestamp": datetime.now().isoformat()})
            return jsonify({"success": True, "message": f"🗑️ Deleted bot {bot_id} ({'hard' if hard else 'soft'})"})
        return jsonify({"success": False, "message": "❌ Delete failed"}), 500

    except Exception as e:
        logger.error(f"❌ Lỗi delete bot: {str(e)}")
        return jsonify({"error": str(e)}), 500



@app.route("/api/system/summary", methods=["GET"])
def get_system_summary():
    data = get_system_summary_data()
    if data.get("error"):
        return jsonify(data), 500
    return jsonify(data)


# ================== BOTS ==================
@app.route("/api/bots", methods=["GET"])
def get_all_bots():
    bots = get_all_bots_status_data()
    return jsonify({"bots": bots})


@app.route("/api/bots/<bot_id>", methods=["GET"])
def get_bot_details(bot_id):
    try:
        conn = get_database_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM bot_configs 
                WHERE bot_id = %s AND deleted_at IS NULL
            """, (bot_id,))
            bot = cursor.fetchone()
            if not bot:
                return jsonify({"error": "Bot not found"}), 404

            cursor.execute("""
                SELECT * FROM bot_positions 
                WHERE bot_id = %s AND status = 'open'
                ORDER BY opened_at DESC
            """, (bot_id,))
            positions = cursor.fetchall()

            cursor.execute("""
                SELECT * FROM trade_history 
                WHERE bot_id = %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (bot_id,))
            trade_history = cursor.fetchall()

            cursor.execute("""
                SELECT * FROM bot_statistics 
                WHERE bot_id = %s
            """, (bot_id,))
            statistics = cursor.fetchone()

        return jsonify({
            "bot": bot,
            "positions": positions,
            "trade_history": trade_history,
            "statistics": statistics if statistics else {}
        })

    except Exception as e:
        logger.error(f"❌ Lỗi lấy bot details: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route("/api/bots", methods=["POST"])
def create_bot():
    """
    Tạo bot mới (gọi BotManager.add_bot)
    Body JSON yêu cầu tối thiểu:
      bot_mode, leverage, percent, tp, sl
    Các field thêm:
      symbol, bot_type, roi_trigger, pyramiding_n, pyramiding_x,
      dynamic_strategy, static_entry_mode, reverse_on_stop, bot_count
    """
    try:
        if not bot_manager:
            return jsonify({"error": "Bot manager not initialized"}), 400

        data = request.get_json(force=True) or {}
        required_fields = ["bot_mode", "leverage", "percent", "tp", "sl"]
        for f in required_fields:
            if f not in data:
                return jsonify({"error": f"Missing field: {f}"}), 400

        bot_mode = data.get("bot_mode")
        bot_type = data.get("bot_type")
        symbol = data.get("symbol")

        lev = int(data.get("leverage"))
        percent = float(data.get("percent"))
        tp = float(data.get("tp"))
        sl = float(data.get("sl"))

        roi_trigger = data.get("roi_trigger")
        bot_count = int(data.get("bot_count", 1))

        kwargs = {
            "pyramiding_n": int(data.get("pyramiding_n", 0) or 0),
            "pyramiding_x": float(data.get("pyramiding_x", 0) or 0),
            "reverse_on_stop": bool(data.get("reverse_on_stop", False))
        }
        if data.get("dynamic_strategy") is not None:
            kwargs["dynamic_strategy"] = data.get("dynamic_strategy")
        if data.get("static_entry_mode") is not None:
            kwargs["static_entry_mode"] = data.get("static_entry_mode")

        ok = bot_manager.add_bot(
            bot_mode=bot_mode,
            bot_type=bot_type,
            symbol=symbol,
            lev=lev,
            percent=percent,
            tp=tp,
            sl=sl,
            roi_trigger=roi_trigger,
            bot_count=bot_count,
            **kwargs
        )

        if ok:
            socketio.emit("bot_created", {"timestamp": datetime.now().isoformat()})
            return jsonify({"success": True, "message": "✅ Tạo bot thành công"})
        return jsonify({"success": False, "message": "❌ Tạo bot thất bại"}), 500

    except Exception as e:
        logger.error(f"❌ Lỗi tạo bot: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bots/<bot_id>/stop", methods=["POST"])
def stop_bot(bot_id):
    try:
        if not bot_manager:
            return jsonify({"error": "Bot manager not initialized"}), 400

        success = bot_manager.stop_bot(bot_id)
        if success:
            socketio.emit("bot_stopped", {"bot_id": bot_id, "timestamp": datetime.now().isoformat()})
            return jsonify({"success": True, "message": f"✅ Stopped {bot_id} successfully!"})
        return jsonify({"success": False, "message": "❌ Bot not found"}), 404

    except Exception as e:
        logger.error(f"❌ Lỗi dừng bot: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bots/stop-all", methods=["POST"])
def stop_all_bots():
    try:
        if not bot_manager:
            return jsonify({"error": "Bot manager not initialized"}), 400

        bot_manager.stop_all()
        socketio.emit("all_bots_stopped", {"timestamp": datetime.now().isoformat()})
        return jsonify({"success": True, "message": "✅ Stopped all bots successfully!"})

    except Exception as e:
        logger.error(f"❌ Lỗi dừng tất cả bot: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================== COINS ==================
@app.route("/api/coins", methods=["GET"])
def get_active_coins():
    """Lấy coin đang hoạt động"""
    try:
        if not bot_manager:
            return jsonify({"coins": []})

        coins = bot_manager.coin_manager.get_active_coins()

        # Lấy chi tiết từ DB
        conn = get_database_connection()
        if not conn:
            return jsonify({"coins": []})

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
                        "symbol": coin,
                        "positions": positions,
                        "total_positions": len(positions),
                        "total_quantity": sum((p.get("quantity") or 0) for p in positions)
                    })

        return jsonify({"coins": coin_details})

    except Exception as e:
        logger.error(f"❌ Lỗi lấy active coins: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route("/api/coins/<symbol>/stop", methods=["POST"])
def stop_coin(symbol):
    try:
        if not bot_manager:
            return jsonify({"error": "Bot manager not initialized"}), 400

        success = bot_manager.stop_coin(symbol)
        if success:
            socketio.emit("coin_stopped", {"symbol": symbol, "timestamp": datetime.now().isoformat()})
            return jsonify({"success": True, "message": f"✅ Coin {symbol} stopped successfully!"})

        return jsonify({"success": False, "message": f"❌ Coin {symbol} not found"}), 404

    except Exception as e:
        logger.error(f"❌ Lỗi dừng coin: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/coins/stop-all", methods=["POST"])
def stop_all_coins():
    try:
        if not bot_manager:
            return jsonify({"error": "Bot manager not initialized"}), 400

        stopped_count = bot_manager.stop_all_coins()
        socketio.emit("all_coins_stopped", {"count": stopped_count, "timestamp": datetime.now().isoformat()})
        return jsonify({"success": True, "message": f"✅ Stopped {stopped_count} coins successfully!"})

    except Exception as e:
        logger.error(f"❌ Lỗi dừng tất cả coin: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================== POSITIONS ==================
@app.route("/api/positions", methods=["GET"])
def get_open_positions():
    positions = get_open_positions_data()
    return jsonify({"positions": positions})


# ================== TRADE HISTORY / STATS ==================
@app.route("/api/trades", methods=["GET"])
def get_trade_history():
    try:
        bot_id = request.args.get("bot_id")
        limit = int(request.args.get("limit", "100"))

        trades = db_manager.get_trade_history(bot_id=bot_id, limit=limit)
        return jsonify({"trades": trades})

    except Exception as e:
        logger.error(f"❌ Lỗi lấy trade history: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/statistics", methods=["GET"])
def get_statistics():
    try:
        bot_id = request.args.get("bot_id")
        stats = db_manager.get_statistics(bot_id=bot_id)
        return jsonify({"statistics": stats})

    except Exception as e:
        logger.error(f"❌ Lỗi lấy statistics: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================== BALANCE / MARGIN SAFETY ==================
@app.route("/api/balance", methods=["GET"])
def get_balance_api():
    """
    Trả về thông tin tổng/available/margin/maint + ratio
    Lưu ý: logic chi tiết lấy từ part1/part3 của bạn;
    Ở đây chỉ đọc DB nếu bạn đã lưu snapshot, còn nếu không có thì trả rỗng.
    """
    try:
        # Bạn có thể mở rộng: gọi bot_manager.get_balance() nếu có method.
        # Hiện tại: trả placeholder an toàn.
        return jsonify({
            "available_balance": 0,
            "margin_balance": 0,
            "maint_margin": 0,
            "margin_ratio": 0,
            "is_safe": False,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"❌ Lỗi lấy balance: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================== QUEUE ==================
@app.route("/api/queue", methods=["GET"])
def get_queue_info():
    try:
        if not bot_manager:
            return jsonify({"queue": {}})

        queue_info = bot_manager.bot_coordinator.get_queue_info()
        return jsonify({"queue": queue_info})

    except Exception as e:
        logger.error(f"❌ Lỗi lấy queue info: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ================== SOCKET EVENTS ==================
@socketio.on("connect")
def handle_connect():
    logger.info(f"Client connected: {request.sid}")
    emit("connected", {
        "message": "Connected to Trading Bot API",
        "timestamp": datetime.now().isoformat()
    })


@socketio.on("disconnect")
def handle_disconnect():
    logger.info(f"Client disconnected: {request.sid}")


@socketio.on("subscribe")
def handle_subscribe(data):
    channels = (data or {}).get("channels", [])
    logger.info(f"Client {request.sid} subscribed to: {channels}")
    emit("subscribed", {
        "channels": channels,
        "timestamp": datetime.now().isoformat()
    })


# ================== RUN SERVER / THREAD ==================
def run_api_server(host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
    """
    Chạy API server.
    Lưu ý: deploy Railway nên chạy bằng gunicorn (Procfile).
    """
    # Khởi tạo BotManager
    if not initialize_bot_manager():
        logger.warning("⚠️ BotManager khởi tạo thất bại, API server vẫn chạy")

    ws_url = f"ws://{host}:{port}"
    http_url = f"http://{host}:{port}"
    logger.info(f"🚀 REST API Server đang chạy tại: {http_url}")
    logger.info(f"📡 WebSocket sẵn sàng tại: {ws_url} (async_mode={_socket_async_mode})")

    # Nếu debug=False mà chạy trực tiếp, Flask sẽ warn dev server — vẫn ok để test.
    socketio.run(app, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)


def start_web_in_thread(host: str = "0.0.0.0", port: Optional[int] = None, debug: bool = False):
    """Dùng cho main.py: chạy web server trên thread nền"""
    if port is None:
        port = int(os.getenv("PORT", "5000"))

    t = threading.Thread(
        target=run_api_server,
        kwargs={"host": host, "port": port, "debug": debug},
        daemon=True
    )
    t.start()
    return t


if __name__ == "__main__":
    run_api_server(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
