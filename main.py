# main.py
import os
import json
import threading
import time
from dotenv import load_dotenv

# Web server + BotManager được khởi tạo từ ENV trong part4
from trading_bot_lib_part4 import run_api_server, initialize_bot_manager
import trading_bot_lib_part4 as part4  # để truy cập part4.bot_manager (global)
from trading_bot_lib_part1 import logger


load_dotenv()


def _mask(value: str) -> str:
    return "***" if value else "Không có"


def print_env_status():
    # Lưu ý: part3/part4 dùng BINANCE_SECRET_KEY (không phải BINANCE_SECRET_KEY)
    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_SECRET_KEY", "")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "")
    db_url = os.getenv("DATABASE_URL", "")

    print(f"BINANCE_API_KEY: {_mask(api_key)}")
    print(f"BINANCE_SECRET_KEY: {_mask(api_secret)}")
    print(f"TELEGRAM_BOT_TOKEN: {_mask(tg_token)}")
    print(f"TELEGRAM_CHAT_ID: {tg_chat if tg_chat else 'Không có'}")
    print(f"DATABASE_URL: {_mask(db_url)}")


def bootstrap_bots_from_env():
    """
    OPTIONAL: Nạp bot mẫu từ biến môi trường BOOTSTRAP_BOTS (JSON).
    Bạn có thể bỏ qua nếu bot đã được restore từ DB.

    Format gợi ý (list):
    [
      {
        "bot_mode": "static",
        "bot_type": "static",
        "symbol": "XRPUSDT",
        "lev": 50,
        "percent": 25,
        "tp": 200,
        "sl": 200,
        "roi_trigger": 100,
        "static_entry_mode": "signal",
        "reverse_on_stop": false,
        "pyramiding_n": 0,
        "pyramiding_x": 0,
        "bot_count": 1
      },
      {
        "bot_mode": "dynamic",
        "bot_type": "dynamic",
        "lev": 50,
        "percent": 25,
        "tp": 500,
        "sl": 0,
        "roi_trigger": 100,
        "dynamic_strategy": "volume",
        "reverse_on_stop": false,
        "pyramiding_n": 0,
        "pyramiding_x": 0,
        "bot_count": 2
      }
    ]
    """
    raw = os.getenv("BOOTSTRAP_BOTS", "").strip()
    if not raw:
        logger.info("ℹ️ Không có BOOTSTRAP_BOTS -> bỏ qua bootstrap bot mẫu.")
        return

    try:
        configs = json.loads(raw)
        if not isinstance(configs, list):
            logger.warning("⚠️ BOOTSTRAP_BOTS phải là JSON list -> bỏ qua.")
            return
    except Exception as e:
        logger.warning(f"⚠️ Lỗi parse BOOTSTRAP_BOTS: {e}")
        return

    if not part4.bot_manager:
        logger.warning("⚠️ BotManager chưa sẵn sàng -> không bootstrap bot.")
        return

    for cfg in configs:
        try:
            bot_mode = cfg.get("bot_mode", "static")
            bot_type = cfg.get("bot_type", "static")

            symbol = cfg.get("symbol")
            lev = int(cfg.get("lev", 20))
            percent = float(cfg.get("percent", 25))
            tp = float(cfg.get("tp", 200))
            sl = float(cfg.get("sl", 200))
            roi_trigger = cfg.get("roi_trigger", None)
            bot_count = int(cfg.get("bot_count", 1))

            # Các kwargs mở rộng (tùy bot)
            kwargs = {}
            if "static_entry_mode" in cfg:
                kwargs["static_entry_mode"] = cfg.get("static_entry_mode")
            if "dynamic_strategy" in cfg:
                kwargs["dynamic_strategy"] = cfg.get("dynamic_strategy")
            if "reverse_on_stop" in cfg:
                kwargs["reverse_on_stop"] = bool(cfg.get("reverse_on_stop"))
            if "pyramiding_n" in cfg:
                kwargs["pyramiding_n"] = int(cfg.get("pyramiding_n", 0))
            if "pyramiding_x" in cfg:
                kwargs["pyramiding_x"] = int(cfg.get("pyramiding_x", 0))

            ok = part4.bot_manager.add_bot(
                bot_mode=bot_mode,
                bot_type=bot_type,
                lev=lev,
                percent=percent,
                tp=tp,
                sl=sl,
                roi_trigger=roi_trigger,
                symbol=symbol,
                bot_count=bot_count,
                **kwargs
            )

            if ok:
                logger.info(f"✅ Bootstrap bot OK: mode={bot_mode} symbol={symbol} count={bot_count}")
            else:
                logger.warning(f"❌ Bootstrap bot FAIL: mode={bot_mode} symbol={symbol} count={bot_count}")

        except Exception as e:
            logger.warning(f"❌ Lỗi bootstrap bot: {e}")


def start_web_in_thread(host="0.0.0.0", port=None, debug=False):
    """
    Chạy web server (Flask/SocketIO) trên thread riêng.
    Railway sẽ cấp PORT qua env PORT.
    """
    if port is None:
        port = int(os.getenv("PORT", "5000"))

    t = threading.Thread(
        target=run_api_server,
        kwargs={"host": host, "port": port, "debug": debug},
        daemon=True
    )
    t.start()
    return t


def main():
    print_env_status()

    # Khởi tạo BotManager (restore bot từ DB + chạy telegram listener nếu có token/chat_id)
    ok = initialize_bot_manager()
    if not ok:
        logger.warning("⚠️ initialize_bot_manager() thất bại. Web vẫn có thể chạy nhưng bot_manager có thể None.")

    # OPTIONAL: bootstrap bot mẫu từ env
    bootstrap_bots_from_env()

    # Start web server song song
    web_thread = start_web_in_thread(debug=False)
    logger.info("🟢 Hệ thống đã chạy (Telegram + Web).")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("🛑 Nhận Ctrl+C, đang dừng hệ thống...")
    finally:
        # Dừng bot an toàn nếu có
        try:
            if part4.bot_manager:
                part4.bot_manager.stop_all()
        except Exception:
            pass


if __name__ == "__main__":
    main()

