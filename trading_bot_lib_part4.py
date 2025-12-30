# trading_bot_lib_part4_web.py
# PHẦN 4: WEB API (FastAPI) CHO REACT - CHẠY SONG SONG VỚI PART 3
# - Dùng chung DatabaseManager (db_manager) để lấy bot configs / positions / history
# - Nếu truyền vào BotManager instance (part3) thì có thể điều khiển bot trực tiếp
# - Nếu không truyền BotManager, API sẽ chỉ READ-ONLY (an toàn tránh double trade)

import os
import time
import threading
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from trading_bot_lib_part1 import logger, db_manager


# =========================
# Pydantic Models (Request)
# =========================

class AddBotRequest(BaseModel):
    bot_mode: str = Field(..., description="static | dynamic")
    bot_type: str = Field(..., description="Tên bot/chiến lược (tuỳ logic part3)")
    lev: int
    percent: float
    tp: float
    sl: float
    roi_trigger: float = 0
    symbol: Optional[str] = None
    bot_count: int = 1

    # extra options (phù hợp part3)
    dynamic_strategy: str = "volume"       # volume | volatility
    static_entry_mode: str = "signal"      # signal | reverse | wait
    reverse_on_stop: bool = False
    pyramiding_n: int = 0
    pyramiding_x: float = 0


class StopBotRequest(BaseModel):
    bot_id: str


class UpdateBotStatusRequest(BaseModel):
    bot_id: str
    status: str = Field(..., description="running | stopped")


# =========================
# Web API Bridge
# =========================

class WebAPIBridge:
    """
    Cầu nối Web API <-> BotManager.

    - Nếu manager != None: có thể điều khiển bot trực tiếp bằng hàm part3.
    - Nếu manager == None: chỉ đọc DB (không điều khiển) để tránh chạy song song 2 BotManager.
    """
    def __init__(self, manager=None):
        self.manager = manager
        self.start_time = time.time()

    def can_control(self) -> bool:
        return self.manager is not None

    # -------- READ from DB --------
    def get_bots(self) -> List[Dict[str, Any]]:
        return db_manager.get_all_bots()

    def get_open_positions(self) -> List[Dict[str, Any]]:
        return db_manager.get_open_positions()

    def get_trade_history(self, bot_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return db_manager.get_trade_history(bot_id=bot_id, limit=limit)

    def get_statistics(self, bot_id: Optional[str] = None) -> Dict[str, Any]:
        return db_manager.get_statistics(bot_id=bot_id)

    # -------- CONTROL (needs manager) --------
    def add_bot(self, payload: AddBotRequest) -> Dict[str, Any]:
        if not self.can_control():
            raise RuntimeError("CONTROL_DISABLED")

        ok = self.manager.add_bot(
            bot_mode=payload.bot_mode,
            bot_type=payload.bot_type,
            lev=payload.lev,
            percent=payload.percent,
            tp=payload.tp,
            sl=payload.sl,
            roi_trigger=payload.roi_trigger,
            symbol=payload.symbol,
            bot_count=payload.bot_count,
            dynamic_strategy=payload.dynamic_strategy,
            static_entry_mode=payload.static_entry_mode,
            reverse_on_stop=payload.reverse_on_stop,
            pyramiding_n=payload.pyramiding_n,
            pyramiding_x=payload.pyramiding_x,
        )
        return {"ok": bool(ok)}

    def stop_bot(self, bot_id: str) -> Dict[str, Any]:
        if not self.can_control():
            raise RuntimeError("CONTROL_DISABLED")

        # Nếu part3 của bạn có hàm stop_bot(bot_id) thì gọi.
        # Nếu chưa có, bạn có thể implement trong BotManager rồi dùng ở đây.
        if hasattr(self.manager, "stop_bot"):
            ok = self.manager.stop_bot(bot_id)
            return {"ok": bool(ok)}

        # fallback: update DB status
        ok = db_manager.update_bot_status(bot_id, "stopped")
        return {"ok": bool(ok), "note": "Manager has no stop_bot(); used DB status update only."}


# =========================
# FastAPI App Factory
# =========================

def create_app(manager=None) -> FastAPI:
    """
    Tạo FastAPI app.
    React gọi vào đây: /api/...
    """
    bridge = WebAPIBridge(manager=manager)

    app = FastAPI(title="Trading Bot Web API", version="1.0.0")

    # CORS cho React dev/prod (bạn chỉnh domain sau)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # dev cho dễ; lên prod nên giới hạn domain React
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    def health():
        return {
            "ok": True,
            "uptime_sec": int(time.time() - bridge.start_time),
            "control_enabled": bridge.can_control(),
        }

    # ---------- READ ----------
    @app.get("/api/bots")
    def api_get_bots():
        return {"bots": bridge.get_bots()}

    @app.get("/api/positions")
    def api_get_positions():
        return {"positions": bridge.get_open_positions()}

    @app.get("/api/stats")
    def api_get_stats():
        return {"stats": bridge.get_statistics()}

    @app.get("/api/stats/{bot_id}")
    def api_get_bot_stats(bot_id: str):
        return {"stats": bridge.get_statistics(bot_id=bot_id)}

    @app.get("/api/history")
    def api_get_history(limit: int = 200, bot_id: Optional[str] = None):
        return {"history": bridge.get_trade_history(bot_id=bot_id, limit=limit)}

    # ---------- CONTROL ----------
    @app.post("/api/bot/add")
    def api_add_bot(payload: AddBotRequest):
        try:
            return bridge.add_bot(payload)
        except RuntimeError as e:
            if str(e) == "CONTROL_DISABLED":
                raise HTTPException(
                    status_code=403,
                    detail="Web API đang chạy READ-ONLY (không có BotManager instance). "
                           "Hãy chạy Web cùng process với BotManager để bật điều khiển."
                )
            raise

    @app.post("/api/bot/stop")
    def api_stop_bot(payload: StopBotRequest):
        try:
            return bridge.stop_bot(payload.bot_id)
        except RuntimeError as e:
            if str(e) == "CONTROL_DISABLED":
                raise HTTPException(status_code=403, detail="READ-ONLY mode.")
            raise

    @app.post("/api/bot/status")
    def api_update_status(payload: UpdateBotStatusRequest):
        # cái này có thể dùng cả khi READ-ONLY (vì chỉ update DB status)
        ok = db_manager.update_bot_status(payload.bot_id, payload.status)
        return {"ok": bool(ok)}

    return app


# =========================
# Helper: Run uvicorn in thread (để chạy song song part3)
# =========================

def start_web_in_thread(manager=None, host="0.0.0.0", port: Optional[int] = None):
    """
    Gọi hàm này từ main (cùng process với BotManager) để chạy web song song.
    """
    import uvicorn

    if port is None:
        port = int(os.getenv("PORT", "8000"))

    app = create_app(manager=manager)

    def _run():
        logger.info(f"🌐 Web API running on http://{host}:{port}")
        uvicorn.run(app, host=host, port=port, log_level="info")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t
