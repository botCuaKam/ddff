/* =========================
   Helpers
========================= */
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function setPill(ok, text){
  const el = $("#pill-status");
  el.textContent = (ok ? "● Connected" : "● Disconnected") + " — " + text;
  el.style.color = ok ? "#93f7b2" : "#ff9aa4";
}

function badge(status){
  if (status === "running" || status === "open") return `<span class="badge badge--ok">● running</span>`;
  if (status === "stopped") return `<span class="badge badge--stop">● stopped</span>`;
  return `<span class="badge badge--warn">● ${status || "unknown"}</span>`;
}

async function api(path, options={}){
  const res = await fetch(path, {
    headers: {
      "Content-Type":"application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) throw new Error(data?.error || data?.msg || res.statusText);
  return data;
}

/* =========================
   Tabs
========================= */
function initTabs(){
  $$(".nav__item").forEach(btn=>{
    btn.addEventListener("click", ()=>{
      $$(".nav__item").forEach(x=>x.classList.remove("is-active"));
      btn.classList.add("is-active");

      const tab = btn.dataset.tab;
      $("#tab-bots").classList.toggle("is-active", tab==="bots");
      $("#tab-chart").classList.toggle("is-active", tab==="chart");
      $("#tab-logs").classList.toggle("is-active", tab==="logs");
    });
  });
}

/* =========================
   State (Bots + Summary)
========================= */
function renderState(state){
  const bots = state.bots || {};
  const botList = Object.values(bots);

  $("#sum-bots").textContent = String(state.bot_count ?? botList.length ?? "—");
  $("#sum-balance").textContent = (state.balance == null ? "N/A" : String(state.balance));
  $("#sum-ws").textContent = "WS: " + ((state.ws_symbols || []).join(", ") || "—");

  const q = state.queue || {};
  $("#sum-queue").textContent = q.current_bot_id || "Idle";
  $("#sum-queue-meta").textContent = q.queue_len != null ? `queue_len=${q.queue_len}` : "—";

  const tbody = $("#bots-tbody");
  if (!botList.length){
    tbody.innerHTML = `<tr><td colspan="7" class="muted">Chưa có bot nào</td></tr>`;
    return;
  }

  tbody.innerHTML = botList.map(b=>{
    const syms = (b.active_symbols || []).join(", ") || "—";
    return `
      <tr>
        <td><span style="font-family:var(--mono)">${b.bot_id}</span></td>
        <td>${badge(b.status)}</td>
        <td>${b.leverage ?? "—"}</td>
        <td>${b.percent ?? "—"}</td>
        <td>${(b.tp ?? "—")}/${(b.sl ?? "—")}</td>
        <td>${syms}</td>
        <td style="text-align:right;">
          <button class="btn btn--danger" data-stop="${b.bot_id}">Stop</button>
        </td>
      </tr>
    `;
  }).join("");

  // bind stop buttons
  $$("button[data-stop]").forEach(btn=>{
    btn.addEventListener("click", async ()=>{
      const id = btn.dataset.stop;
      btn.disabled = true;
      try{
        await api(`/api/stop-bot/${encodeURIComponent(id)}`, { method:"POST" });
        await refreshAll();
      }catch(e){
        alert("Stop bot lỗi: " + e.message);
      }finally{
        btn.disabled = false;
      }
    });
  });
}

async function fetchState(){
  const data = await api("/api/state");
  setPill(true, "state ok");
  renderState(data);
  return data;
}

/* =========================
   Create / Stop all
========================= */
async function createBot(){
  const body = {
    lev: Number($("#f-lev").value),
    percent: Number($("#f-percent").value),
    tp: Number($("#f-tp").value),
    sl: Number($("#f-sl").value),
    roi_trigger: Number($("#f-roi").value),
  };

  $("#create-msg").textContent = "Đang tạo bot...";
  try{
    const r = await api("/api/add-bot", { method:"POST", body: JSON.stringify(body) });
    $("#create-msg").textContent = r.msg || "OK";
    await refreshAll();
  }catch(e){
    $("#create-msg").textContent = "Lỗi: " + e.message;
  }
}

async function stopAll(){
  if (!confirm("Dừng toàn bộ bot?")) return;
  try{
    await api("/api/stop-all", { method:"POST" });
    await refreshAll();
  }catch(e){
    alert("Stop all lỗi: " + e.message);
  }
}

/* =========================
   Logs
========================= */
let logCache = [];
function renderLogs(events){
  const lines = (events || []).map(ev=>{
    const t = new Date((ev.timestamp || 0) * 1000).toLocaleTimeString();
    const msg = ev.message || JSON.stringify(ev);
    return `[${t}] (${ev.type}) ${msg}`;
  }).join("\n");
  $("#logs").textContent = lines || "—";
}

async function fetchLogs(){
  const limit = Number($("#l-limit").value || 80);
  const events = await api(`/api/events?limit=${limit}`);
  logCache = events;
  renderLogs(events);
  return events;
}

/* =========================
   Chart
========================= */
let chart, candleSeries;

function initChart(){
  const el = $("#chart");
  chart = LightweightCharts.createChart(el, {
    layout: {
      background: { type: 'solid', color: 'transparent' },
      textColor: 'rgba(230,237,247,.9)',
    },
    grid: {
      vertLines: { color: 'rgba(255,255,255,.06)' },
      horzLines: { color: 'rgba(255,255,255,.06)' },
    },
    timeScale: { borderColor: 'rgba(255,255,255,.10)' },
    rightPriceScale: { borderColor: 'rgba(255,255,255,.10)' },
    height: el.clientHeight,
  });

  candleSeries = chart.addCandlestickSeries({
    upColor: 'rgba(34,197,94,.9)',
    downColor: 'rgba(255,77,77,.9)',
    borderUpColor: 'rgba(34,197,94,.9)',
    borderDownColor: 'rgba(255,77,77,.9)',
    wickUpColor: 'rgba(34,197,94,.9)',
    wickDownColor: 'rgba(255,77,77,.9)',
  });

  window.addEventListener("resize", ()=>{
    chart.applyOptions({ width: el.clientWidth, height: el.clientHeight });
  });
}

async function loadChart(){
  const symbol = ($("#c-symbol").value || "BTCUSDT").trim().toUpperCase();
  const interval = $("#c-interval").value || "1m";
  const limit = Number($("#c-limit").value || 60);

  $("#chart-meta").textContent = "Loading…";
  try{
    const r = await api(`/api/chart?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`);
    const candles = r.candles || [];

    const data = candles.map(c => ({
      time: Math.floor(c.t / 1000),
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));

    candleSeries.setData(data);
    $("#chart-meta").textContent = `${symbol} • ${interval} • ${candles.length} candles`;
  }catch(e){
    $("#chart-meta").textContent = "Lỗi: " + e.message;
  }
}

/* =========================
   Refresh orchestration
========================= */
async function refreshAll(){
  try{
    await fetchState();
  }catch(e){
    setPill(false, "state error");
  }

  try{
    await fetchLogs();
  }catch(e){
    // logs error is not critical
  }
}

function initActions(){
  $("#btn-add-bot").addEventListener("click", createBot);
  $("#btn-stop-all").addEventListener("click", stopAll);
  $("#btn-refresh").addEventListener("click", refreshAll);
  $("#btn-load-chart").addEventListener("click", loadChart);
  $("#btn-clear").addEventListener("click", ()=>{
    logCache = [];
    renderLogs([]);
  });
}

/* =========================
   Boot
========================= */
initTabs();
initActions();
initChart();
refreshAll();
loadChart();

// Auto refresh
setInterval(fetchState, 3000);
setInterval(fetchLogs, 2000);
