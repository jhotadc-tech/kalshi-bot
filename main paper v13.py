#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  KALSHI BTC BOT — PAPER TRADING (ARCHIVO ÚNICO)                 ║
║  Estrategia: Riesgo Asimétrico Positivo — BTC 15-min            ║
║  Versión: 1.3 | Python 3.11+ | Auth: RSA-PSS                   ║
║  + Tracking de señales omitidas (análisis grueso)               ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import base64
import csv
import json
import logging
import logging.handlers
import math
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ══════════════════════════════════════════════════════════════════
# CREDENCIALES DESDE .env
# ══════════════════════════════════════════════════════════════════
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)

def _requerir(nombre):
    val = os.getenv(nombre, "").strip()
    if not val:
        print(f"\n❌ ERROR: Falta '{nombre}' en el archivo .env")
        print(f"   Ruta: {_ENV_PATH}\n")
        sys.exit(1)
    return val

def _opcional(nombre, default=""):
    return os.getenv(nombre, default).strip()

KALSHI_API_KEY          = _requerir("KALSHI_API_KEY")
KALSHI_PRIVATE_KEY_PATH = _requerir("KALSHI_PRIVATE_KEY_PATH")
TELEGRAM_BOT_TOKEN      = _opcional("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID        = _opcional("TELEGRAM_CHAT_ID")

def _cargar_llave():
    p = Path(KALSHI_PRIVATE_KEY_PATH)
    if not p.exists():
        print(f"\n❌ ERROR: No se encuentra la llave privada en: {p}")
        sys.exit(1)
    with open(p, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

_PRIVATE_KEY = _cargar_llave()

def _kalshi_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = f"{ts}{method.upper()}{path}".encode()
    sig = _PRIVATE_KEY.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
    }

# ══════════════════════════════════════════════════════════════════
# PARÁMETROS
# ══════════════════════════════════════════════════════════════════
KALSHI_SERIES       = "KXBTC15M"
BALANCE_INICIAL_USD = 500.0
RIESGO_PCT          = 0.02
MAX_CARTUCHO_USD    = 30.0
KILL_SWITCH_PERDIDA = 80.0
MAX_PRECIO_ENTRADA  = 0.10
MIN_PROBABILIDAD    = 0.30

KALSHI_REST        = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS          = "wss://api.elections.kalshi.com/trade-api/ws/v2"
BINANCE_WS         = "wss://stream.binance.us:9443/stream"
PAPER_DIR          = Path("./paper_data")
LOG_DIR            = Path("./logs")
RECONNECT_BASE     = 1.0
RECONNECT_MAX      = 60.0
HEARTBEAT_INTERVAL = 20.0
HTTP_TIMEOUT       = 5.0

# ══════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════
class _MicroFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"
    def format(self, record):
        record.asctime = self.formatTime(record)
        return (f"{record.asctime} | {record.levelname:<8} | "
                f"{record.name:<18} | {record.getMessage()}")

def _setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = _MicroFormatter()
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "bot.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(ch)
    root.addHandler(fh)
    for lib in ("websockets", "asyncio", "aiohttp"):
        logging.getLogger(lib).setLevel(logging.WARNING)

def _log(name): return logging.getLogger(name)

# ══════════════════════════════════════════════════════════════════
# ESTADO COMPARTIDO
# ══════════════════════════════════════════════════════════════════
@dataclass
class OrderBookLevel:
    price: float
    qty: float

@dataclass
class BinanceState:
    bids: list = field(default_factory=list)
    asks: list = field(default_factory=list)
    last_price: float = 0.0
    prev_price: float = 0.0
    last_trade_ts: float = 0.0
    obi: float = 0.0
    momentum: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

@dataclass
class KalshiMarketState:
    market_id: str = ""
    ticker: str = ""
    yes_ask: float = 1.0
    strike_price: float = 0.0
    expiry_ts: float = 0.0
    is_open: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

@dataclass
class RiskState:
    current_balance: float = 0.0
    daily_pnl: float = 0.0
    daily_reset_date: date = field(default_factory=date.today)
    kill_switch_active: bool = False
    kill_switch_activated_at: Optional[datetime] = None
    active_positions: dict = field(default_factory=dict)
    trades_today: list = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

@dataclass
class SharedState:
    binance: BinanceState = field(default_factory=BinanceState)
    kalshi_market: KalshiMarketState = field(default_factory=KalshiMarketState)
    risk: RiskState = field(default_factory=RiskState)
    shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    def is_shutdown(self): return self.shutdown_event.is_set()
    def request_shutdown(self, reason=""):
        _log("state").warning(f"SHUTDOWN: {reason}")
        self.shutdown_event.set()

# ══════════════════════════════════════════════════════════════════
# MOTOR DE SEÑAL
# ══════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class SignalResult:
    probability: float
    obi_score: float
    momentum_score: float
    distance_score: float
    triggered: bool
    reason: str

_W_OBI=0.45; _W_MOM=0.35; _W_DIST=0.20
_MOM_BAND=0.0005; _MAX_DIST=2000.0; _K=8.0

def _logistic(x, k=_K):
    try:    return 1.0/(1.0+math.exp(-k*x))
    except: return 0.0 if x<0 else 1.0

def _s_obi(obi):   return _logistic(obi, k=_K)
def _s_mom(m):
    if abs(m)<_MOM_BAND: return 0.5
    return _logistic(max(-1.0,min(1.0,m/0.005)),k=_K)
def _s_dist(spot,strike):
    if strike<=0 or spot<=0: return 0.0
    return _logistic(1.0-abs(spot-strike)/_MAX_DIST*2,k=4.0)

def calcular_probabilidad_real(obi,momentum,spot,strike)->SignalResult:
    so=_s_obi(obi); sm=_s_mom(momentum); sd=_s_dist(spot,strike)
    prob=max(0.0,min(1.0,_W_OBI*so+_W_MOM*sm+_W_DIST*sd))
    trig=prob>=MIN_PROBABILIDAD
    reason=(f"SEÑAL prob={prob:.3f} obi={obi:.3f} dist={abs(spot-strike):.0f}USD"
            if trig else f"Sin señal prob={prob:.3f}")
    return SignalResult(prob,so,sm,sd,trig,reason)

def evaluar_entrada(obi,momentum,spot,strike,yes_ask):
    r=calcular_probabilidad_real(obi,momentum,spot,strike)
    return r.triggered and yes_ask<=MAX_PRECIO_ENTRADA, r

# ══════════════════════════════════════════════════════════════════
# GESTIÓN DE RIESGO
# ══════════════════════════════════════════════════════════════════
log_risk=_log("risk")

def calcular_cartucho(bal): return round(min(bal*RIESGO_PCT,MAX_CARTUCHO_USD),2)
def calcular_contratos(monto,ask): return int(monto/ask) if ask>0 else 0

def _reset_diario(risk:RiskState):
    hoy=date.today()
    if hoy!=risk.daily_reset_date:
        log_risk.info(f"Nuevo día {hoy}. Reset P&L (anterior: ${risk.daily_pnl:.2f})")
        risk.daily_pnl=0.0; risk.daily_reset_date=hoy; risk.trades_today=[]

async def puede_operar(shared:SharedState,mid:str)->tuple[bool,str]:
    async with shared.risk.lock:
        _reset_diario(shared.risk)
        if shared.risk.kill_switch_active: return False,"Kill-switch activo"
        if shared.risk.active_positions.get(mid,False): return False,f"Posición activa {mid}"
        if calcular_cartucho(shared.risk.current_balance)<1.0: return False,"Saldo insuficiente"
        return True,"OK"

async def registrar_entrada(shared:SharedState,mid:str,monto:float,precio:float):
    async with shared.risk.lock:
        shared.risk.active_positions[mid]=True
    log_risk.info(f"ENTRADA | {mid} | ${monto:.2f} | precio={precio:.4f}")

async def registrar_cierre(shared:SharedState,mid:str,pnl:float,bal:float):
    async with shared.risk.lock:
        _reset_diario(shared.risk)
        shared.risk.active_positions.pop(mid,None)
        shared.risk.daily_pnl+=pnl
        shared.risk.current_balance=bal
        log_risk.info(f"CIERRE | {mid} | pnl=${pnl:+.2f} | daily=${shared.risk.daily_pnl:.2f}")
        if shared.risk.daily_pnl<=-KILL_SWITCH_PERDIDA and not shared.risk.kill_switch_active:
            shared.risk.kill_switch_active=True
            shared.risk.kill_switch_activated_at=datetime.now(tz=timezone.utc)
            log_risk.critical(f"🚨 KILL-SWITCH | pérdida=${shared.risk.daily_pnl:.2f}")
            await _notif_kill_switch(abs(shared.risk.daily_pnl),bal)

async def verificar_kill_switch_expiry(shared:SharedState):
    while not shared.is_shutdown():
        await asyncio.sleep(60)
        async with shared.risk.lock:
            if shared.risk.kill_switch_active and shared.risk.kill_switch_activated_at:
                secs=(datetime.now(tz=timezone.utc)-shared.risk.kill_switch_activated_at).total_seconds()
                if secs>=86400:
                    shared.risk.kill_switch_active=False
                    shared.risk.kill_switch_activated_at=None
                    shared.risk.daily_pnl=0.0
                    log_risk.info("Kill-switch expirado. Reactivado.")

# ══════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════
log_tg=_log("telegram")

async def _tg(texto:str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":texto,
                      "parse_mode":"HTML","disable_web_page_preview":True},
                timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status!=200: log_tg.warning(f"Telegram {r.status}")
    except Exception as e: log_tg.warning(f"Telegram falló: {e}")

def _ts(): return datetime.now(tz=timezone.utc).strftime("%H:%M:%S UTC")
async def _notif_inicio(bal):
    await _tg(f"🤖 <b>BOT PAPER INICIADO</b> — {_ts()}\n🏦 Balance virtual: <b>${bal:.2f}</b>\n📋 SIN DINERO REAL")
async def _notif_entrada(mid,monto,n,precio,prob):
    await _tg(f"🟢 <b>[PAPER] ENTRADA</b> — {_ts()}\n📌 <code>{mid}</code>\n💵 ${monto:.2f} ({n} contratos × {precio:.4f})\n🧠 Prob: <b>{prob:.1%}</b>")
async def _notif_vencimiento(mid,pnl,bal):
    e="💰" if pnl>0 else "🔴"
    await _tg(f"{e} <b>[PAPER] VENCIMIENTO</b> — {_ts()}\n📌 <code>{mid}</code>\n📊 P&L: <b>{'+' if pnl>=0 else ''}${pnl:.2f}</b>\n🏦 Balance: <b>${bal:.2f}</b>")
async def _notif_kill_switch(perdida,bal):
    await _tg(f"🚨 <b>[PAPER] KILL-SWITCH</b> — {_ts()}\n⛔ Habría suspendido 24h\n📉 Pérdida: <b>-${perdida:.2f}</b> | Bal: <b>${bal:.2f}</b>")

# ══════════════════════════════════════════════════════════════════
# PAPER ENGINE
# ══════════════════════════════════════════════════════════════════
SIGNALS_CSV=PAPER_DIR/"signals.csv"
TRADES_CSV=PAPER_DIR/"trades.csv"
DAILY_CSV=PAPER_DIR/"daily_pnl.csv"
OMITIDAS_CSV=PAPER_DIR/"omitidas.csv"

_SH=["ts_utc","market_id","ticker","spot","strike","yes_ask","obi","momentum",
     "probability","signal_triggered","price_ok","buy_signal","would_fill","motivo"]
_TH=["ts_entrada","market_id","ticker","spot_entrada","strike","yes_ask","contratos",
     "monto_usd","probability","ts_vencimiento","resultado_yes","pnl_usd","balance_virtual"]
_OH=["ts_deteccion","market_id","ticker","spot","strike","yes_ask","probability",
     "motivo_omision","ts_vencimiento","resultado_yes","hubiera_ganado_usd"]

def _init_csv(p,h):
    if not p.exists():
        with open(p,"w",newline="",encoding="utf-8") as f:
            csv.DictWriter(f,fieldnames=h).writeheader()

def _write_csv(p,row):
    with open(p,"a",newline="",encoding="utf-8") as f:
        csv.DictWriter(f,fieldnames=list(row.keys())).writerow(row)

log_p=_log("paper")

class PaperEngine:
    def __init__(self,saldo=BALANCE_INICIAL_USD):
        PAPER_DIR.mkdir(parents=True,exist_ok=True)
        _init_csv(SIGNALS_CSV,_SH)
        _init_csv(TRADES_CSV,_TH)
        _init_csv(DAILY_CSV,["fecha","trades","ganados","perdidos","pendientes","pnl_usd","balance_cierre"])
        _init_csv(OMITIDAS_CSV,_OH)
        self.balance_virtual=saldo
        self.daily_pnl=0.0
        self.trades_abiertos={}
        self.trades_cerrados=[]
        self.omitidas_abiertas={}   # market_id -> dict (señales con prob>=30% que NO se compraron)
        self._lock=asyncio.Lock()
        log_p.info(f"Paper Engine | balance=${saldo:.2f} | {PAPER_DIR.resolve()}")

    async def reg_señal(self,mid,ticker,spot,strike,yes_ask,r:SignalResult,buy:bool,motivo=""):
        row={"ts_utc":datetime.now(tz=timezone.utc).isoformat(),
             "market_id":mid,"ticker":ticker,"spot":round(spot,2),"strike":round(strike,2),
             "yes_ask":round(yes_ask,4),"obi":round(r.obi_score,4),"momentum":round(r.momentum_score,4),
             "probability":round(r.probability,4),"signal_triggered":r.triggered,
             "price_ok":yes_ask<=MAX_PRECIO_ENTRADA,"buy_signal":buy,
             "would_fill":buy and not motivo,"motivo":motivo}
        async with self._lock: _write_csv(SIGNALS_CSV,row)

    async def reg_omitida(self,mid,ticker,spot,strike,yes_ask,prob,motivo_omision):
        """
        Registra una señal con probabilidad >=30% que NO se compró
        (por precio caro, mutex de posición, kill-switch, etc.)
        para poder evaluar después si hubiera ganado o perdido.
        Solo se registra una vez por mercado (evita duplicados del hot path).
        """
        async with self._lock:
            if mid in self.omitidas_abiertas:
                return
            row = {
                "ts_deteccion": datetime.now(tz=timezone.utc).isoformat(),
                "market_id": mid, "ticker": ticker,
                "spot": round(spot,2), "strike": round(strike,2),
                "yes_ask": round(yes_ask,4), "probability": round(prob,4),
                "motivo_omision": motivo_omision,
                "ts_vencimiento": "", "resultado_yes": None, "hubiera_ganado_usd": None,
            }
            self.omitidas_abiertas[mid] = row
            _write_csv(OMITIDAS_CSV, row)

    async def resolver_omitida(self, mid:str, yes:bool):
        """Al vencer el mercado, calcula si la señal omitida hubiera ganado."""
        async with self._lock:
            row = self.omitidas_abiertas.pop(mid, None)
        if row is None:
            return None
        row["ts_vencimiento"] = datetime.now(tz=timezone.utc).isoformat()
        row["resultado_yes"] = yes
        # Simula: si hubiéramos comprado 1 contrato a ese yes_ask
        yes_ask = row["yes_ask"] if row["yes_ask"] > 0 else 1.0
        if yes:
            row["hubiera_ganado_usd"] = round(1.0 - yes_ask, 4)   # ganancia por contrato
        else:
            row["hubiera_ganado_usd"] = round(-yes_ask, 4)        # pérdida por contrato
        async with self._lock:
            _write_csv(OMITIDAS_CSV, row)
        e = "✅" if yes else "❌"
        log_p.info(f"📝 OMITIDA {e} | {mid} | motivo={row['motivo_omision']} | "
                   f"hubiera_ganado=${row['hubiera_ganado_usd']:+.4f}/contrato")
        return row

    async def simular_entrada(self,mid,ticker,spot,strike,yes_ask,n,monto,prob)->dict:
        now=datetime.now(tz=timezone.utc).isoformat()
        t={"ts_entrada":now,"market_id":mid,"ticker":ticker,"spot_entrada":round(spot,2),
           "strike":round(strike,2),"yes_ask":round(yes_ask,4),"contratos":n,
           "monto_usd":round(monto,2),"probability":round(prob,4),
           "ts_vencimiento":"","resultado_yes":None,"pnl_usd":0.0,"balance_virtual":0.0}
        async with self._lock:
            self.balance_virtual-=monto
            self.trades_abiertos[mid]=t
            _write_csv(TRADES_CSV,t)
        log_p.info(f"📋 ENTRADA | {mid} | {n} contratos | ${monto:.2f} | prob={prob:.1%} | bal=${self.balance_virtual:.2f}")
        await _notif_entrada(mid,monto,n,yes_ask,prob)
        return t

    async def resolver_vencimiento(self,mid:str,yes:bool)->Optional[dict]:
        async with self._lock: t=self.trades_abiertos.pop(mid,None)
        if t is None: return None
        t["ts_vencimiento"]=datetime.now(tz=timezone.utc).isoformat()
        t["resultado_yes"]=yes
        t["pnl_usd"]=round(float(t["contratos"])-t["monto_usd"],2) if yes else round(-t["monto_usd"],2)
        async with self._lock:
            self.balance_virtual+=t["monto_usd"]+t["pnl_usd"]
            self.daily_pnl+=t["pnl_usd"]
            t["balance_virtual"]=round(self.balance_virtual,2)
            self.trades_cerrados.append(t)
            _write_csv(TRADES_CSV,t)
        e="✅" if yes else "❌"
        log_p.info(f"📋 {e} VENCIMIENTO | {mid} | pnl=${t['pnl_usd']:+.2f} | bal=${self.balance_virtual:.2f}")
        await _notif_vencimiento(mid,t["pnl_usd"],self.balance_virtual)
        return t

    async def cerrar_dia(self,fecha:str):
        async with self._lock:
            hoy=[t for t in self.trades_cerrados if fecha in (t.get("ts_vencimiento") or "")]
            g=sum(1 for t in hoy if t.get("resultado_yes") is True)
            p=sum(1 for t in hoy if t.get("resultado_yes") is False)
            pnl=sum(t.get("pnl_usd",0) for t in hoy)
            row={"fecha":fecha,"trades":len(hoy),"ganados":g,"perdidos":p,
                 "pendientes":len(self.trades_abiertos),"pnl_usd":round(pnl,2),
                 "balance_cierre":round(self.balance_virtual,2)}
            _write_csv(DAILY_CSV,row)
            log_p.info(f"📊 DÍA {fecha} | ✅{g} ❌{p} | P&L=${pnl:+.2f} | bal=${self.balance_virtual:.2f}")

# ══════════════════════════════════════════════════════════════════
# ORÁCULO BINANCE
# ══════════════════════════════════════════════════════════════════
log_ora=_log("oracle")
_BSTREAM=f"{BINANCE_WS}?streams=btcusdt@depth10@100ms/btcusdt@aggTrade"

def _obi(bids,asks):
    bv=sum(l.qty for l in bids); av=sum(l.qty for l in asks)
    return 0.0 if bv+av==0 else (bv-av)/(bv+av)
def _mom(last,prev): return 0.0 if prev==0 else (last-prev)/prev
def _parse_depth(d,st:BinanceState):
    st.bids=[OrderBookLevel(float(p),float(q)) for p,q in d.get("bids",[])]
    st.asks=[OrderBookLevel(float(p),float(q)) for p,q in d.get("asks",[])]
    st.obi=_obi(st.bids,st.asks)
def _parse_trade(d,st:BinanceState):
    np=float(d["p"])
    st.prev_price=st.last_price if st.last_price else np
    st.last_price=np; st.last_trade_ts=float(d["T"]); st.momentum=_mom(st.last_price,st.prev_price)

async def run_oracle(shared:SharedState):
    delay=RECONNECT_BASE
    while not shared.is_shutdown():
        try:
            log_ora.info("Conectando a Binance WS...")
            async with websockets.connect(_BSTREAM,ping_interval=HEARTBEAT_INTERVAL,
                                          ping_timeout=10,close_timeout=5,max_size=2**20) as ws:
                log_ora.info("Binance WS ✓")
                delay=RECONNECT_BASE
                async for raw in ws:
                    if shared.is_shutdown(): break
                    try:
                        env=json.loads(raw)
                        async with shared.binance.lock:
                            if "depth" in env.get("stream",""): _parse_depth(env.get("data",{}),shared.binance)
                            elif "aggTrade" in env.get("stream",""): _parse_trade(env.get("data",{}),shared.binance)
                    except Exception as e: log_ora.warning(f"Msg ignorado: {e}")
        except (ConnectionClosedError,ConnectionClosedOK) as e:
            log_ora.warning(f"Binance cerrado ({e}). Retry {delay:.0f}s...")
        except Exception as e:
            log_ora.exception(f"Oracle error: {e}. Retry {delay:.0f}s...")
        if not shared.is_shutdown():
            await asyncio.sleep(delay)
            delay=min(delay*2,RECONNECT_MAX)
    log_ora.info("Oracle detenido.")

# ══════════════════════════════════════════════════════════════════
# EJECUCIÓN KALSHI — PAPER
# ══════════════════════════════════════════════════════════════════
log_ex=_log("paper_exec")
_ultimo_msg_kalshi = 0.0

async def _resultado_mercado(ticker:str):
    path=f"/trade-api/v2/markets/{ticker}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as s:
            async with s.get(f"{KALSHI_REST}/markets/{ticker}",
                             headers=_kalshi_headers("GET",path)) as r:
                if r.status!=200: return None,f"HTTP {r.status}"
                m=(await r.json()).get("market",{})
                if m.get("status")=="finalized":
                    return m.get("result","")=="yes","finalized"
                return None,m.get("status","")
    except Exception as e:
        log_ex.warning(f"Error resultado {ticker}: {e}"); return None,"error"

async def _descubrir_mercado(shared:SharedState)->bool:
    path="/trade-api/v2/markets"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{KALSHI_REST}/markets",
                headers=_kalshi_headers("GET",path),
                params={"series_ticker":KALSHI_SERIES},
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as r:
                if r.status!=200:
                    log_ex.error(f"Error mercados HTTP {r.status}: {await r.text()}")
                    return False
                mercados=(await r.json()).get("markets",[])
    except Exception as e:
        log_ex.error(f"Error mercados: {e}"); return False

    mercados = [m for m in mercados if m.get("status") == "active"]
    if not mercados: return False
    m=sorted(mercados,key=lambda x:x.get("expiration_time",""))[0]
    ticker=m.get("ticker","")
    try:    strike=float(m.get("floor_strike") or 0.0)
    except: strike=0.0

    async with shared.kalshi_market.lock:
        shared.kalshi_market.strike_price=strike
        shared.kalshi_market.market_id=m.get("id",ticker)
        shared.kalshi_market.ticker=ticker
        shared.kalshi_market.strike_price=strike
        shared.kalshi_market.is_open=True
        try:
            dt=datetime.fromisoformat(m.get("expiration_time","").replace("Z","+00:00"))
            shared.kalshi_market.expiry_ts=dt.timestamp()
        except: pass

    try:
        yes_ask_str = m.get("yes_ask_dollars") or m.get("last_price_dollars") or "1.0"
        shared.kalshi_market.yes_ask = float(yes_ask_str)
    except:
        shared.kalshi_market.yes_ask = 1.0
    log_ex.info(f"Mercado: {ticker} | strike=${strike:,.2f} | yes_ask=${shared.kalshi_market.yes_ask:.4f}")
    return True

def _parse_ob(msg:dict,st:KalshiMarketState):
    if msg.get("type") not in ("orderbook_snapshot","orderbook_delta"): return
    asks=msg.get("msg",{}).get("yes",{}).get("asks",[])
    if asks and asks[0][0] > 0: st.yes_ask=asks[0][0]/100.0
    elif asks and asks[0][0] == 0 and len(asks) > 1 and asks[1][0] > 0: st.yes_ask=asks[1][0]/100.0
    else: st.yes_ask=0.0

async def _actualizar_yes_ask(shared:SharedState):
    """Consulta el precio real del mercado via REST si el WS no lo envía."""
    async with shared.kalshi_market.lock:
        ticker = shared.kalshi_market.ticker
        yes_ask = shared.kalshi_market.yes_ask
    if yes_ask > 0 or not ticker:
        return
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as s:
            async with s.get(f"{KALSHI_REST}/markets/{ticker}",
                            headers=_kalshi_headers("GET",path)) as r:
                if r.status == 200:
                    m = (await r.json()).get("market",{})
                    yes_str = m.get("yes_ask_dollars") or m.get("last_price_dollars") or "0"
                    yes_val = float(yes_str)
                    if yes_val > 0:
                        async with shared.kalshi_market.lock:
                            shared.kalshi_market.yes_ask = yes_val
                        log_ex.debug(f"yes_ask actualizado via REST: ${yes_val:.4f}")
    except Exception as e:
        log_ex.warning(f"Error actualizando yes_ask: {e}")

async def _evaluar(shared:SharedState,paper:PaperEngine):
    async with shared.binance.lock:
        obi=shared.binance.obi; mom=shared.binance.momentum; spot=shared.binance.last_price
    async with shared.kalshi_market.lock:
        yes_ask=shared.kalshi_market.yes_ask; strike=shared.kalshi_market.strike_price
        mid=shared.kalshi_market.market_id; ticker=shared.kalshi_market.ticker
        is_open=shared.kalshi_market.is_open

    if spot==0 or not is_open or not ticker: return

    await _actualizar_yes_ask(shared)
    async with shared.kalshi_market.lock:
        yes_ask=shared.kalshi_market.yes_ask
    buy,r=evaluar_entrada(obi,mom,spot,strike,yes_ask)
    motivo=""
    if buy:
        ok,motivo=await puede_operar(shared,mid)
        if not ok: buy=False

    await paper.reg_señal(mid,ticker,spot,strike,yes_ask,r,buy,motivo)

    # ── Tracking de señales omitidas (prob>=30% pero no compramos) ────────
    # Caso A: probabilidad alta pero precio > $0.10 (la señal en sí no dispara "buy")
    # Caso B: sí disparó pero el risk manager bloqueó (motivo no vacío)
    if r.triggered and not (buy and not motivo):
        motivo_omision = motivo if motivo else "precio_alto"
        await paper.reg_omitida(mid,ticker,spot,strike,yes_ask,r.probability,motivo_omision)

    if not buy: return

    monto=calcular_cartucho(paper.balance_virtual)
    n=calcular_contratos(monto,yes_ask)
    if n<1: return

    t=await paper.simular_entrada(mid,ticker,spot,strike,yes_ask,n,round(n*yes_ask,2),r.probability)
    await registrar_entrada(shared,mid,t["monto_usd"],yes_ask)

async def run_paper_expiry_poller(shared:SharedState,paper:PaperEngine):
    while not shared.is_shutdown():
        await asyncio.sleep(60)
        async with paper._lock:
            abiertos=dict(paper.trades_abiertos)
            omitidas=dict(paper.omitidas_abiertas)

        for mid,trade in abiertos.items():
            yes,_=await _resultado_mercado(trade["ticker"])
            if yes is None: continue
            tc=await paper.resolver_vencimiento(mid,yes)
            if tc: await registrar_cierre(shared,mid,tc["pnl_usd"],paper.balance_virtual)

        for mid,om in omitidas.items():
            yes,_=await _resultado_mercado(om["ticker"])
            if yes is None: continue
            await paper.resolver_omitida(mid,yes)

async def run_paper_execution(shared:SharedState,paper:PaperEngine):
    delay=RECONNECT_BASE
    while not shared.is_shutdown():
        if not await _descubrir_mercado(shared):
            log_ex.info("Sin mercado activo. Reintentando en 30s...")
            await asyncio.sleep(30); continue

        ticker=shared.kalshi_market.ticker
        try:
            log_ex.info(f"Conectando a Kalshi WS — {ticker}")
            ts=str(int(time.time()*1000))
            ws_path="/trade-api/ws/v2"
            ws_msg=f"{ts}GET{ws_path}".encode()
            ws_sig=_PRIVATE_KEY.sign(
                ws_msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256())
            ws_headers={
                "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(ws_sig).decode(),
            }
            async with websockets.connect(KALSHI_WS,additional_headers=ws_headers,
                                          ping_interval=HEARTBEAT_INTERVAL,
                                          ping_timeout=10,close_timeout=5) as ws:
                await ws.send(json.dumps({"id":1,"cmd":"subscribe",
                    "params":{"channels":["orderbook_delta"],"market_tickers":[ticker]}}))
                log_ex.info(f"Kalshi WS ✓ | {ticker}")
                delay=RECONNECT_BASE
                async for raw in ws:
                    if shared.is_shutdown(): break
                    try:
                        global _ultimo_msg_kalshi
                        _ultimo_msg_kalshi = time.time()
                        msg_data=json.loads(raw)
                        async with shared.kalshi_market.lock:
                            _parse_ob(msg_data,shared.kalshi_market)
                        await _evaluar(shared,paper)
                    except Exception as e: log_ex.warning(f"Msg error: {e}")
        except (ConnectionClosedError,ConnectionClosedOK) as e:
            log_ex.warning(f"Kalshi cerrado ({e}). Retry {delay:.0f}s...")
        except Exception as e:
            log_ex.exception(f"Kalshi error: {e}. Retry {delay:.0f}s...")
        if not shared.is_shutdown():
            await asyncio.sleep(delay)
            delay=min(delay*2,RECONNECT_MAX)
    log_ex.info("Ejecución paper detenida.")

# ══════════════════════════════════════════════════════════════════
# REPORTE
# ══════════════════════════════════════════════════════════════════
def _leer(p):
    if not p.exists(): return []
    with open(p,encoding="utf-8") as f: return list(csv.DictReader(f))
def _f(v):
    try: return float(v)
    except: return 0.0
def _b(v): return str(v).strip().lower() in ("true","1","yes")

def generar_reporte()->str:
    signals=_leer(SIGNALS_CSV); trades=_leer(TRADES_CSV); daily=_leer(DAILY_CSV)
    omitidas=_leer(OMITIDAS_CSV)
    if not signals: return "⚠️  Sin datos aún."
    dias=max(len(daily),1); ts=len(signals)
    disp=sum(1 for s in signals if _b(s.get("signal_triggered","False")))
    fills=sum(1 for s in signals if _b(s.get("would_fill","False")))
    cerr=[t for t in trades if str(t.get("resultado_yes","")) in ("True","False")]
    gan=[t for t in cerr if _b(t.get("resultado_yes","False"))]
    per=[t for t in cerr if not _b(t.get("resultado_yes","False"))]
    wr=len(gan)/len(cerr)*100 if cerr else 0
    pnl_t=sum(_f(t.get("pnl_usd")) for t in cerr)
    gp=sum(_f(t.get("pnl_usd")) for t in gan)/len(gan) if gan else 0
    pp=sum(_f(t.get("pnl_usd")) for t in per)/len(per) if per else 0
    ev=(wr/100)*gp+(1-wr/100)*pp
    pnl_d=pnl_t/dias; pnl_m=pnl_d*30; roi=pnl_m/BALANCE_INICIAL_USD*100
    montos=[_f(t.get("monto_usd")) for t in cerr]
    mp=sum(montos)/len(montos) if montos else 0
    bf=_f(cerr[-1].get("balance_virtual","500")) if cerr else BALANCE_INICIAL_USD
    racha_max=racha=0
    for t in cerr:
        if not _b(t.get("resultado_yes","False")): racha+=1; racha_max=max(racha_max,racha)
        else: racha=0

    # ── Análisis de señales omitidas (examen grueso) ──────────────────────
    om_cerr = [o for o in omitidas if str(o.get("resultado_yes","")) in ("True","False")]
    om_gan  = [o for o in om_cerr if _b(o.get("resultado_yes","False"))]
    om_wr   = len(om_gan)/len(om_cerr)*100 if om_cerr else 0
    om_ev_por_contrato = sum(_f(o.get("hubiera_ganado_usd")) for o in om_cerr)/len(om_cerr) if om_cerr else 0

    if not cerr:          v="⏳ SIN DATOS — Espera ≥10 trades cerrados."
    elif ev>0 and wr>=10: v="✅ VIABLE — Sigue ≥30 días antes de decidir."
    elif ev>-0.05:        v="⚠️  MARGINAL — Necesitas más datos."
    else:                 v="❌ NO VIABLE — EV negativo. No usar dinero real."
    s="─"*52
    return f"""
╔════════════════════════════════════════════════════╗
║   REPORTE PAPER TRADING — Kalshi BTC 15-min        ║
╚════════════════════════════════════════════════════╝
  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} | {dias} día(s)
{s}
  Señales evaluadas  : {ts:>8,}  |  Fills simulados: {fills:>6,}
  Win Rate           : {wr:>7.1f}%  |  EV por trade  : ${ev:>+7.3f}
  P&L total          : ${pnl_t:>+7.2f}  |  Balance virtual: ${bf:>7.2f}
  P&L mes proyectado : ${pnl_m:>+7.2f}  |  ROI mensual   : {roi:>+7.1f}%
{s}
 SEÑALES OMITIDAS (prob≥30% pero no compradas)
{s}
  Detectadas         : {len(omitidas):>8,}
  Resueltas          : {len(om_cerr):>8,}
  Hubieran ganado    : {len(om_gan):>8,}
  Win Rate (omitidas): {om_wr:>7.1f}%
  EV/contrato omitido: ${om_ev_por_contrato:>+7.4f}
{s}
  VEREDICTO: {v}
{s}
"""

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
async def _watchdog_kalshi(shared:SharedState,paper:PaperEngine):
    global _ultimo_msg_kalshi
    log = _log("watchdog")
    await asyncio.sleep(120)
    while not shared.is_shutdown():
        await asyncio.sleep(60)
        if _ultimo_msg_kalshi == 0:
            continue
        silencio = time.time() - _ultimo_msg_kalshi
        if silencio > 1200:
            log.warning(f"Kalshi WS silencioso {silencio/60:.1f} min. Reconectando...")
            async with shared.kalshi_market.lock:
                shared.kalshi_market.is_open = False
                shared.kalshi_market.ticker = ""
                shared.kalshi_market.yes_ask = 1.0
            _ultimo_msg_kalshi = time.time()

async def _status_loop(shared:SharedState,paper:PaperEngine):
    log=_log("status")
    while not shared.is_shutdown():
        await asyncio.sleep(900)
        async with paper._lock:
            log.info(f"[STATUS] bal=${paper.balance_virtual:.2f} | P&L={paper.daily_pnl:+.2f} | "
                     f"abiertos={len(paper.trades_abiertos)} | cerrados={len(paper.trades_cerrados)} | "
                     f"omitidas_abiertas={len(paper.omitidas_abiertas)}")

async def main():
    _setup_logging()
    log=_log("main")
    print("""
╔══════════════════════════════════════════════════════════╗
║   📋  KALSHI BTC BOT — PAPER TRADING  v1.3              ║
║       SIN DINERO REAL | Auth: RSA-PSS                   ║
║   ✅ Precios Kalshi : REALES    🚫 Órdenes: NINGUNA      ║
║   ✅ Binance US     : REALES    🚫 Dinero : $0.00        ║
║   ✅ Tracking de señales omitidas activo                 ║
║   Logs → ./logs/bot.log | Datos → ./paper_data/          ║
╚══════════════════════════════════════════════════════════╝""")

    log.info(f"API Key: {KALSHI_API_KEY[:8]}...")
    log.info(f"Llave privada: {KALSHI_PRIVATE_KEY_PATH}")

    shared=SharedState()
    paper=PaperEngine(BALANCE_INICIAL_USD)
    shared.risk.current_balance=BALANCE_INICIAL_USD

    loop=asyncio.get_running_loop()
    def _stop(): log.warning("Apagando..."); shared.request_shutdown("señal SO")
    for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,_stop)

    await _notif_inicio(BALANCE_INICIAL_USD)

    tareas=[
        asyncio.create_task(run_oracle(shared),                     name="oracle"),
        asyncio.create_task(run_paper_execution(shared,paper),      name="exec"),
        asyncio.create_task(run_paper_expiry_poller(shared,paper),  name="expiry"),
        asyncio.create_task(verificar_kill_switch_expiry(shared),   name="killswitch"),
        asyncio.create_task(_status_loop(shared,paper),             name="status"),
        asyncio.create_task(_watchdog_kalshi(shared,paper),         name="watchdog"),
    ]
    log.info(f"{len(tareas)} corutinas activas.")

    try:
        done,_=await asyncio.wait(tareas,return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            if t.exception():
                log.error(f"Tarea '{t.get_name()}' falló: {t.exception()}")
                shared.request_shutdown(f"excepción en {t.get_name()}")
    except asyncio.CancelledError: pass
    finally:
        shared.request_shutdown("finally")
        for t in tareas: t.cancel()
        await asyncio.wait_for(asyncio.gather(*tareas,return_exceptions=True),timeout=5)
        await paper.cerrar_dia(datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"))
        print("\n"+"═"*54)
        print(generar_reporte())
        log.info("Bot detenido.")

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
