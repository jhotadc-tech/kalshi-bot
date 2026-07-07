#!/usr/bin/env python3
"""
KALSHI BTC 15-MIN — BOT DE INVESTIGACION DE ABSORCION
Version: 1.0 | Python 3.11+ | Auth: RSA-PSS
Solo Kalshi. Sin Binance. Sin trading. Solo observacion.

OBJETIVO UNICO:
  Cuando un contrato (YES o NO) se transacciona REALMENTE a un precio
  <= UMBRAL_PANICO, con que frecuencia ese lado termina ganando
  (resolviendo en $1.00) al vencer el mercado de 15 minutos?

Este bot NO calcula probabilidad, NO simula compras, NO usa momentum.
Solo: detecta transacciones reales de panico, espera el resultado,
y cuenta -- agrupando siempre por MERCADO UNICO, nunca por fila.

Instrucciones:
  1. Crear .env con KALSHI_API_KEY y KALSHI_PRIVATE_KEY_PATH
  2. pip install aiohttp websockets python-dotenv cryptography
  3. python3 main_investigacion.py
"""

import asyncio
import base64
import csv
import json
import logging
import logging.handlers
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ==============================================================
# CREDENCIALES DESDE .env
# ==============================================================
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)

def _requerir(nombre):
    val = os.getenv(nombre, "").strip()
    if not val:
        print(f"\n[ERROR] Falta '{nombre}' en el archivo .env")
        print(f"   Ruta: {_ENV_PATH}\n")
        sys.exit(1)
    return val

KALSHI_API_KEY          = _requerir("KALSHI_API_KEY")
KALSHI_PRIVATE_KEY_PATH = _requerir("KALSHI_PRIVATE_KEY_PATH")

def _cargar_llave():
    p = Path(KALSHI_PRIVATE_KEY_PATH)
    if not p.exists():
        print(f"\n[ERROR] No se encuentra la llave privada en: {p}")
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

# ==============================================================
# PARAMETROS
# ==============================================================
KALSHI_SERIES        = "KXBTC15M"
UMBRAL_PANICO         = 0.04
INTERVALO_TRADES_SEG  = 30.0
INTERVALO_DESCUBRIR_SEG = 20.0
INTERVALO_VENCIMIENTO_SEG = 45.0

KALSHI_REST = "https://api.elections.kalshi.com/trade-api/v2"
DATA_DIR    = Path("./data")
LOG_DIR     = Path("./logs")
HTTP_TIMEOUT = 8.0

ABSORCION_CSV = DATA_DIR / "absorcion_panico.csv"
_AH = ["ts_deteccion", "market_id", "ticker", "lado", "precio_absorcion",
       "contratos", "ts_vencimiento", "resultado_yes", "gano_ese_lado"]

# ==============================================================
# LOGGING
# ==============================================================
class _MicroFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond:06d}"
    def format(self, record):
        record.asctime = self.formatTime(record)
        return f"{record.asctime} | {record.levelname:<8} | {record.name:<14} | {record.getMessage()}"

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

log_main = _log("main")
log_desc = _log("descubrir")
log_abs  = _log("absorcion")
log_res  = _log("resolver")

# ==============================================================
# ESTADO COMPARTIDO
# ==============================================================
class Estado:
    def __init__(self):
        self.market_id: str = ""
        self.ticker: str = ""
        self.is_open: bool = False
        self.lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        self.ultima_consulta_trades: dict = {}
        self.absorciones_pendientes: dict = defaultdict(list)
        self.mercados_resueltos: set = set()
        self.mercados_vistos: set = set()

    def is_shutdown(self) -> bool:
        return self.shutdown_event.is_set()

    def request_shutdown(self, reason=""):
        log_main.warning(f"SHUTDOWN: {reason}")
        self.shutdown_event.set()

# ==============================================================
# CSV HELPERS
# ==============================================================
def _init_csv(path, headers):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=headers).writeheader()

def _write_csv(path, row):
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=list(row.keys())).writerow(row)

# ==============================================================
# CLIENTE REST KALSHI
# ==============================================================
async def descubrir_mercado_activo(estado: Estado):
    """
    Busca el mercado BTC 15-min actualmente 'active'.
    IMPORTANTE: usa limit=200 explicito -- sin esto, Kalshi devuelve
    solo 100 mercados por defecto y el mercado activo puede quedar
    fuera de esa muestra una vez que la serie acumula historial
    (bug real detectado y confirmado en produccion).
    """
    path = "/trade-api/v2/markets"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as s:
            async with s.get(
                f"{KALSHI_REST}/markets",
                headers=_kalshi_headers("GET", path),
                params={"series_ticker": KALSHI_SERIES, "limit": 200},
            ) as r:
                if r.status != 200:
                    log_desc.error(f"Error HTTP {r.status}: {await r.text()}")
                    return False
                data = await r.json()
    except Exception as e:
        log_desc.error(f"Error consultando mercados: {e}")
        return False

    mercados = [m for m in data.get("markets", []) if m.get("status") == "active"]
    if not mercados:
        return False

    m = sorted(mercados, key=lambda x: x.get("expiration_time", ""))[0]
    ticker = m.get("ticker", "")

    async with estado.lock:
        cambio = (ticker != estado.ticker)
        estado.market_id = m.get("id", ticker)
        estado.ticker = ticker
        estado.is_open = True
        estado.mercados_vistos.add(ticker)

    if cambio:
        log_desc.info(f"Mercado activo: {ticker}")
    return True


async def consultar_trades_panico(ticker: str, desde_ts: float) -> list:
    """
    Consulta GET /markets/trades para trades REALES ejecutados.
    Filtra solo los que representan absorcion a precio de panico
    segun el lado que efectivamente compro (taker_side).
    """
    path = "/trade-api/v2/markets/trades"
    min_ts = int(desde_ts)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as s:
            async with s.get(
                f"{KALSHI_REST}/markets/trades",
                headers=_kalshi_headers("GET", path),
                params={"ticker": ticker, "min_ts": min_ts, "limit": 200},
            ) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                trades = data.get("trades", [])
    except Exception as e:
        log_abs.warning(f"Error consultando trades de {ticker}: {e}")
        return []

    absorciones = []
    for t in trades:
        try:
            yes_p = float(t.get("yes_price_dollars", "1"))
            no_p  = float(t.get("no_price_dollars", "1"))
            taker_side = t.get("taker_side", "")
            contratos  = float(t.get("count_fp", "0"))
        except (ValueError, TypeError):
            continue

        if taker_side == "yes" and yes_p <= UMBRAL_PANICO:
            absorciones.append({"lado": "yes", "precio": yes_p, "contratos": contratos})
        elif taker_side == "no" and no_p <= UMBRAL_PANICO:
            absorciones.append({"lado": "no", "precio": no_p, "contratos": contratos})

    return absorciones


async def resultado_mercado(ticker: str):
    """Devuelve True si YES gano, False si NO gano, None si aun no resuelve."""
    path = f"/trade-api/v2/markets/{ticker}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as s:
            async with s.get(f"{KALSHI_REST}/markets/{ticker}",
                             headers=_kalshi_headers("GET", path)) as r:
                if r.status != 200:
                    return None
                m = (await r.json()).get("market", {})
                if m.get("status") == "finalized":
                    return m.get("result", "") == "yes"
                return None
    except Exception as e:
        log_res.warning(f"Error consultando resultado {ticker}: {e}")
        return None

# ==============================================================
# REGISTRO DE ABSORCIONES
# ==============================================================
async def registrar_absorcion(estado: Estado, market_id, ticker, lado, precio, contratos):
    row = {
        "ts_deteccion": datetime.now(tz=timezone.utc).isoformat(),
        "market_id": market_id, "ticker": ticker, "lado": lado,
        "precio_absorcion": round(precio, 4), "contratos": round(contratos, 2),
        "ts_vencimiento": "", "resultado_yes": None, "gano_ese_lado": None,
    }
    async with estado.lock:
        estado.absorciones_pendientes[market_id].append(row)
    _write_csv(ABSORCION_CSV, row)
    log_abs.info(f"Absorcion | {market_id} | lado={lado} | precio=${precio:.4f} | contratos={contratos:.0f}")


async def resolver_absorciones(estado: Estado, market_id, yes):
    async with estado.lock:
        pendientes = estado.absorciones_pendientes.pop(market_id, None)
        estado.mercados_resueltos.add(market_id)
    if not pendientes:
        return
    ts_venc = datetime.now(tz=timezone.utc).isoformat()
    for row in pendientes:
        gano = yes if row["lado"] == "yes" else (not yes)
        row["ts_vencimiento"] = ts_venc
        row["resultado_yes"] = yes
        row["gano_ese_lado"] = gano
        _write_csv(ABSORCION_CSV, row)
    ganados = sum(1 for r in pendientes if r["gano_ese_lado"])
    log_res.info(f"Mercado resuelto | {market_id} | resultado_yes={yes} | "
                 f"{len(pendientes)} absorciones, {ganados} del lado ganador")

# ==============================================================
# CORUTINAS PRINCIPALES
# ==============================================================
async def loop_descubrir(estado: Estado):
    while not estado.is_shutdown():
        await descubrir_mercado_activo(estado)
        await asyncio.sleep(INTERVALO_DESCUBRIR_SEG)


async def loop_absorcion(estado: Estado):
    while not estado.is_shutdown():
        await asyncio.sleep(INTERVALO_TRADES_SEG)
        async with estado.lock:
            market_id = estado.market_id
            ticker = estado.ticker
            is_open = estado.is_open

        if not is_open or not ticker:
            continue

        ahora = time.time()
        desde = estado.ultima_consulta_trades.get(market_id, ahora - INTERVALO_TRADES_SEG - 5)
        estado.ultima_consulta_trades[market_id] = ahora

        absorciones = await consultar_trades_panico(ticker, desde)
        for a in absorciones:
            await registrar_absorcion(estado, market_id, ticker, a["lado"], a["precio"], a["contratos"])


async def loop_vencimientos(estado: Estado):
    while not estado.is_shutdown():
        await asyncio.sleep(INTERVALO_VENCIMIENTO_SEG)
        async with estado.lock:
            pendientes = dict(estado.absorciones_pendientes)

        for market_id, lista in pendientes.items():
            if not lista:
                continue
            ticker = lista[0]["ticker"]
            yes = await resultado_mercado(ticker)
            if yes is None:
                continue
            await resolver_absorciones(estado, market_id, yes)


async def loop_reporte(estado: Estado):
    while not estado.is_shutdown():
        await asyncio.sleep(600)
        print("\n" + generar_reporte(estado))

# ==============================================================
# REPORTE -- SIEMPRE AGRUPADO POR MERCADO UNICO, NUNCA POR FILA
# ==============================================================
def generar_reporte(estado: Estado = None) -> str:
    if not ABSORCION_CSV.exists():
        return "Sin datos aun."

    with open(ABSORCION_CSV, encoding="utf-8") as f:
        filas = list(csv.DictReader(f))

    resueltas = [f for f in filas if f["resultado_yes"] in ("True", "False")]
    if not resueltas:
        return f"{len(filas)} detecciones registradas, ninguna resuelta todavia."

    mercados = {}
    for f in resueltas:
        mid = f["market_id"]
        if mid not in mercados:
            mercados[mid] = {"yes_gano": None, "no_gano": None}
        gano = f["gano_ese_lado"] == "True"
        if f["lado"] == "yes":
            mercados[mid]["yes_gano"] = gano
        else:
            mercados[mid]["no_gano"] = gano

    yes_total = sum(1 for m in mercados.values() if m["yes_gano"] is not None)
    yes_win   = sum(1 for m in mercados.values() if m["yes_gano"] is True)
    no_total  = sum(1 for m in mercados.values() if m["no_gano"] is not None)
    no_win    = sum(1 for m in mercados.values() if m["no_gano"] is True)

    yes_wr = yes_win/yes_total*100 if yes_total else 0
    no_wr  = no_win/no_total*100 if no_total else 0

    vistos = len(estado.mercados_vistos) if estado else "?"

    s = "-"*52
    return f"""
================================================
   INVESTIGACION: ABSORCION DE PANICO EN KALSHI
================================================
  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
{s}
  Mercados vistos (activos alguna vez): {vistos}
  Mercados con absorcion detectada    : {len(mercados)}
  Detecciones totales (filas)         : {len(filas)}
  Detecciones resueltas               : {len(resueltas)}
{s}
  LADO YES  -- mercados con absorcion: {yes_total:>3}  |  gano: {yes_win:>3}  ({yes_wr:.1f}%)
  LADO NO   -- mercados con absorcion: {no_total:>3}  |  gano: {no_win:>3}  ({no_wr:.1f}%)
{s}
  Umbral de panico: precio <= ${UMBRAL_PANICO}
{s}
"""

# ==============================================================
# MAIN
# ==============================================================
async def main():
    _setup_logging()
    print("""
================================================================
   BOT DE INVESTIGACION -- ABSORCION DE PANICO
   Solo Kalshi. Sin Binance. Sin trading.
   Objetivo: medir si contratos absorbidos a precio
   ridiculo (<=$0.04) terminan ganando, en YES y NO.
   Logs -> ./logs/bot.log | Datos -> ./data/
================================================================""")

    _init_csv(ABSORCION_CSV, _AH)
    estado = Estado()

    loop = asyncio.get_running_loop()
    import signal as signal_module
    def _stop():
        log_main.warning("Senal de apagado recibida.")
        estado.request_shutdown("senal SO")
    for sig in (signal_module.SIGINT, signal_module.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    tareas = [
        asyncio.create_task(loop_descubrir(estado),    name="descubrir"),
        asyncio.create_task(loop_absorcion(estado),    name="absorcion"),
        asyncio.create_task(loop_vencimientos(estado), name="vencimientos"),
        asyncio.create_task(loop_reporte(estado),      name="reporte"),
    ]
    log_main.info(f"{len(tareas)} corutinas activas.")

    try:
        done, _ = await asyncio.wait(tareas, return_when=asyncio.FIRST_EXCEPTION)
        for t in done:
            if t.exception():
                log_main.error(f"Tarea '{t.get_name()}' fallo: {t.exception()}")
                estado.request_shutdown(f"excepcion en {t.get_name()}")
    except asyncio.CancelledError:
        pass
    finally:
        estado.request_shutdown("finally")
        for t in tareas:
            t.cancel()
        await asyncio.wait_for(asyncio.gather(*tareas, return_exceptions=True), timeout=5)
        print("\n" + generar_reporte(estado))
        log_main.info("Bot detenido.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
