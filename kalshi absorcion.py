#!/usr/bin/env python3
"""
Bot de INVESTIGACIÓN PASIVA — Absorción de pánico en Kalshi BTC 15-min (KXBTC15M)

Pregunta única que responde:
    Cuando un contrato (YES o NO) se transacciona REALMENTE a <= $0.04,
    ¿con qué frecuencia ese mismo lado termina resolviendo en $1.00?

Este bot NO predice, NO simula compras, NO usa momentum ni indicadores.
Solo: DETECTA trades reales baratos -> ESPERA la resolución -> CUENTA.

Fuente de verdad: el endpoint de TRADES EJECUTADOS (/markets/trades),
NO el libro de órdenes. Una orden gigante en el libro a $0.001 puede no
ejecutarse nunca; solo un trade real confirma una absorción.

Nota de diseño: para esta pregunta concreta NO hace falta el WebSocket de
orderbook (bids/asks), ni la fórmula yes_ask = 1 - best_no_bid, ni el índice
CF Benchmarks/BRTI. Todo eso mide OFERTAS o precio de settlement predictivo;
aquí solo miramos EJECUCIONES reales + resultado final. Arquitectura mínima = REST.
"""

import os
import csv
import time
import base64
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ----------------------------------------------------------------------------
# CONFIGURACIÓN
# ----------------------------------------------------------------------------
BASE_URL              = "https://api.elections.kalshi.com/trade-api/v2"
SIGN_PREFIX           = "/trade-api/v2"      # prefijo que va DENTRO de la firma
SERIES_TICKER         = "KXBTC15M"
PANIC_PRICE           = 0.04                 # umbral de "precio ridículo"

DISCOVERY_INTERVAL    = 30    # s — refrescar la lista de mercados
POLL_INTERVAL_NORMAL  = 30    # s — sondeo de trades en condiciones normales
POLL_INTERVAL_HOT     = 4     # s — sondeo denso cerca del vencimiento
HOT_WINDOW            = 90    # s antes del vencimiento => modo denso
RESOLVE_CHECK_INTERVAL= 15    # s — cada cuánto revisar si ya finalizó
TRACK_NEAREST         = 2     # cuántos mercados próximos a vencer seguir a la vez
SAVE_INTERVAL         = 60    # s — volcado periódico del CSV (no perder datos)
REQUEST_TIMEOUT       = 15    # s

CSV_PATH              = "absorcion_panico.csv"

# Buckets de tamaño para el análisis final (contratos)
SIZE_SMALL_MAX        = 100    # "pocos"   : < 100
SIZE_MASSIVE_MIN      = 1000   # "masivo"  : >= 1000  (medio = intermedio)


# ----------------------------------------------------------------------------
# UTILIDADES
# ----------------------------------------------------------------------------
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def to_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def parse_ts(s) -> float:
    """ISO 8601 (con Z) -> epoch segundos. 0 si no parseable."""
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def iso(ts: float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def pct(a: int, b: int) -> str:
    return f"{100 * a / b:.1f}%" if b else "n/a"


# ----------------------------------------------------------------------------
# AUTENTICACIÓN — RSA-PSS (no HMAC)
# ----------------------------------------------------------------------------
class KalshiAuth:
    def __init__(self, api_key_id: str, private_key_path: str):
        self.api_key_id = api_key_id
        with open(private_key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, message: str) -> str:
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,  # si sale 401, probar DIGEST_LENGTH
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def headers(self, method: str, sign_path: str) -> dict:
        # La firma usa el PATH sin query string, con el prefijo /trade-api/v2
        ts = str(int(time.time() * 1000))
        signature = self._sign(f"{ts}{method}{sign_path}")
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }


# ----------------------------------------------------------------------------
# CLIENTE REST
# ----------------------------------------------------------------------------
class KalshiClient:
    def __init__(self, auth: KalshiAuth):
        self.auth = auth
        self.session = requests.Session()

    def _get(self, path: str, params: dict | None = None) -> dict:
        headers = self.auth.headers("GET", SIGN_PREFIX + path)
        r = self.session.get(BASE_URL + path, headers=headers,
                             params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def get_markets(self) -> list:
        # limit=200 EXPLÍCITO: sin él Kalshi devuelve 100 y el activo puede quedar fuera.
        # status=active como query da 400 -> se filtra en Python.
        data = self._get("/markets", {"series_ticker": SERIES_TICKER, "limit": 200})
        return data.get("markets", [])

    def get_trades(self, ticker: str, min_ts: int) -> list:
        data = self._get("/markets/trades",
                        {"ticker": ticker, "min_ts": min_ts, "limit": 200})
        return data.get("trades", [])

    def get_market(self, ticker: str) -> dict:
        return self._get(f"/markets/{ticker}").get("market", {})


# ----------------------------------------------------------------------------
# ESTADO DE UN MERCADO
# ----------------------------------------------------------------------------
class Market:
    def __init__(self, ticker: str, expiration_ts: float, open_ts: float):
        self.ticker = ticker
        self.expiration_ts = expiration_ts
        # Empezar a mirar trades desde la apertura (captura todo el ciclo de 15 min)
        self.min_ts = int(open_ts) if open_ts else int(time.time())
        self.last_trade_poll = 0.0
        self.last_resolve_check = 0.0
        self.result = None          # "yes" / "no" al resolver
        self.resolved = False


# ----------------------------------------------------------------------------
# CLASIFICADOR DE ABSORCIÓN
# ----------------------------------------------------------------------------
def classify_trade(tr: dict):
    """
    Devuelve (es_absorcion, lado, precio).
    El precio de pánico es el del lado que compró el TAKER.
      taker_side=='yes' y yes_price<=0.04  -> absorción real de YES
      taker_side=='no'  y no_price <=0.04  -> absorción real de NO
    Soporta campos *_dollars (nuevos) y *_price en centavos (legacy).
    """
    side = tr.get("taker_side")
    if side == "yes":
        p = to_float(tr.get("yes_price_dollars"))
        if p is None:
            c = to_float(tr.get("yes_price"))
            p = c / 100 if c is not None else None
        if p is not None and p <= PANIC_PRICE:
            return True, "yes", p
    elif side == "no":
        p = to_float(tr.get("no_price_dollars"))
        if p is None:
            c = to_float(tr.get("no_price"))
            p = c / 100 if c is not None else None
        if p is not None and p <= PANIC_PRICE:
            return True, "no", p
    return False, None, None


def trade_count(tr: dict) -> int:
    c = to_float(tr.get("count_fp"))
    if c is None:
        c = to_float(tr.get("count"))
    return int(c) if c is not None else 0


# ----------------------------------------------------------------------------
# BOT
# ----------------------------------------------------------------------------
class AbsorcionBot:
    CSV_FIELDS = ["ts_deteccion", "market_id", "ticker", "lado", "precio_absorcion",
                  "contratos", "ts_vencimiento", "resultado_yes", "gano_ese_lado"]

    def __init__(self, client: KalshiClient):
        self.client = client
        self.markets: dict[str, Market] = {}   # ticker -> Market (activos / sin resolver)
        self.absorptions: list[dict] = []       # cada trade de absorción detectado
        self.seen_trades: set[str] = set()       # dedupe por trade_id
        self.last_discovery = 0.0
        self.last_save = 0.0

    # ---- descubrimiento -----------------------------------------------------
    def discover(self):
        try:
            markets = self.client.get_markets()
        except Exception as e:
            log(f"[discovery] error: {e}")
            return
        active = [m for m in markets if m.get("status") == "active"]
        active.sort(key=lambda m: parse_ts(m.get("expiration_time") or m.get("close_time")))
        for m in active[:TRACK_NEAREST]:
            t = m.get("ticker")
            if t and t not in self.markets:
                exp = parse_ts(m.get("expiration_time") or m.get("close_time"))
                opn = parse_ts(m.get("open_time"))
                self.markets[t] = Market(t, exp, opn)
                dt = int(exp - time.time())
                log(f"[discovery] siguiendo {t}  (vence en {dt}s)")

    # ---- trades -------------------------------------------------------------
    def poll_trades(self, m: Market):
        try:
            trades = self.client.get_trades(m.ticker, m.min_ts)
        except Exception as e:
            log(f"[trades {m.ticker}] error: {e}")
            return
        newest = m.min_ts
        for tr in trades:
            tid = tr.get("trade_id")
            if tid in self.seen_trades:
                continue
            self.seen_trades.add(tid)
            ct = parse_ts(tr.get("created_time"))
            if ct > newest:
                newest = int(ct)
            hit, lado, precio = classify_trade(tr)
            if hit:
                n = trade_count(tr)
                self.absorptions.append({
                    "ts_deteccion": iso(ct),
                    "market_id": m.ticker,     # Kalshi usa el ticker como id único de mercado
                    "ticker": m.ticker,
                    "lado": lado,
                    "precio_absorcion": round(precio, 4),
                    "contratos": n,
                    "ts_vencimiento": iso(m.expiration_ts),
                    "resultado_yes": "",       # se rellena al resolver
                    "gano_ese_lado": "",
                    "_resolved": False,
                })
                log(f"[ABSORCIÓN] {m.ticker}  {lado.upper()} @ ${precio:.4f}  x{n} contratos")
        m.min_ts = newest

    # ---- resolución ---------------------------------------------------------
    def check_resolution(self, m: Market):
        try:
            data = self.client.get_market(m.ticker)
        except Exception as e:
            log(f"[resolve {m.ticker}] error: {e}")
            return
        result = data.get("result", "")
        status = data.get("status", "")
        if result in ("yes", "no"):
            m.result = result
            m.resolved = True
            self._resolve_absorptions(m)
            log(f"[RESUELTO] {m.ticker} -> {result.upper()}  (status={status})")

    def _resolve_absorptions(self, m: Market):
        for rec in self.absorptions:
            if rec["market_id"] == m.ticker and not rec["_resolved"]:
                rec["resultado_yes"] = "1" if m.result == "yes" else "0"
                rec["gano_ese_lado"] = "1" if rec["lado"] == m.result else "0"
                rec["_resolved"] = True

    # ---- limpieza -----------------------------------------------------------
    def _cleanup(self):
        for t in [t for t, m in self.markets.items() if m.resolved]:
            del self.markets[t]
        if len(self.seen_trades) > 50000:
            self.seen_trades = set(list(self.seen_trades)[-25000:])

    # ---- persistencia -------------------------------------------------------
    def save_csv(self):
        with open(CSV_PATH, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
            w.writeheader()
            for rec in self.absorptions:
                w.writerow({k: rec[k] for k in self.CSV_FIELDS})

    # ---- bucle principal ----------------------------------------------------
    def run(self):
        log("Bot de investigación de absorción iniciado. Ctrl+C para reporte final.")
        try:
            while True:
                now = time.time()

                if now - self.last_discovery > DISCOVERY_INTERVAL:
                    self.discover()
                    self.last_discovery = now

                for m in list(self.markets.values()):
                    if m.resolved:
                        continue
                    tte = m.expiration_ts - now
                    # seguir sondeando trades hasta ~10s después del vencimiento
                    if tte > -10:
                        interval = POLL_INTERVAL_HOT if tte < HOT_WINDOW else POLL_INTERVAL_NORMAL
                        if now - m.last_trade_poll >= interval:
                            self.poll_trades(m)
                            m.last_trade_poll = now
                    # una vez vencido, intentar resolver
                    if now >= m.expiration_ts and now - m.last_resolve_check >= RESOLVE_CHECK_INTERVAL:
                        self.check_resolution(m)
                        m.last_resolve_check = now

                if now - self.last_save > SAVE_INTERVAL:
                    self.save_csv()
                    self.last_save = now

                self._cleanup()
                time.sleep(1)
        except KeyboardInterrupt:
            log("Interrumpido. Guardando y generando reporte...")
        finally:
            self.save_csv()
            self.report()

    # ---- análisis / reporte -------------------------------------------------
    def report(self):
        resolved = [r for r in self.absorptions if r["_resolved"]]
        pending = len(self.absorptions) - len(resolved)

        # Agrupar por MERCADO ÚNICO + lado (nunca por fila: una orden grande
        # se fragmenta en cientos de trades y inflaría la muestra).
        markets: dict[str, dict] = {}
        for r in resolved:
            e = markets.setdefault(r["market_id"], {"yes": 0, "no": 0, "result": None})
            e[r["lado"]] += r["contratos"]
            e["result"] = "yes" if r["resultado_yes"] == "1" else "no"

        yes_mk = [e for e in markets.values() if e["yes"] > 0]
        no_mk  = [e for e in markets.values() if e["no"]  > 0]
        yes_wins = sum(1 for e in yes_mk if e["result"] == "yes")
        no_wins  = sum(1 for e in no_mk  if e["result"] == "no")

        print("\n" + "=" * 64)
        print(f"REPORTE — ABSORCIÓN DE PÁNICO (precio <= ${PANIC_PRICE:.2f})")
        print("=" * 64)
        print(f"Absorciones registradas (filas de trade):  {len(self.absorptions)}")
        print(f"  · resueltas:                             {len(resolved)}")
        print(f"  · pendientes de resolución:              {pending}")
        print(f"Mercados ÚNICOS con >=1 absorción:          {len(markets)}")
        print("-" * 64)
        print(f"YES absorbido barato en {len(yes_mk)} mercados")
        print(f"   -> YES ganó (resolvió $1.00) en {yes_wins}   [{pct(yes_wins, len(yes_mk))}]")
        print(f"NO  absorbido barato en {len(no_mk)} mercados")
        print(f"   -> NO  ganó (resolvió $1.00) en {no_wins}   [{pct(no_wins, len(no_mk))}]")
        print("-" * 64)
        self._size_report(resolved)
        print("=" * 64)
        print(f"CSV completo en: {CSV_PATH}")

    def _size_report(self, resolved: list):
        # Evento = (mercado, lado). Sumamos contratos de todos los fragmentos.
        events: dict[tuple, dict] = {}
        for r in resolved:
            k = (r["market_id"], r["lado"])
            e = events.setdefault(k, {"contratos": 0, "gano": r["gano_ese_lado"] == "1"})
            e["contratos"] += r["contratos"]

        buckets = {
            f"pocos   (<{SIZE_SMALL_MAX})": [],
            f"medio   ({SIZE_SMALL_MAX}-{SIZE_MASSIVE_MIN - 1})": [],
            f"masivo  (>={SIZE_MASSIVE_MIN})": [],
        }
        names = list(buckets.keys())
        for e in events.values():
            c = e["contratos"]
            key = names[0] if c < SIZE_SMALL_MAX else (names[2] if c >= SIZE_MASSIVE_MIN else names[1])
            buckets[key].append(e["gano"])

        print("Win rate por tamaño de la absorción (evento = mercado+lado):")
        for name, lst in buckets.items():
            wins = sum(1 for g in lst if g)
            print(f"   {name:<22} n={len(lst):<4} ganó={wins:<4} [{pct(wins, len(lst))}]")


# ----------------------------------------------------------------------------
def main():
    load_dotenv()
    api_key_id = os.getenv("KALSHI_API_KEY")
    key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")
    if not api_key_id:
        raise SystemExit("Falta KALSHI_API_KEY en el .env")
    if not os.path.exists(key_path):
        raise SystemExit(f"No encuentro la llave privada RSA en: {key_path}")

    bot = AbsorcionBot(KalshiClient(KalshiAuth(api_key_id, key_path)))
    bot.run()


if __name__ == "__main__":
    main()
