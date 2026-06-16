import base64
import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional

import gspread
import requests
from fastapi import FastAPI
from google.oauth2.service_account import Credentials


# -----------------------------
# Config
# -----------------------------

SHEET_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
HEADERS = ["symbol", "close", "sma_200", "sma_50", "pos_52w", "dollar_vol_m"]
FORMULA_SAFE_COLUMN_COUNT = 7  # Keep column G available for sheet-owned scoring formulas.


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Config:
    alpaca_api_key: str
    alpaca_secret_key: str
    alpaca_paper: bool
    alpaca_data_feed: str
    google_sheet_id: str
    worksheet_name: str
    batch_size: int
    lookback_days: int
    request_timeout_seconds: int
    request_retries: int
    request_sleep_seconds: float
    rate_limit_sleep_seconds: int
    error_sleep_seconds: int
    market_closed_sleep_seconds: int
    success_sleep_seconds: float
    sheet_clear_max_rows: int
    min_chunk_success_ratio: float
    allow_off_hours_data_pull: bool

    @property
    def trading_base_url(self) -> str:
        if self.alpaca_paper:
            return "https://paper-api.alpaca.markets"
        return "https://api.alpaca.markets"

    @property
    def data_base_url(self) -> str:
        return "https://data.alpaca.markets"


def load_config() -> Config:
    return Config(
        alpaca_api_key=required_env("ALPACA_API_KEY"),
        alpaca_secret_key=required_env("ALPACA_SECRET_KEY"),
        alpaca_paper=env_bool("ALPACA_PAPER", True),
        alpaca_data_feed=os.getenv("ALPACA_DATA_FEED", "iex").strip().lower(),
        google_sheet_id=required_env("GOOGLE_SHEET_ID"),
        worksheet_name=os.getenv("GOOGLE_WORKSHEET_NAME", "Screener"),
        batch_size=env_int("BATCH_SIZE", 100),
        lookback_days=env_int("LOOKBACK_DAYS", 430),
        request_timeout_seconds=env_int("REQUEST_TIMEOUT_SECONDS", 10),
        request_retries=env_int("REQUEST_RETRIES", 5),
        request_sleep_seconds=env_float("REQUEST_SLEEP_SECONDS", 0.75),
        rate_limit_sleep_seconds=env_int("RATE_LIMIT_SLEEP_SECONDS", 20),
        error_sleep_seconds=env_int("ERROR_SLEEP_SECONDS", 30),
        market_closed_sleep_seconds=env_int("MARKET_CLOSED_SLEEP_SECONDS", 60),
        success_sleep_seconds=env_float("SUCCESS_SLEEP_SECONDS", 0.0),
        sheet_clear_max_rows=env_int("SHEET_CLEAR_MAX_ROWS", 20000),
        min_chunk_success_ratio=env_float("MIN_CHUNK_SUCCESS_RATIO", 0.90),
        allow_off_hours_data_pull=env_bool("ALLOW_OFF_HOURS_DATA_PULL", False),
    )


# -----------------------------
# Logging and app status
# -----------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("alpaca-screener")

STATUS_LOCK = threading.Lock()
STATUS: Dict[str, Any] = {
    "started": False,
    "market_open": None,
    "last_run_started_at": None,
    "last_run_finished_at": None,
    "last_success_at": None,
    "last_error": None,
    "rows_written": 0,
    "allow_off_hours_data_pull": None,
}


def set_status(**kwargs: Any) -> None:
    with STATUS_LOCK:
        STATUS.update(kwargs)


# -----------------------------
# Clients
# -----------------------------


def alpaca_session(cfg: Config) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "APCA-API-KEY-ID": cfg.alpaca_api_key,
            "APCA-API-SECRET-KEY": cfg.alpaca_secret_key,
            "Accept": "application/json",
        }
    )
    return session


def google_client() -> gspread.Client:
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")

    if raw_json:
        try:
            info = json.loads(raw_json)
        except json.JSONDecodeError:
            # Railway env vars sometimes work better with base64 encoded JSON.
            info = json.loads(base64.b64decode(raw_json).decode("utf-8"))
        creds = Credentials.from_service_account_info(info, scopes=SHEET_SCOPES)
    else:
        creds = Credentials.from_service_account_file(file_path, scopes=SHEET_SCOPES)

    return gspread.authorize(creds)


def worksheet(gc: gspread.Client, cfg: Config) -> gspread.Worksheet:
    spreadsheet = gc.open_by_key(cfg.google_sheet_id)
    try:
        return spreadsheet.worksheet(cfg.worksheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=cfg.worksheet_name, rows=1000, cols=FORMULA_SAFE_COLUMN_COUNT)


# -----------------------------
# HTTP helpers
# -----------------------------


def retry_after_seconds(resp: requests.Response) -> Optional[float]:
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value < 0 or not math.isfinite(value):
        return None
    return value


def get_json(
    session: requests.Session,
    url: str,
    cfg: Config,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None

    for attempt in range(1, cfg.request_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=cfg.request_timeout_seconds)

            if resp.status_code == 429:
                exc = requests.HTTPError(f"rate limited status 429: {resp.text[:300]}", response=resp)
                last_exc = exc
                if attempt >= cfg.request_retries:
                    break
                retry_after = retry_after_seconds(resp)
                sleep_for = retry_after if retry_after is not None else cfg.rate_limit_sleep_seconds * attempt
                log.warning("GET rate limited attempt=%s url=%s err=%s; retrying in %ss", attempt, url, exc, sleep_for)
                time.sleep(sleep_for)
                continue

            if resp.status_code in {500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable status {resp.status_code}: {resp.text[:300]}", response=resp)

            resp.raise_for_status()
            data = resp.json()
            if cfg.request_sleep_seconds > 0:
                time.sleep(cfg.request_sleep_seconds)
            return data

        except Exception as exc:  # noqa: BLE001 - log and retry any request/JSON error
            last_exc = exc
            if attempt >= cfg.request_retries:
                break
            sleep_for = min(2 ** attempt, 15)
            log.warning("GET failed attempt=%s url=%s err=%s; retrying in %ss", attempt, url, exc, sleep_for)
            time.sleep(sleep_for)

    raise RuntimeError(f"GET failed after {cfg.request_retries} attempts: {url}: {last_exc}")


# -----------------------------
# Alpaca data
# -----------------------------


def market_is_open(session: requests.Session, cfg: Config) -> bool:
    data = get_json(session, f"{cfg.trading_base_url}/v2/clock", cfg)
    return bool(data.get("is_open"))


def get_tradable_symbols(session: requests.Session, cfg: Config) -> List[str]:
    data = get_json(
        session,
        f"{cfg.trading_base_url}/v2/assets",
        cfg,
        params={"status": "active", "asset_class": "us_equity"},
    )
    symbols = [
        asset["symbol"]
        for asset in data
        if asset.get("tradable") is True
        and asset.get("status") == "active"
        and asset.get("class") == "us_equity"
        and asset.get("symbol")
    ]
    return sorted(set(symbols))


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_daily_bars_for_chunk(
    session: requests.Session,
    cfg: Config,
    symbols: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=cfg.lookback_days)
    url = f"{cfg.data_base_url}/v2/stocks/bars"

    params: Dict[str, Any] = {
        "symbols": ",".join(symbols),
        "timeframe": "1Day",
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "limit": 10000,
        "sort": "asc",
    }
    if cfg.alpaca_data_feed:
        params["feed"] = cfg.alpaca_data_feed

    bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in symbols}
    page_token: Optional[str] = None

    while True:
        page_params = dict(params)
        if page_token:
            page_params["page_token"] = page_token

        data = get_json(session, url, cfg, params=page_params)
        page_bars = data.get("bars") or {}
        for symbol, bars in page_bars.items():
            if symbol in bars_by_symbol:
                bars_by_symbol[symbol].extend(bars or [])

        page_token = data.get("next_page_token")
        if not page_token:
            break

    for symbol in bars_by_symbol:
        bars_by_symbol[symbol].sort(key=lambda b: b.get("t", ""))

    return bars_by_symbol


# -----------------------------
# Metrics
# -----------------------------


def as_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def rounded(value: Optional[float], digits: int) -> Any:
    if value is None or not math.isfinite(value):
        return ""
    return round(value, digits)


def row_for_symbol(symbol: str, bars: List[Dict[str, Any]]) -> Optional[List[Any]]:
    clean = []
    for bar in bars:
        close = as_float(bar.get("c"))
        high = as_float(bar.get("h"))
        low = as_float(bar.get("l"))
        volume = as_float(bar.get("v"))
        if close is None or high is None or low is None or volume is None:
            continue
        if close <= 0 or high <= 0 or low <= 0 or volume < 0:
            continue
        clean.append({"close": close, "high": high, "low": low, "volume": volume})

    if not clean:
        return None

    closes = [b["close"] for b in clean]
    latest = clean[-1]
    close = latest["close"]

    # For recent IPOs/new listings, use whatever valid history exists instead
    # of requiring a full 50/200 trading-day window. Example: if a symbol has
    # 17 bars, both SMA columns use those 17 closes.
    sma_50 = mean(closes[-50:])
    sma_200 = mean(closes[-200:])

    # Same idea for the 52-week position: use the available history up to 252
    # trading days. If the observed high/low range is zero, use a neutral 0.5
    # rather than leaving the cell blank for brand-new/flat-history symbols.
    window_52w = clean[-252:]
    high_52w = max(b["high"] for b in window_52w)
    low_52w = min(b["low"] for b in window_52w)
    if high_52w > low_52w:
        pos_52w = (close - low_52w) / (high_52w - low_52w)
        pos_52w = max(0.0, min(1.0, pos_52w))
    else:
        pos_52w = 0.5

    dollar_vol_m = close * latest["volume"] / 1_000_000.0

    row = [
        symbol,
        rounded(close, 4),
        rounded(sma_200, 4),
        rounded(sma_50, 4),
        rounded(pos_52w, 4),
        rounded(dollar_vol_m, 2),
    ]

    # Inclusion filter: only close and dollar volume are hard requirements.
    # Recent IPOs can have less than 50/200/252 bars, so SMA and pos_52w are
    # computed from available history and are not used to exclude the symbol.
    close_value = as_float(row[1])
    dollar_vol_value = as_float(row[5])
    if close_value is None or close_value <= 0:
        return None
    if dollar_vol_value is None or dollar_vol_value <= 0:
        return None

    return row


# -----------------------------
# Sheets
# -----------------------------


def ensure_sheet_size_for_screener(ws: gspread.Worksheet, needed_rows: int) -> None:
    """Resize only when necessary, and never shrink/delete column G.

    The screener owns columns A:F. Column G is intentionally reserved for
    Google Sheets formulas used by downstream bots. A prior implementation
    resized the sheet to exactly six columns, which could remove column G.
    """
    target_rows = max(needed_rows, ws.row_count)
    target_cols = max(ws.col_count, FORMULA_SAFE_COLUMN_COUNT)

    if ws.row_count < needed_rows or ws.col_count < FORMULA_SAFE_COLUMN_COUNT:
        ws.resize(rows=target_rows, cols=target_cols)


def clear_screener_sheet(gc: gspread.Client, cfg: Config) -> None:
    ws = worksheet(gc, cfg)
    ensure_sheet_size_for_screener(ws, cfg.sheet_clear_max_rows)
    ws.batch_clear([f"A1:F{cfg.sheet_clear_max_rows}"])
    log.info("Screener columns A:F cleared because market is closed; column G preserved")


def write_screener_sheet(gc: gspread.Client, cfg: Config, rows: List[List[Any]]) -> None:
    ws = worksheet(gc, cfg)
    values = [HEADERS] + rows

    needed_rows = max(len(values) + 10, 1000)
    ensure_sheet_size_for_screener(ws, needed_rows)

    # Update only A:F. Column G is reserved for sheet-owned scoring formulas.
    # Update first, then clear extra old rows below. This keeps the prior screener visible
    # while the new run is being computed.
    ws.update(range_name="A1:F" + str(len(values)), values=values, value_input_option="RAW")

    clear_start = len(values) + 1
    if clear_start <= cfg.sheet_clear_max_rows:
        ws.batch_clear([f"A{clear_start}:F{cfg.sheet_clear_max_rows}"])

    log.info("Wrote %s screener rows to columns A:F; column G preserved", len(rows))


# -----------------------------
# Main screener loop
# -----------------------------


def build_screener_rows(session: requests.Session, cfg: Config) -> List[List[Any]]:
    symbols = get_tradable_symbols(session, cfg)
    log.info("Found %s active tradable Alpaca US equity symbols", len(symbols))

    rows_by_symbol: Dict[str, List[Any]] = {}
    total_chunks = 0
    successful_chunks = 0
    skipped_invalid_price_or_volume = 0

    for chunk in chunks(symbols, cfg.batch_size):
        total_chunks += 1
        try:
            bars_by_symbol = fetch_daily_bars_for_chunk(session, cfg, chunk)
            chunk_kept = 0
            for symbol in chunk:
                row = row_for_symbol(symbol, bars_by_symbol.get(symbol, []))
                if row is None:
                    skipped_invalid_price_or_volume += 1
                    continue
                rows_by_symbol[symbol] = row
                chunk_kept += 1
            successful_chunks += 1
            log.info(
                "Processed chunk %s symbols=%s kept=%s skipped_invalid_price_or_volume=%s success",
                total_chunks,
                len(chunk),
                chunk_kept,
                len(chunk) - chunk_kept,
            )
        except Exception as exc:  # noqa: BLE001 - continue so one chunk does not kill process
            log.exception("Chunk failed; skipping this chunk. first_symbol=%s err=%s", chunk[0], exc)

    if total_chunks == 0:
        raise RuntimeError("No symbol chunks were created")

    success_ratio = successful_chunks / total_chunks
    if success_ratio < cfg.min_chunk_success_ratio:
        raise RuntimeError(
            f"Only {successful_chunks}/{total_chunks} chunks succeeded "
            f"({success_ratio:.1%}); refusing to overwrite sheet"
        )

    rows = [rows_by_symbol[symbol] for symbol in symbols if symbol in rows_by_symbol]
    log.info(
        "Built %s screener rows; skipped %s symbols with blank/zero close or dollar_vol_m",
        len(rows),
        skipped_invalid_price_or_volume,
    )
    return rows


def screener_loop() -> None:
    cfg = load_config()
    session = alpaca_session(cfg)
    gc = google_client()
    closed_sheet_already_blank = False

    set_status(started=True, allow_off_hours_data_pull=cfg.allow_off_hours_data_pull)
    log.info(
        "Screener service started request_sleep_seconds=%s rate_limit_sleep_seconds=%s request_retries=%s",
        cfg.request_sleep_seconds,
        cfg.rate_limit_sleep_seconds,
        cfg.request_retries,
    )

    while True:
        try:
            set_status(last_error=None)

            try:
                is_open = market_is_open(session, cfg)
                set_status(market_open=is_open)
            except Exception as exc:  # noqa: BLE001 - fail safe if Alpaca clock is unavailable
                if cfg.allow_off_hours_data_pull:
                    log.exception(
                        "Market clock check failed, but ALLOW_OFF_HOURS_DATA_PULL=true; attempting screener run anyway. err=%s",
                        exc,
                    )
                    set_status(market_open=None, last_error=f"market clock check failed; continuing because override is enabled: {exc}")
                    is_open = True
                else:
                    log.exception(
                        "Market clock check failed; treating market as closed and clearing screener. err=%s",
                        exc,
                    )
                    set_status(market_open=None, last_error=f"market clock check failed: {exc}")
                    if not closed_sheet_already_blank:
                        clear_screener_sheet(gc, cfg)
                        closed_sheet_already_blank = True
                        set_status(rows_written=0)
                    time.sleep(cfg.error_sleep_seconds)
                    continue

            if not is_open and not cfg.allow_off_hours_data_pull:
                if not closed_sheet_already_blank:
                    clear_screener_sheet(gc, cfg)
                    closed_sheet_already_blank = True
                    set_status(rows_written=0)
                time.sleep(cfg.market_closed_sleep_seconds)
                continue

            if not is_open and cfg.allow_off_hours_data_pull:
                log.info("Market is closed, but ALLOW_OFF_HOURS_DATA_PULL=true; pulling screener data anyway")

            closed_sheet_already_blank = False
            started_at = datetime.now(timezone.utc).isoformat()
            set_status(last_run_started_at=started_at)

            rows = build_screener_rows(session, cfg)
            write_screener_sheet(gc, cfg, rows)

            finished_at = datetime.now(timezone.utc).isoformat()
            set_status(
                last_run_finished_at=finished_at,
                last_success_at=finished_at,
                rows_written=len(rows),
            )

            if cfg.success_sleep_seconds > 0:
                time.sleep(cfg.success_sleep_seconds)

        except Exception as exc:  # noqa: BLE001 - never let the perpetual worker die
            log.exception("Screener loop error: %s", exc)
            set_status(last_error=str(exc))
            time.sleep(cfg.error_sleep_seconds)


app = FastAPI(title="Alpaca Screener")


@app.on_event("startup")
def start_worker() -> None:
    thread = threading.Thread(target=screener_loop, daemon=True)
    thread.start()


@app.get("/")
def root() -> Dict[str, str]:
    return {"service": "alpaca-screener", "status": "ok"}


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    with STATUS_LOCK:
        return dict(STATUS)
