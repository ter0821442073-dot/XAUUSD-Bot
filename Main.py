import io
import os
import sys
import json
import html
import time
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import yfinance as yf

try:  # ให้ print ขึ้นหน้า Logs ของ Render ทันที
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ==========================================
# ⚙️ ตั้งค่า (ค่าลับอ่านจาก Environment Variable บน Render)
# ==========================================
TOKEN = os.environ["TG_TOKEN"]
CHAT_ID = os.environ["TG_CHAT_ID"]
SHEET_ID = os.environ["SHEET_ID"]

# 📡 MT5_FEED=1 = ใช้ราคาจาก EA ใน MT5 (ส่งเข้า /feed) แทน Yahoo / ตั้ง PRICE_OFFSET เป็น 0 ให้อัตโนมัติ
FEED_ENABLED = os.environ.get("MT5_FEED", "0") == "1"
SYMBOL = os.environ.get("SYMBOL", "XAUUSD" if FEED_ENABLED else "GC=F")
# ส่วนต่างราคา futures - spot (GC=F สูงกว่า MT5 12 จุด ใส่ 12) ข้อความแจ้งเตือนจะแสดงเป็นราคาระดับ MT5
PRICE_OFFSET = 0.0 if FEED_ENABLED else float(os.environ.get("PRICE_OFFSET", "0"))

# เวลา MT5 = UTC + SERVER_UTC_OFFSET_HOURS (ค่าคงที่ ไม่เปลี่ยนตาม DST) ค่าเริ่มต้น 0 = GMT+0
# ถ้าโบรกเป็นแบบ NY close (GMT+2/+3 ตาม DST) ให้ตั้งค่านี้เป็นค่าว่าง แล้วใช้ SERVER_SHIFT_HOURS (NY + 7) แทน
NY_TZ = "America/New_York"
_utc_off = os.environ.get("SERVER_UTC_OFFSET_HOURS", "0").strip()
SERVER_UTC_OFFSET_HOURS = float(_utc_off) if _utc_off != "" else None
SERVER_SHIFT_HOURS = int(os.environ.get("SERVER_SHIFT_HOURS", "7"))
H4_OFFSET_HOURS = int(os.environ.get("H4_OFFSET_HOURS", "0"))
H4_LIVE_REQUIRE_STOCH = os.environ.get("H4_LIVE_REQUIRE_STOCH", "0") == "1"

# Stochastic (K period, Slowing)
STOCH_K = int(os.environ.get("STOCH_K", "14"))
STOCH_SLOW = int(os.environ.get("STOCH_SLOW", "3"))

# 1) กันเตือนทันทีซ้ำรัว: เตือนฝั่งเดิมใหม่ได้เมื่อผ่านไปกี่แท่ง (เปลี่ยนทิศ = รีเซ็ต)
COOLDOWN_BARS = {
    "1h": int(os.environ.get("COOLDOWN_1H", "4")),
    "4h": int(os.environ.get("COOLDOWN_4H", "2")),
}

# 2) แท่งคู่ต้องเป็นกลืนกิน หรือมีไส้เทียนปฏิเสธราคา (ตั้ง 0 = แท่งคู่ธรรมดาก็ผ่าน)
REQUIRE_STRONG_PATTERN = os.environ.get("REQUIRE_STRONG_PATTERN", "1") == "1"
ENGULF_TOL = float(os.environ.get("ENGULF_TOL", "0.5"))   # ความคลาดเคลื่อนราคาเปิด (ดอลลาร์)
WICK_RATIO = float(os.environ.get("WICK_RATIO", "2"))     # ไส้ต้องยาว >= กี่เท่าของตัวแท่ง

# 4) เทียบ %K ของ 4H กับสัญญาณ 1H (ติดป้าย / ตั้ง H4_ZONE_FILTER=1 = ไม่ตรงโซนไม่ส่ง)
ZONE_SELL = float(os.environ.get("ZONE_SELL", "70"))
ZONE_BUY = float(os.environ.get("ZONE_BUY", "30"))
H4_ZONE_FILTER = os.environ.get("H4_ZONE_FILTER", "0") == "1"

# 5) SL/TP ตาม ATR
ATR_PERIOD = int(os.environ.get("ATR_PERIOD", "14"))
ATR_MULT = float(os.environ.get("ATR_MULT", "1.5"))

# 6) ช่วงเสี่ยง: รอยต่อวันตลาดทอง (16:00-18:00 เวลานิวยอร์ก ไม่ขึ้นกับเวลา MT5) + เวลาข่าวจากคอลัมน์ "News" ในชีต (เวลา MT5)
ROLLOVER_START = int(os.environ.get("ROLLOVER_START", "16"))
ROLLOVER_END = int(os.environ.get("ROLLOVER_END", "18"))
NEWS_BEFORE_MIN = int(os.environ.get("NEWS_BEFORE_MIN", "30"))
NEWS_AFTER_MIN = int(os.environ.get("NEWS_AFTER_MIN", "30"))
SKIP_RISK_WINDOWS = os.environ.get("SKIP_RISK_WINDOWS", "0") == "1"  # 1 = ไม่ส่งสัญญาณช่วงเสี่ยง

CHECK_INTERVAL = 60
# Yahoo มักส่งแท่งชั่วโมงใหม่ช้าไม่กี่นาที: ถ้ายังไม่เห็นแท่งใหม่ ให้ถือว่าแท่งล่าสุดยังวิ่งอยู่ได้อีกกี่วินาที
# (กันการตัดสินแท่งปิดจากข้อมูลที่ยังไม่ครบ และไม่พลาดเตือนทันทีช่วงต้นชั่วโมง)
CLOSE_GRACE_SEC = int(os.environ.get("CLOSE_GRACE_SEC", "600"))
HEARTBEAT_INTERVAL = 43200
ERROR_ALERT_AT = 3
STATE_FILE = "alert_state.json"

# เก็บสถานะ (รวมสมุดบันทึกผลไม้) ไว้ที่ Upstash Redis ฟรี เพื่อไม่ให้หายตอน Render รีสตาร์ท/redeploy (ไม่บังคับ)
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().strip("\"'“”‘’ ").rstrip("/")   # ตัดเครื่องหมายคำพูดที่เผลอติดมา
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip().strip("\"'“”‘’ ")
STATE_KEY = os.environ.get("STATE_KEY", "xauusd_bot_state")
REMOTE_ENABLED = bool(UPSTASH_URL and UPSTASH_TOKEN)

# ==========================================
# 💾 สถานะ
# ==========================================
def _remote(cmd):
    r = requests.post(UPSTASH_URL, headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}, json=cmd, timeout=8)
    r.raise_for_status()
    return r.json().get("result")


def load_state():
    if REMOTE_ENABLED:
        try:
            raw = _remote(["GET", STATE_KEY])
            if raw:
                data = json.loads(raw)
                if isinstance(data, dict):
                    print("☁️ โหลดสถานะจาก Upstash แล้ว")
                    return data
        except Exception as e:
            print(f"⚠️ โหลดสถานะจาก Upstash ไม่สำเร็จ: {e}")
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(sent, f)
    except Exception as e:
        print(f"⚠️ บันทึกสถานะไม่สำเร็จ: {e}")
    if REMOTE_ENABLED:
        try:
            _remote(["SET", STATE_KEY, json.dumps(sent)])
        except Exception as e:
            print(f"⚠️ บันทึกสถานะลง Upstash ไม่สำเร็จ: {e}")


sent = load_state()  # "1h:live_sell" -> id แท่งที่แจ้งแล้ว / "1h:cd_sell" -> แท่งที่เตือนทันทีล่าสุด / "ledger" -> สมุดบันทึกผลไม้
last_heartbeat_time = time.time()
last_cycle_time = time.time()
WATCHDOG_SEC = int(os.environ.get("WATCHDOG_SEC", "600"))
sheet_errors = 0
data_errors = 0
sheet_alerted = False
data_alerted = False
sheet_last_err = ""

# ==========================================
# 🌐 เว็บเซิร์ฟเวอร์จำลองให้ Render ผ่านพอร์ต
# ==========================================
# ==========================================
# 📡 รับราคาจาก EA ใน MT5 (POST /feed) เก็บในหน่วยความจำ ใช้แทน Yahoo เมื่อ MT5_FEED=1
# EA ส่งแท่ง M1 / M5 / H1 (เวลาเซิร์ฟเวอร์โบรกเกอร์ ตรงกับ "เวลา MT5" ที่บอทใช้) / ถ้าบอทรีสตาร์ท EA จะส่งประวัติใหม่ให้เอง
# ==========================================
FEED_SECRET = os.environ.get("FEED_SECRET", "").strip().strip("\"'“”‘’ ")
FEED_STALE_SEC = int(os.environ.get("FEED_STALE_SEC", "180"))      # ไม่มีข้อมูลจาก EA เกินกี่วินาที ถือว่าขาดการเชื่อมต่อ
FEED_CAP = {"M1": 5000, "M5": 20000, "M15": 3000, "H1": 2000}                  # เก็บสูงสุดกี่แท่งต่อ TF
FEED_MIN = {"M1": 500, "M5": 500, "M15": 100, "H1": 100}                      # ต่ำกว่านี้ = บอกให้ EA ส่งประวัติมาเพิ่ม
_feed = {"bars": {k: {} for k in FEED_CAP}, "rx": 0.0, "srv_off": None, "resync": set()}
_feed_lock = threading.Lock()
_feed_warned_off = None


def feed_ingest(data):
    tf = data.get("tf")
    if tf not in _feed["bars"]:
        raise ValueError("tf ไม่ถูกต้อง")
    with _feed_lock:
        if _feed["rx"] > 0 and time.time() - _feed["rx"] > FEED_STALE_SEC and not _feed["resync"]:
            _feed["resync"] = set(_feed["bars"])      # ขาดการเชื่อมต่อมา: ให้ EA ส่งประวัติเติมช่องว่างทุก TF
        d = _feed["bars"][tf]
        for b in data.get("bars", []):
            d[int(b[0])] = (float(b[1]), float(b[2]), float(b[3]), float(b[4]))
        if len(d) > FEED_CAP[tf]:
            for k in sorted(d)[:len(d) - FEED_CAP[tf]]:
                d.pop(k, None)
        _feed["rx"] = time.time()
        if "srv" in data and "gmt" in data:
            _feed["srv_off"] = int(data["srv"]) - int(data["gmt"])
        if len(data.get("bars", [])) >= 100:         # ก้อนซิงก์ประวัติ (ไม่ใช่แท่งล่าสุด 3 แท่ง)
            _feed["resync"].discard(tf)
        need = [k for k, v in _feed["bars"].items() if len(v) < FEED_MIN[k] or k in _feed["resync"]]
    return {"ok": True, "need": need}


def feed_df(tf):
    with _feed_lock:
        items = sorted(_feed["bars"][tf].items())
    if not items:
        return None
    df = pd.DataFrame([v for _, v in items], columns=["Open", "High", "Low", "Close"],
                      index=pd.to_datetime([k for k, _ in items], unit="s"))
    return df


def feed_fresh():
    """ข้อมูลจาก EA ใช้ได้: ส่งมาล่าสุดไม่เกิน FEED_STALE_SEC และไม่ได้รอซิงก์ประวัติเติมช่องว่างหลังขาดการเชื่อมต่อ"""
    return (time.time() - _feed["rx"]) <= FEED_STALE_SEC and not _feed["resync"]


# ---------- ราคาสำรองจาก Yahoo เมื่อ EA ไม่ส่งราคา (เน็ต/ไฟดับ/MT5 ค้าง) ----------
FEED_FALLBACK = os.environ.get("FEED_FALLBACK", "1") == "1"        # 1 = ใช้ Yahoo ชั่วคราวเมื่อไม่ได้รับราคาจาก EA
FALLBACK_PRICE_OFFSET = float(os.environ.get("FALLBACK_PRICE_OFFSET", os.environ.get("PRICE_OFFSET", "0") or "0"))
FEED_SWITCH_DELAY = int(os.environ.get("FEED_SWITCH_DELAY", "120"))   # ขาดราคาต่อเนื่องกี่วินาทีถึงแจ้งว่าสลับไป Yahoo
_fb = {"off": None}        # ส่วนต่าง Yahoo(GC=F) - MT5 ที่คำนวณล่าสุด
_src = {"mode": "mt5", "stale_since": None}


def fallback_offset():
    return _fb["off"] if _fb["off"] is not None else FALLBACK_PRICE_OFFSET


def _calibrate(yh):
    """หาส่วนต่าง Yahoo - MT5 จากแท่ง 1H ที่ซ้อนกันในหน่วยความจำ (ค่ามัธยฐาน 24 แท่งล่าสุด) ถ้าหาไม่ได้ใช้ค่าเดิม/ค่าตั้งเอง"""
    try:
        m = feed_df("H1")
        if m is None or yh is None:
            return
        common = yh.index.intersection(m.index)
        if len(common) < 6:
            return
        off = float((yh.loc[common, "Close"] - m.loc[common, "Close"]).tail(24).median())
        if abs(off) <= 60:
            _fb["off"] = off
    except Exception as e:
        print(f"calibrate error: {e}")


def _to_mt5(df):
    return None if df is None else df - fallback_offset()


def data_source_note():
    """ข้อความเตือนต่อท้ายแจ้งเตือนเมื่อกำลังใช้ราคาสำรอง"""
    if FEED_ENABLED and FEED_FALLBACK and not feed_fresh():
        return f"⚠️ ราคาจาก Yahoo (สำรอง) ปรับให้ตรง MT5 ประมาณ {fallback_offset():+.2f} จุด อาจคลาดเคลื่อน เช็กกราฟ MT5 ก่อนเสมอ"
    return ""


def fetch_1h():
    if not FEED_ENABLED:
        return _fetch_1h_yahoo()
    if feed_fresh():
        return feed_df("H1")
    if not FEED_FALLBACK:
        return None
    yh = _fetch_1h_yahoo()
    _calibrate(yh)
    return _to_mt5(yh)


def fetch_5m(days):
    if not FEED_ENABLED:
        return _fetch_5m_yahoo(days)
    m = feed_df("M5")
    if feed_fresh() and m is not None:
        return m
    if FEED_FALLBACK:
        yh = _fetch_5m_yahoo(days)
        if yh is not None:
            return _to_mt5(yh)
    return m


def fetch_1m():
    if not FEED_ENABLED:
        return _fetch_1m_yahoo()
    if feed_fresh():
        return feed_df("M1")
    if not FEED_FALLBACK:
        return None
    return _to_mt5(_fetch_1m_yahoo())


def fetch_15m():
    if not FEED_ENABLED:
        return _fetch_15m_yahoo()
    if feed_fresh():
        return feed_df("M15")
    if not FEED_FALLBACK:
        return None
    return _to_mt5(_fetch_15m_yahoo())


def feed_source_notice():
    """แจ้ง Telegram เมื่อสลับระหว่าง MT5 กับ Yahoo (ต้องขาดราคาต่อเนื่อง FEED_SWITCH_DELAY วินาทีก่อน กันแจ้งมั่วตอนบอทรีสตาร์ท)"""
    now = time.time()
    if feed_fresh():
        _src["stale_since"] = None
        if _src["mode"] == "yahoo":
            _src["mode"] = "mt5"
            send_telegram_message("✅ <b>กลับมาใช้ราคาจาก MT5 แล้ว</b>\nEA ส่งราคาและซิงก์ประวัติเรียบร้อย")
        return
    if _src["stale_since"] is None:
        _src["stale_since"] = now
    if _src["mode"] == "mt5" and now - _src["stale_since"] >= FEED_SWITCH_DELAY:
        _src["mode"] = "yahoo"
        if FEED_FALLBACK:
            send_telegram_message(
                "⚠️ <b>ไม่ได้รับราคาจาก MT5 สลับไปใช้ Yahoo ชั่วคราว</b>\n"
                f"ปรับราคาให้ตรง MT5 ด้วยส่วนต่าง {fallback_offset():+.2f} จุด แต่อาจคลาดเคลื่อนได้ โดยเฉพาะแท่ง M1\n"
                "📡 ตรวจว่าคอม/VPS ที่รัน MT5 เปิดอยู่ เน็ตไม่หลุด และ EA ยังทำงาน")
        else:
            send_telegram_message("⚠️ <b>ไม่ได้รับราคาจาก MT5</b> บอทหยุดส่งสัญญาณจนกว่าจะกลับมา (FEED_FALLBACK=0)")


class SimpleHandler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            if self.path.split("?")[0] != "/feed" or not FEED_ENABLED:
                self._json(404, {"ok": False, "error": "not found"})
                return
            if not FEED_SECRET:
                self._json(503, {"ok": False, "error": "ยังไม่ตั้ง FEED_SECRET ในบอท"})
                return
            n = int(self.headers.get("Content-Length", "0"))
            if n <= 0 or n > 3_000_000:
                self._json(413, {"ok": False, "error": "ขนาดข้อมูลไม่ถูกต้อง"})
                return
            data = json.loads(self.rfile.read(n).decode("utf-8"))
            if data.get("secret") != FEED_SECRET:
                self._json(403, {"ok": False, "error": "secret ไม่ตรง"})
                return
            self._json(200, feed_ingest(data))
        except Exception as e:
            self._json(400, {"ok": False, "error": str(e)[:100]})

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"XAUUSD Bot is running!")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def run_web():
    port = int(os.environ.get("PORT", "10000"))
    ThreadingHTTPServer(("0.0.0.0", port), SimpleHandler).serve_forever()

# ==========================================
# 📨 Telegram
# ==========================================
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print(f"Telegram ส่งไม่สำเร็จ [{r.status_code}]: {r.text}")
            return False
        return True
    except Exception as e:
        print(f"Telegram Error: {e}")
        return False


def fire(key, bar_id, msg):
    """ส่งครั้งเดียวต่อแท่งต่อ key คืน True เฉพาะเมื่อเพิ่งส่งสำเร็จ"""
    if sent.get(key) == bar_id:
        return False
    if send_telegram_message(msg):
        sent[key] = bar_id
        save_state()
        print(f"✅ ส่งแจ้งเตือนแล้ว: {key}")
        return True
    print(f"❌ ส่งแจ้งเตือนไม่สำเร็จ: {key}")
    return False


def cooldown_ok(tf, side, level, bar_start, delta):
    last = sent.get(f"{tf}:cd_{side}:{level:.2f}")
    if not last:
        return True
    try:
        gap = (bar_start - pd.Timestamp(last)) / delta
    except Exception:
        return True
    return gap >= COOLDOWN_BARS[tf]


def mark_cooldown(tf, side, level, bar_start):
    opp = "buy" if side == "sell" else "sell"
    sent[f"{tf}:cd_{side}:{level:.2f}"] = str(bar_start)
    for k in [k for k in sent if k.startswith(f"{tf}:cd_{opp}")]:
        sent.pop(k, None)  # เปลี่ยนทิศ = ปลดล็อกฝั่งตรงข้าม
    save_state()

# ==========================================
# 📊 Google Sheets
# ==========================================
def parse_news(df):
    out = []
    if "News" in df.columns:
        for v in df["News"].dropna().astype(str):
            try:
                out.append(pd.Timestamp(v.strip()))
            except Exception:
                pass
    return out


def read_levels(df, col):
    """อ่านทุกช่องที่มีตัวเลขในคอลัมน์ (แถว 2 ลงไป) = หลายแนว"""
    if col not in df.columns:
        raise ValueError(f"ไม่พบคอลัมน์ {col} ในชีต")
    vals = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").dropna()
    if vals.empty:
        print(f"ℹ️ คอลัมน์ {col} ไม่มีแนว (ว่าง)")   # ว่างได้ เพราะแนวที่เบรกแล้วถูกลบออกจากชีตอัตโนมัติ
    return sorted(float(v) for v in vals)


def fetch_sheet_df():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def get_levels_from_google_sheet():
    try:
        df = fetch_sheet_df()
        try:
            trades = parse_trades(df)
            trades_ok = getattr(parse_trades, "last_ok", True)
        except Exception as e:
            print(f"⚠️ อ่านออเดอร์จากชีตไม่สำเร็จ: {e}")
            trades, trades_ok = [], False
        problems = list(getattr(parse_trades, "last_problems", []))
        result = {
            "1h": {"res": read_levels(df, "1H_Res"), "sup": read_levels(df, "1H_Sup")},
            "4h": {"res": read_levels(df, "4H_Res"), "sup": read_levels(df, "4H_Sup")},
            "news": parse_news(df),
            "trades": trades,
            "trades_ok": trades_ok,
            "problems": problems,
        }
        apply_removed(result)
        get_levels_from_google_sheet.last_error = ""
        return result
    except Exception as e:
        get_levels_from_google_sheet.last_error = str(e)
        print(f"⚠️ อ่าน Google Sheets ไม่สำเร็จ: {e}")
        return None


get_levels_from_google_sheet.last_error = ""

# ==========================================
# 🕒 เวลา MT5
# ==========================================
def now_server():
    if SERVER_UTC_OFFSET_HOURS is not None:
        utc = datetime.now(timezone.utc).replace(tzinfo=None)
        return pd.Timestamp(utc + timedelta(hours=SERVER_UTC_OFFSET_HOURS))
    ny = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ)).replace(tzinfo=None)
    return pd.Timestamp(ny + timedelta(hours=SERVER_SHIFT_HOURS))


def to_server_index(idx):
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    if SERVER_UTC_OFFSET_HOURS is not None:
        return idx.tz_convert("UTC").tz_localize(None) + pd.Timedelta(hours=SERVER_UTC_OFFSET_HOURS)
    return idx.tz_convert(NY_TZ).tz_localize(None) + pd.Timedelta(hours=SERVER_SHIFT_HOURS)


def fmt(ts):
    return ts.strftime("%d/%m %H:%M")


def px(v):
    """แปลงราคา GC=F เป็นระดับราคา MT5 สำหรับแสดงผล"""
    return v - PRICE_OFFSET

# ==========================================
# 📈 ข้อมูลราคา + อินดิเคเตอร์
# ==========================================
def _fetch_1h_yahoo():
    df = yf.download(SYMBOL, period="30d", interval="1h", progress=False, auto_adjust=False, timeout=20)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()

    df.index = to_server_index(df.index)
    return df[~df.index.duplicated(keep="last")]


def to_4h(df_1h):
    return (
        df_1h.resample("4h", offset=f"{H4_OFFSET_HOURS}h")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
        .dropna()
    )


def calculate_stoch(df):
    low_min = df["Low"].rolling(STOCH_K).min()
    high_max = df["High"].rolling(STOCH_K).max()
    k_raw = 100 * (df["Close"] - low_min) / (high_max - low_min)
    return k_raw.rolling(STOCH_SLOW).mean()


def calculate_atr(df):
    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [df["High"] - df["Low"], (df["High"] - prev_close).abs(), (df["Low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(ATR_PERIOD).mean()


def ohlc(df, i):
    r = df.iloc[i]
    return float(r["Open"]), float(r["High"]), float(r["Low"]), float(r["Close"])

# ==========================================
# 🧩 ตัวช่วยประกอบสัญญาณ
# ==========================================
def reversal_pattern(side, p_open, p_close, c_open, c_high, c_low, c_close):
    """คืนชื่อรูปแบบถ้าผ่านเงื่อนไขแท่งคู่ (None = ไม่ผ่าน)"""
    body = max(abs(c_close - c_open), 0.01)
    if side == "sell":
        if not (p_close > p_open and c_close < c_open):
            return None
        if c_close <= p_open and c_open >= p_close - ENGULF_TOL:
            return "กลืนกิน"
        if (c_high - max(c_open, c_close)) >= WICK_RATIO * body:
            return "ไส้เทียนปฏิเสธราคา"
    else:
        if not (p_close < p_open and c_close > c_open):
            return None
        if c_close >= p_open and c_open <= p_close + ENGULF_TOL:
            return "กลืนกิน"
        if (min(c_open, c_close) - c_low) >= WICK_RATIO * body:
            return "ไส้เทียนปฏิเสธราคา"
    return None if REQUIRE_STRONG_PATTERN else "แท่งคู่ธรรมดา"


def zone_label(side, k4):
    """ป้ายเทียบ %K ของ 4H คืน (ข้อความ, ตรงโซนหรือไม่)"""
    if k4 is None or pd.isna(k4):
        return "", True
    ok = k4 >= ZONE_SELL if side == "sell" else k4 <= ZONE_BUY
    if ok:
        return f"✅ 4H %K {k4:.0f} อยู่โซนเดียวกัน", True
    return f"⚠️ 4H %K {k4:.0f} ไม่ตรงโซน", False


def sl_tp_text(side, entry, atr_val, target):
    if atr_val is None or pd.isna(atr_val):
        return ""
    risk = ATR_MULT * atr_val
    if side == "sell":
        sl = entry + risk
        reward, tp_ok = (entry - target, True) if target is not None else (0, False)
        tp_name = "แนวรับ"
    else:
        sl = entry - risk
        reward, tp_ok = (target - entry, True) if target is not None else (0, False)
        tp_name = "แนวต้าน"
    text = f"🛑 SL ({ATR_MULT:g}×ATR {atr_val:.2f}): <code>{px(sl):.2f}</code>"
    if tp_ok and risk > 0:
        text += f"\n🎯 เป้า ({tp_name}): <code>{px(target):.2f}</code> | R:R 1:{reward / risk:.1f}"
    return text


def risk_notes(news_times):
    notes = []
    now = now_server()
    ny_hour = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ)).hour
    if ROLLOVER_START <= ny_hour < ROLLOVER_END:
        notes.append("⚠️ ช่วงรอยต่อวัน สเปรดกว้าง")
    for t in news_times:
        if t - timedelta(minutes=NEWS_BEFORE_MIN) <= now <= t + timedelta(minutes=NEWS_AFTER_MIN):
            notes.append(f"⚠️ ใกล้ช่วงข่าวแรง ({t:%H:%M} เวลา MT5)")
            break
    return notes


def compose(title, label, lines, side, entry, atr_val, target, zone_txt, risk, note=""):
    parts = [title, f"📊 XAUUSD ({SYMBOL}) | 🕒 แท่ง {label} (เวลา MT5)"]
    parts += lines
    parts.append(zone_txt)
    parts.append(sl_tp_text(side, entry, atr_val, target))
    parts += risk
    parts.append(note)
    if PRICE_OFFSET:
        parts.append(f"ℹ️ ราคาแสดงเป็นระดับ MT5 (GC=F − {PRICE_OFFSET:g})")
    parts.append(data_source_note())
    parts.append("📱 <b>เปิด MT5 เช็กกราฟก่อนตัดสินใจ</b>")
    return "\n".join(p for p in parts if p)

# ==========================================
# 🕒 ตลาดเปิดหรือไม่
# ==========================================
def is_market_open():
    """ทองปิดศุกร์ 17:00 - เปิดอาทิตย์ 18:00 เวลานิวยอร์ก (อิงเวลานิวยอร์กจริง รวม DST)"""
    ny = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ))
    wd, hr = ny.weekday(), ny.hour
    if wd == 5:
        return False
    if wd == 4 and hr >= 17:
        return False
    if wd == 6 and hr < 18:
        return False
    return True

# ==========================================
# 🔔 ตรวจสัญญาณ
# ==========================================
def next_target(side, entry, sup_list, res_list):
    """เป้าหมาย = แนวถัดไปในทิศที่ราคาจะไป (ขาย: แนวรับใต้ราคา / ซื้อ: แนวต้านเหนือราคา)"""
    if side == "sell":
        c = [s for s in sup_list if s < entry]
        return max(c) if c else None
    c = [r for r in res_list if r > entry]
    return min(c) if c else None


def pre_alerted(tf, side, bar_id):
    """เคยเตือนทันทีฝั่งนี้ในแท่งนี้แล้วหรือไม่ (ไม่ว่าชนแนวไหน)"""
    prefix = f"{tf}:live_{side}"
    return any(k.startswith(prefix) and v == bar_id for k, v in sent.items())


def _seen_key(tf, kind, level_gc):
    return f"{tf}:{kind}:{level_gc - PRICE_OFFSET:.2f}"


def update_first_seen(levels):
    """จดเวลาที่บอทเห็นแนวแต่ละเส้นในชีตครั้งแรก (ใช้กันแจ้งเตือนจากการชนที่เกิดก่อนเพิ่มแนว)
    รอบแรกสุด: แนวที่มีอยู่แล้วถือว่าเป็นแนวเก่า (ไม่บล็อกอะไร) / แนวที่ถูกลบออกจากชีตจะถูกเคลียร์"""
    first_run = "seen" not in sent
    seen = sent.setdefault("seen", {})
    now_txt = str(now_server())
    cur = set()
    changed = first_run
    for tf in ("1h", "4h"):
        for kind in ("res", "sup"):
            for v in levels[tf][kind]:
                k = f"{tf}:{kind}:{v:.2f}"
                cur.add(k)
                if k not in seen:
                    seen[k] = "2000-01-01 00:00:00" if first_run else now_txt
                    changed = True
    for k in [k for k in seen if k not in cur]:
        seen.pop(k, None)
        changed = True
    if changed:
        save_state()


def level_known_before(tf, kind, level_gc, ts):
    """แนวนี้ถูกเพิ่มในชีตก่อนเวลา ts หรือไม่ (ไม่มีบันทึก = ถือว่ามีมาก่อน)"""
    seen = sent.get("seen", {}).get(_seen_key(tf, kind, level_gc))
    if not seen:
        return True
    try:
        return pd.Timestamp(seen) <= ts
    except Exception:
        return True


def level_armed(tf, kind, level_gc, bar_start):
    """นับการชนแนวในแท่งนี้ได้หรือไม่: แนวมีอยู่ก่อนแท่งเริ่ม = นับ / แนวเพิ่มกลางแท่ง = ต้องมีการชนหลังเวลาที่เพิ่ม
    (กันเตือนจากการชนที่เกิดไปแล้วก่อนเพิ่มแนว ตอนที่ราคาวิ่งหนีแนวไปไกลแล้ว)"""
    seen = sent.get("seen", {}).get(_seen_key(tf, kind, level_gc))
    if not seen:
        return True
    try:
        added = pd.Timestamp(seen)
    except Exception:
        return True
    if added <= bar_start:
        return True
    bars = fetch_5m(2)
    if bars is None:
        print(f"[{tf.upper()}] แนว {px(level_gc):.2f} เพิ่งเพิ่มกลางแท่ง ดึงข้อมูล 5 นาทีไม่ได้ ข้ามรอบนี้")
        return False
    sub = bars[bars.index >= added.floor("5min") + pd.Timedelta(minutes=5)]
    if sub.empty:
        return False
    ok = bool((sub["High"] >= level_gc).any()) if kind == "res" else bool((sub["Low"] <= level_gc).any())
    if not ok:
        print(f"[{tf.upper()}] แนว {px(level_gc):.2f} เพิ่มในชีตเมื่อ {fmt(added)} หลังจากชนไปแล้ว → ยังไม่แจ้ง รอชนใหม่")
    return ok


def check_timeframe(tf, df, levels, last_1h_start, k4_now):
    if len(df) < 25:
        return

    delta = pd.Timedelta(hours=1 if tf == "1h" else 4)
    stoch_k = calculate_stoch(df)
    atr = calculate_atr(df)
    now = now_server()

    last_start = df.index[-1]
    last_end = last_start + delta
    is_live = last_end + pd.Timedelta(seconds=CLOSE_GRACE_SEC) > now

    # 4H ที่ปิดแล้ว แต่ข้อมูล 1H ชั่วโมงสุดท้ายยังมาไม่ครบ → รอรอบหน้า
    if not is_live and tf == "4h" and last_1h_start < last_end - pd.Timedelta(hours=1):
        return

    live_i = -1 if is_live else None
    cl_i = -2 if is_live else -1
    pv_i = cl_i - 1

    res_list = [v + PRICE_OFFSET for v in levels[tf]["res"]]
    sup_list = [v + PRICE_OFFSET for v in levels[tf]["sup"]]
    tf_u = tf.upper()
    atr_val = float(atr.iloc[cl_i])
    risk = risk_notes(levels.get("news", []))

    print(f"[{tf_u}] แนวต้าน {[round(v, 2) for v in res_list]} | แนวรับ {[round(v, 2) for v in sup_list]} "
          f"(ระดับ GC=F รวม offset {PRICE_OFFSET:g}) | แท่งสด={is_live}")

    if SKIP_RISK_WINDOWS and risk:
        print(f"[{tf_u}] ข้ามรอบนี้ (ช่วงเสี่ยง): {' / '.join(risk)}")
        return

    # ---------- แท่งที่กำลังวิ่ง: แจ้งเตือนทันที ----------
    if live_i is not None:
        _, l_high, l_low, l_close = ohlc(df, live_i)
        k_live = float(stoch_k.iloc[live_i])
        lid = str(df.index[live_i])
        llabel = fmt(df.index[live_i])
        need_stoch = (tf == "1h") or H4_LIVE_REQUIRE_STOCH
        k_txt = "-" if pd.isna(k_live) else f"{k_live:.1f}"
        stoch_part = " + Stoch" if need_stoch else ""
        note = "⏳ แท่งยังไม่ปิด รอสัญญาณยืนยันตอนปิดแท่ง" if tf == "1h" else "⏳ แท่งยังไม่ปิด"

        print(f"[{tf_u}] สด {llabel} H:{l_high:.2f} L:{l_low:.2f} C:{l_close:.2f} | %K {k_txt}")

        for side in ("sell", "buy"):
            if side == "sell":
                hit = [r for r in res_list if l_high >= r and level_armed(tf, "res", r, df.index[live_i])]
                level = max(hit) if hit else None   # แนวต้านสูงสุดที่ชนแล้ว
                stoch_ok = (not need_stoch) or (not pd.isna(k_live) and k_live >= 80)
            else:
                hit = [s for s in sup_list if l_low <= s and level_armed(tf, "sup", s, df.index[live_i])]
                level = min(hit) if hit else None   # แนวรับต่ำสุดที่ชนแล้ว
                stoch_ok = (not need_stoch) or (not pd.isna(k_live) and k_live <= 20)
            if level is None:
                continue
            cd_ok = cooldown_ok(tf, side, level, df.index[live_i], delta)
            print(f"[{tf_u}] {side}: ชนแนว {level:.2f} | Stoch ผ่าน={stoch_ok} | พ้น cooldown={cd_ok}")
            if not stoch_ok or not cd_ok:
                continue
            zone_txt, zone_ok = zone_label(side, k4_now) if tf == "1h" else ("", True)
            if H4_ZONE_FILTER and not zone_ok:
                continue

            target = next_target(side, l_close, sup_list, res_list)
            if side == "sell":
                title = f"⚡ <b>เตือนทันที SELL (ชนแนวต้าน{stoch_part}) - TF {tf_u}</b>"
                lines = [
                    f"📈 High: <code>{px(l_high):.2f}</code> ชนแนวต้าน <code>{px(level):.2f}</code>",
                    f"💵 ราคาตอนนี้: <code>{px(l_close):.2f}</code> | Stoch %K: <code>{k_txt}</code>",
                ]
            else:
                title = f"⚡ <b>เตือนทันที BUY (ชนแนวรับ{stoch_part}) - TF {tf_u}</b>"
                lines = [
                    f"📉 Low: <code>{px(l_low):.2f}</code> ชนแนวรับ <code>{px(level):.2f}</code>",
                    f"💵 ราคาตอนนี้: <code>{px(l_close):.2f}</code> | Stoch %K: <code>{k_txt}</code>",
                ]

            msg = compose(title, llabel, lines, side, l_close, atr_val, target, zone_txt, risk, note)
            if fire(f"{tf}:live_{side}:{level:.2f}", lid, msg):
                mark_cooldown(tf, side, level, df.index[live_i])

    # ---------- แท่งที่ปิดแล้ว: ยืนยันแท่งคู่ / Stoch ตัดกลับ ----------
    if len(df) < abs(pv_i):
        return
    c_open, c_high, c_low, c_close = ohlc(df, cl_i)
    p_open, _, _, p_close = ohlc(df, pv_i)
    k_c, k_p = float(stoch_k.iloc[cl_i]), float(stoch_k.iloc[pv_i])
    if pd.isna(k_c) or pd.isna(k_p):
        return

    cid = str(df.index[cl_i])
    clabel = fmt(df.index[cl_i])

    sell_hit = [r for r in res_list if c_high >= r and level_armed(tf, "res", r, df.index[cl_i])]
    buy_hit = [s for s in sup_list if c_low <= s and level_armed(tf, "sup", s, df.index[cl_i])]
    sell_level = max(sell_hit) if sell_hit else None
    buy_level = min(buy_hit) if buy_hit else None

    # ถ้าเคยเตือนทันที (1H) ในแท่งนี้แล้ว ถือว่าชนแนว + Stoch เกิดขึ้นแล้ว
    sell_pre_alerted = tf == "1h" and pre_alerted(tf, "sell", cid)
    buy_pre_alerted = tf == "1h" and pre_alerted(tf, "buy", cid)

    sell_pat = reversal_pattern("sell", p_open, p_close, c_open, c_high, c_low, c_close)
    buy_pat = reversal_pattern("buy", p_open, p_close, c_open, c_high, c_low, c_close)

    z_sell_txt, z_sell_ok = zone_label("sell", k4_now) if tf == "1h" else ("", True)
    z_buy_txt, z_buy_ok = zone_label("buy", k4_now) if tf == "1h" else ("", True)
    sell_zone_ok = z_sell_ok or not H4_ZONE_FILTER
    buy_zone_ok = z_buy_ok or not H4_ZONE_FILTER

    print(f"[{tf_u}] ปิด {clabel} H:{c_high:.2f} L:{c_low:.2f} C:{c_close:.2f} | %K {k_c:.1f} (ก่อนหน้า {k_p:.1f}) | ATR {atr_val:.2f}")

    if sell_level is not None and (k_c >= 80 or sell_pre_alerted) and sell_pat and sell_zone_ok:
        lines = [
            f"📈 High: <code>{px(c_high):.2f}</code> ชนแนวต้าน <code>{px(sell_level):.2f}</code>",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code> (ห่างแนวต้าน {sell_level - c_close:.2f})",
            f"📉 แท่งคู่: เขียว ➡️ แดง ({sell_pat}) | Stoch %K: <code>{k_c:.2f}</code>",
        ]
        fire(f"{tf}:close_sell", cid,
             compose(f"🚨 <b>ยืนยัน SELL (ชนแนวต้าน + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>",
                     clabel, lines, "sell", c_close, atr_val,
                     next_target("sell", c_close, sup_list, res_list), z_sell_txt, risk))
    elif tf == "4h" and k_p >= 80 and k_c < 80:
        lines = [
            f"📉 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&lt; 80)",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code>",
        ]
        fire("4h:cross_sell", cid,
             compose("🚨 <b>สัญญาณ SELL (Stoch ตัดกลับ) - TF 4H</b>",
                     clabel, lines, "sell", c_close, atr_val,
                     next_target("sell", c_close, sup_list, res_list), "", risk))
    elif buy_level is not None and (k_c <= 20 or buy_pre_alerted) and buy_pat and buy_zone_ok:
        lines = [
            f"📉 Low: <code>{px(c_low):.2f}</code> ชนแนวรับ <code>{px(buy_level):.2f}</code>",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code> (ห่างแนวรับ {c_close - buy_level:.2f})",
            f"📈 แท่งคู่: แดง ➡️ เขียว ({buy_pat}) | Stoch %K: <code>{k_c:.2f}</code>",
        ]
        fire(f"{tf}:close_buy", cid,
             compose(f"🟢 <b>ยืนยัน BUY (ชนแนวรับ + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>",
                     clabel, lines, "buy", c_close, atr_val,
                     next_target("buy", c_close, sup_list, res_list), z_buy_txt, risk))
    elif tf == "4h" and k_p <= 20 and k_c > 20:
        lines = [
            f"📈 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&gt; 20)",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code>",
        ]
        fire("4h:cross_buy", cid,
             compose("🟢 <b>สัญญาณ BUY (Stoch ตัดกลับ) - TF 4H</b>",
                     clabel, lines, "buy", c_close, atr_val,
                     next_target("buy", c_close, sup_list, res_list), "", risk))

# ==========================================
# 🧾 ออเดอร์จากชีต + สรุปผลรายสัปดาห์/รายเดือน
# คอลัมน์ในชีต: Time, Side, Entry, SL, TP (บังคับ) | SL0, Exit, ExitTime (ไม่บังคับ ใช้เมื่อปิดมือ/ขยับ SL)
# ราคาในชีตเป็นระดับ MT5, เวลาเป็นเวลา MT5 รูปแบบ 2026-10-27 14:30
# ==========================================
BE_R = float(os.environ.get("BE_R", "0.05"))                       # |R| ไม่เกินค่านี้ นับเป็นเสมอ
BE_PTS = float(os.environ.get("BE_PTS", "0.5"))                       # ไม้ที่ไม่มี SL เดิม: |กำไร| ไม่เกินกี่จุด นับเป็นเสมอ
MONITOR_DAYS = 4                                                   # เฝ้าไม้ที่เปิดไม่เกินกี่วัน
TRADE_ALERT_WINDOW_MIN = int(os.environ.get("TRADE_ALERT_WINDOW_MIN", "20"))
WEEKLY_FORCE = os.environ.get("WEEKLY_FORCE", "0") == "1"          # ทดสอบ: ส่งสรุปทันที 1 ครั้งต่อการรัน
_weekly_force_done = False
_known_exits = None   # {id ไม้: ราคา Exit} ที่บอทเห็นแล้ว (กันแจ้งซ้ำ แต่แจ้งเมื่อกรอก/แก้ Exit ใหม่)
# แจ้งผลเมื่อกรอก Exit (ปิดมือ) ถ้าเวลาปิดอยู่ภายในกี่นาทีที่ผ่านมา (กันส่งซ้ำของเก่าเมื่อบอทรีสตาร์ท)
MANUAL_ALERT_WINDOW_MIN = int(os.environ.get("MANUAL_ALERT_WINDOW_MIN", "120"))
# เวลาในคอลัมน์ Time/ExitTime ของชีต: ค่าว่าง = เวลา MT5 / ตั้ง 7 = เวลาไทย (UTC+7)
_sheet_off = os.environ.get("SHEET_TIME_UTC_OFFSET_HOURS", "").strip()
SHEET_TIME_UTC_OFFSET_HOURS = float(_sheet_off) if _sheet_off != "" else None
_bars5_cache = {"ts": 0.0, "period": None, "df": None}


def _fetch_5m_yahoo(days):
    period = "5d" if days <= 5 else "1mo" if days <= 29 else "60d"
    c = _bars5_cache
    if c["df"] is not None and c["period"] == period and time.time() - c["ts"] < 100:
        return c["df"]
    df = yf.download(SYMBOL, period=period, interval="5m", progress=False, auto_adjust=False, timeout=30)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()
    df.index = to_server_index(df.index)
    df = df[~df.index.duplicated(keep="last")]
    c.update(ts=time.time(), period=period, df=df)
    return df


def utc_to_server(dt_utc_naive):
    idx = pd.DatetimeIndex([dt_utc_naive]).tz_localize("UTC")
    return to_server_index(idx)[0]


def _num(v):
    try:
        x = float(str(v).replace(",", "").strip())
        return None if pd.isna(x) else x
    except Exception:
        return None


def _ts(v):
    try:
        if v is None or str(v).strip() in ("", "nan", "NaT", "None"):
            return None
        txt = str(v).strip()
        # รูปแบบ 2026-10-27 14:30 อ่านตามปกติ / รูปแบบไทย 27/10/2026 14:30 อ่านแบบ วัน/เดือน/ปี
        iso = len(txt) >= 5 and txt[:4].isdigit() and txt[4] == "-"
        t = pd.to_datetime(txt, errors="coerce") if iso else pd.to_datetime(txt, errors="coerce", dayfirst=True)
        return None if pd.isna(t) else pd.Timestamp(t)
    except Exception:
        return None


def sheet_to_server(t):
    """แปลงเวลาที่กรอกในชีตเป็นเวลา MT5 (ถ้าตั้ง SHEET_TIME_UTC_OFFSET_HOURS)"""
    if t is None or SHEET_TIME_UTC_OFFSET_HOURS is None:
        return t
    return utc_to_server(t - pd.Timedelta(hours=SHEET_TIME_UTC_OFFSET_HOURS))


def parse_trades(df):
    """อ่านออเดอร์จากชีต: Time, Side, Entry, SL, TP (บังคับ) | SL0 = SL เดิม, Exit, ExitTime (ไม่บังคับ)
    SL ขยับกันหน้าไม้ (ฝั่งกำไร) ได้ ใส่ SL0 เพื่อให้คิด R จากความเสี่ยงเดิม"""
    need = ["Time", "Side", "Entry", "SL", "TP"]
    problems = []
    parse_trades.last_problems = problems
    parse_trades.last_ok = True
    if not all(c in df.columns for c in need):
        parse_trades.last_ok = False   # ไม่มีคอลัมน์ออเดอร์ครบ = อ่านไม่สำเร็จ (ห้ามประกาศว่า "แก้แล้ว")
        return []
    trades = []
    for i, row in df.iterrows():
        side_raw = str(row["Side"]).strip().lower()
        if side_raw in ("", "nan", "none"):
            continue
        if side_raw in ("buy", "b", "long", "ซื้อ"):
            side = "buy"
        elif side_raw in ("sell", "s", "short", "ขาย"):
            side = "sell"
        else:
            reason = f"Side '{side_raw}' ไม่รู้จัก ใช้ BUY หรือ SELL"
            problems.append((i + 2, reason))
            print(f"⚠️ ออเดอร์แถว {i + 2}: {reason}")
            continue
        t, entry, sl, tp = sheet_to_server(_ts(row["Time"])), _num(row["Entry"]), _num(row["SL"]), _num(row["TP"])
        sl0 = _num(row["SL0"]) if "SL0" in df.columns else None
        reason = None
        if t is None:
            reason = "อ่านเวลา (Time) ไม่ออก"
        elif entry is None or sl is None or tp is None:
            reason = "Entry/SL/TP ว่างหรือไม่ใช่ตัวเลข"
        elif side == "buy" and not (tp > entry and sl < tp):
            reason = f"BUY ต้องมี TP &gt; Entry และ SL &lt; TP แต่ตอนนี้ Entry {entry:g} | SL {sl:g} | TP {tp:g}"
        elif side == "sell" and not (tp < entry and sl > tp):
            reason = f"SELL ต้องมี TP &lt; Entry และ SL &gt; TP แต่ตอนนี้ Entry {entry:g} | SL {sl:g} | TP {tp:g}"
        elif sl0 is not None and ((side == "buy" and sl0 >= entry) or (side == "sell" and sl0 <= entry)):
            reason = f"SL0 (SL เดิม) ต้องอยู่ฝั่งขาดทุน แต่ตอนนี้ Entry {entry:g} | SL0 {sl0:g}"
        if reason:
            problems.append((i + 2, reason))
            print(f"⚠️ ออเดอร์แถว {i + 2} ไม่ถูกนับ: {reason}")
            continue

        protected = (sl >= entry) if side == "buy" else (sl <= entry)   # SL กันหน้าไม้ (อยู่ฝั่งกำไร)
        if sl0 is not None:
            risk = abs(entry - sl0)
        elif not protected:
            risk = abs(entry - sl)
        else:
            risk = None   # ขยับ SL แล้วไม่มี SL เดิม คิด R ไม่ได้ (นับผลจากจุดราคา)
        trades.append({
            "row": i + 2,
            "id": f"{t:%Y%m%d%H%M}|{side}|{entry:g}",
            "time": t, "side": side, "entry": entry, "sl": sl, "tp": tp,
            "sl0": sl0, "risk": risk, "protected": protected,
            "exit": _num(row["Exit"]) if "Exit" in df.columns else None,
            "exit_time": sheet_to_server(_ts(row["ExitTime"])) if "ExitTime" in df.columns else None,
        })
    return trades


parse_trades.last_problems = []
parse_trades.last_ok = True


def classify(r, pts):
    """ชนะ/แพ้/เสมอ: ใช้ R ถ้ามี ไม่มีก็ใช้จุดราคา"""
    if r is not None:
        return "win" if r > BE_R else "loss" if r < -BE_R else "be"
    return "win" if pts > BE_PTS else "loss" if pts < -BE_PTS else "be"


def evaluate_trade(t, bars):
    """คืนสถานะไม้: closed (win/loss/be + R + จุดราคา) / open / future / unknown (เก่าเกินข้อมูล)"""
    risk = t.get("risk", abs(t["entry"] - t["sl"]))
    planned = (abs(t["tp"] - t["entry"]) / risk) if risk else None
    base = {"id": t["id"], "side": t["side"], "planned_rr": planned, "time": t["time"]}

    if t["exit"] is not None:  # ปิดมือ / ขยับ SL: ใช้ราคา Exit ที่กรอก
        diff = (t["exit"] - t["entry"]) if t["side"] == "buy" else (t["entry"] - t["exit"])
        r = (diff / risk) if risk else None
        return {**base, "status": "closed", "how": "manual", "result": classify(r, diff), "r": r,
                "close_time": t["exit_time"] or t["time"], "ambiguous": False, "pts": diff}

    if t["time"] > now_server() + pd.Timedelta(minutes=5):
        return {**base, "status": "future"}

    if bars is None or bars.empty or t["time"] < bars.index[0]:
        return {**base, "status": "unknown"}

    sub = bars[bars.index >= t["time"].floor("5min") + pd.Timedelta(minutes=5)]
    if sub.empty:
        return {**base, "status": "open"}

    buy = t["side"] == "buy"
    sl_p, tp_p = t["sl"] + PRICE_OFFSET, t["tp"] + PRICE_OFFSET
    if buy:
        hit_sl, hit_tp = sub["Low"] <= sl_p, sub["High"] >= tp_p
    else:
        hit_sl, hit_tp = sub["High"] >= sl_p, sub["Low"] <= tp_p

    if t.get("protected"):
        # SL กันหน้าไม้ย้ายได้ก็ต่อเมื่อราคาเคยวิ่งผ่านระดับนั้นในฝั่งกำไรแล้ว: นับ SL ชนเฉพาะหลังจากนั้น
        armed = (sub["High"] >= sl_p) if buy else (sub["Low"] <= sl_p)
        if armed.any():
            hit_sl = hit_sl & (sub.index > armed.idxmax())
        else:
            hit_sl = hit_sl & False

    any_hit = hit_sl | hit_tp
    if not any_hit.any():
        return {**base, "status": "open"}

    first = any_hit.idxmax()
    sl_hit, tp_hit = bool(hit_sl.loc[first]), bool(hit_tp.loc[first])
    if sl_hit:  # ถ้า SL และ TP อยู่ในแท่ง 5 นาทีเดียวกัน นับเป็น SL (ระมัดระวังไว้ก่อน)
        pts = (t["sl"] - t["entry"]) if buy else (t["entry"] - t["sl"])
        r = (pts / risk) if risk else None
        return {**base, "status": "closed", "how": "sl", "result": classify(r, pts), "r": r,
                "close_time": first, "ambiguous": tp_hit, "pts": pts}
    return {**base, "status": "closed", "how": "tp", "result": "win", "r": planned,
            "close_time": first, "ambiguous": False, "pts": abs(t["tp"] - t["entry"])}


def summarize(results):
    closed = [r for r in results if r["status"] == "closed"]
    n = len(closed)
    w = sum(1 for r in closed if r["result"] == "win")
    l = sum(1 for r in closed if r["result"] == "loss")
    rs = [r["r"] for r in closed if r["r"] is not None]       # R คิดเฉพาะไม้ที่รู้ความเสี่ยงเดิม
    pl = [r["planned_rr"] for r in closed if r["planned_rr"] is not None]
    sum_r = sum(rs)
    sum_pts = sum(r["pts"] for r in closed)

    # Profit Factor = กำไรรวม / ขาดทุนรวม, Max Drawdown = ยอดตกจากจุดสูงสุดของกำไรสะสม (หน่วยจุดราคา)
    pts_seq = [r["pts"] for r in sorted(closed, key=lambda r: r["close_time"])]
    gross_p = sum(p for p in pts_seq if p > 0)
    gross_l = -sum(p for p in pts_seq if p < 0)
    eq = peak = mdd = 0.0
    for p in pts_seq:
        eq += p
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    return {
        "n": n, "w": w, "l": l, "be": n - w - l,
        "wr": (w / n * 100) if n else 0.0,
        "n_r": len(rs), "sum_r": sum_r,
        "avg_r": (sum_r / len(rs)) if rs else 0.0,
        "n_pl": len(pl), "planned": (sum(pl) / len(pl)) if pl else 0.0,
        "sum_pts": sum_pts,
        "avg_pts": (sum_pts / n) if n else 0.0,
        "gp": gross_p, "gl": gross_l,
        "pf": (gross_p / gross_l) if gross_l > 0 else None,
        "mdd": mdd,
        "rf": (sum_pts / mdd) if mdd > 0 else None,
    }


def r_text(s):
    """ข้อความ R เฉลี่ย/รวม (นับเฉพาะไม้ที่มี SL เดิม)"""
    if s["n_r"] == 0:
        return "R: ไม่มีข้อมูล (ไม้ที่ขยับ SL ใส่ SL0 ในชีตจึงจะคิด R ได้)"
    txt = f"เฉลี่ย {s['avg_r']:+.2f}R/ไม้ | รวม {s['sum_r']:+.2f}R"
    if s["n_r"] < s["n"]:
        txt += f" (คิดจาก {s['n_r']}/{s['n']} ไม้)"
    return txt


def pf_text(s):
    if s["pf"] is not None:
        return f"{s['pf']:.2f}"
    return "∞ (ไม่มีไม้ขาดทุน)" if s["gp"] > 0 else "-"


def risk_lines(s):
    """บรรทัด Profit Factor + Max Drawdown"""
    return [
        f"⚖️ Profit Factor: <b>{pf_text(s)}</b> (กำไรรวม {s['gp']:.2f} / ขาดทุนรวม {s['gl']:.2f})",
        f"📉 Max Drawdown: <b>{s['mdd']:.2f} จุดราคา</b>",
    ]


def rf_text(s):
    if s["rf"] is None:
        return "- (ไม่มี Drawdown)"
    return f"{s['rf']:.2f}"


def compare_text(cur, prev, label):
    """เทียบกับรอบก่อน (ไม่แสดงถ้ารอบก่อนไม่มีไม้)"""
    if not prev["n"]:
        return ""
    d = cur["sum_pts"] - prev["sum_pts"]
    arrow = "▲" if d > 0.005 else "▼" if d < -0.005 else "▬"
    lines = [
        f"🔄 <b>เทียบ{label}ก่อน</b> ({prev['n']} ไม้)",
        f"💵 สุทธิ {cur['sum_pts']:+.2f} (ก่อนหน้า {prev['sum_pts']:+.2f}) {arrow} {d:+.2f} จุด",
        f"🎯 WR {cur['wr']:.0f}% (ก่อนหน้า {prev['wr']:.0f}%)",
    ]
    if cur["pf"] is not None and prev["pf"] is not None:
        lines.append(f"⚖️ PF {cur['pf']:.2f} (ก่อนหน้า {prev['pf']:.2f})")
    return "\n".join(lines)


def pnl_label(pts):
    """กำไร/ติดลบสุทธิเป็นจุดราคา (ราคาทอง 1 ดอลลาร์ = 1 จุดราคา)"""
    if pts > 0.005:
        return f"🟢 กำไร {pts:+.2f} จุดราคา"
    if pts < -0.005:
        return f"🔴 ติดลบ {pts:+.2f} จุดราคา"
    return "⚪ เสมอตัว 0.00 จุดราคา"


# ---------- สมุดบันทึกผล (ledger): จำผลไม้ที่ปิดแล้ว แม้ลบแถวในชีตไปแล้ว ----------
LEDGER_KEEP_DAYS = int(os.environ.get("LEDGER_KEEP_DAYS", "400"))   # เก็บผลไม้ไว้กี่วัน (ต้องเกิน 365 เพื่อทำสรุปรายปี)
def remember_closed(t, r):
    """เก็บผลไม้ที่ปิดแล้วลงสมุดบันทึก ไว้ใช้สรุปเดือนเมื่อแถวถูกลบออกจากชีต"""
    led = sent.setdefault("ledger", {})
    entry = {"side": t["side"], "time": str(t["time"]), "ct": str(r["close_time"]),
             "res": r["result"], "r": r["r"], "pts": r["pts"], "pl": r["planned_rr"], "how": r["how"]}
    if led.get(t["id"]) == entry:
        return
    led[t["id"]] = entry
    cutoff = now_server() - pd.Timedelta(days=LEDGER_KEEP_DAYS)
    for k in [k for k, v in led.items() if pd.Timestamp(v["ct"]) < cutoff]:
        led.pop(k, None)
    save_state()


def ledger_to_result(tid, e):
    return {"id": tid, "side": e["side"], "planned_rr": e["pl"], "time": pd.Timestamp(e["time"]),
            "status": "closed", "how": e["how"], "result": e["res"], "r": e["r"],
            "close_time": pd.Timestamp(e["ct"]), "ambiguous": False, "pts": e["pts"]}


def merge_with_ledger(results):
    """รวมผลจากชีตกับสมุดบันทึก: ไม้ที่ชีตตรวจไม่ได้/ถูกลบไปแล้ว ใช้ค่าในสมุดบันทึกแทน"""
    led = sent.get("ledger", {})
    out, seen = [], set()
    for r in results:
        seen.add(r["id"])
        if r["status"] == "unknown" and r["id"] in led:
            out.append(ledger_to_result(r["id"], led[r["id"]]))
        else:
            out.append(r)
    for tid, e in led.items():
        if tid not in seen:
            out.append(ledger_to_result(tid, e))
    return out


_last_backfill = 0.0


def backfill_ledger(trades):
    """ไม้เก่ากว่า MONITOR_DAYS ที่ยังไม่อยู่ในสมุดบันทึก: ย้อนตรวจจากข้อมูล 5 นาทีแล้วบันทึกผล (ทุก 6 ชม.)"""
    global _last_backfill
    if time.time() - _last_backfill < 21600:
        return
    now = now_server()
    led = sent.get("ledger", {})
    todo = [t for t in trades
            if t["exit"] is None and t["id"] not in led
            and timedelta(days=MONITOR_DAYS) < now - t["time"]]
    if not todo:
        _last_backfill = time.time()
        return
    days = min((now - min(t["time"] for t in todo)).days + 2, 59)
    bars = fetch_5m(days)
    if bars is None:
        print("backfill: ดึงข้อมูล 5 นาทีไม่ได้ ลองใหม่รอบหน้า")
        return
    for t in todo:
        r = evaluate_trade(t, bars)
        if r["status"] == "closed":
            remember_closed(t, r)
    _last_backfill = time.time()
    print(f"📒 backfill สมุดบันทึกผลแล้ว ({len(todo)} ไม้ที่ตรวจ)")


def build_weekly_msg(start_s, end_s, week_res, all_res, skipped=0, prev_res=None):
    ws, al = summarize(week_res), summarize(all_res)
    lines = [
        "📊 <b>สรุปผลเทรดประจำสัปดาห์ (ตลาดปิดแล้ว)</b>",
        f"🗓 {fmt(start_s)} → {fmt(end_s)} (เวลา MT5)",
        "",
    ]
    if ws["n"]:
        lines += [
            f"📌 ไม้ที่ปิดทั้งสัปดาห์: <b>{ws['n']}</b> ไม้",
            f"✅ ชนะ {ws['w']} | ❌ แพ้ {ws['l']} | ➖ เสมอ {ws['be']}",
            f"🎯 Win rate: <b>{ws['wr']:.0f}%</b>",
            f"📐 RR ที่วางไว้เฉลี่ย: 1:{ws['planned']:.2f}" if ws["n_pl"] else "📐 RR ที่วางไว้เฉลี่ย: -",
            f"💰 {r_text(ws)}",
            f"💵 สุทธิทั้งสัปดาห์: <b>{pnl_label(ws['sum_pts'])}</b> (เฉลี่ย {ws['avg_pts']:+.2f}/ไม้)",
        ]
        lines += risk_lines(ws)
    else:
        lines.append("📌 สัปดาห์นี้ไม่มีไม้ที่ปิด")
    cmp_txt = compare_text(ws, summarize(prev_res or []), "สัปดาห์")
    if cmp_txt:
        lines += ["", cmp_txt]
    amb = sum(1 for r in week_res if r.get("ambiguous"))
    if amb:
        lines.append(f"⚠️ {amb} ไม้ SL กับ TP อยู่ในแท่ง 5 นาทีเดียวกัน นับเป็นแพ้")
    if al["n"]:
        lines += [
            "",
            f"📚 สะสมทั้งหมด ({al['n']} ไม้): ชนะ {al['w']} แพ้ {al['l']} เสมอ {al['be']}",
            f"🎯 Win rate {al['wr']:.0f}% | {r_text(al)}",
            f"💵 สุทธิสะสม: {pnl_label(al['sum_pts'])}",
        ]
    open_n = sum(1 for r in all_res if r["status"] == "open")
    unknown_n = sum(1 for r in all_res if r["status"] == "unknown")
    future_n = sum(1 for r in all_res if r["status"] == "future")
    if open_n:
        lines.append(f"⏳ ไม้ที่ยังเปิดอยู่: {open_n}")
    if future_n:
        lines.append(f"🕒 {future_n} ไม้ เวลาเข้าไม้อยู่ในอนาคต (เช็กวันที่ในชีต)")
    if skipped:
        lines.append(f"⚠️ มี {skipped} แถวในชีตไม่ถูกนับ (ข้อมูลไม่ถูกต้อง)")
    if unknown_n:
        lines.append(f"❓ ตรวจผลไม่ได้ {unknown_n} ไม้ (เก่าเกินข้อมูล ใส่ Exit ในชีตได้)")
    lines += journal_lines(start_s, end_s)
    return "\n".join(lines)


DAY_START_HOUR = int(os.environ.get("DAY_START_HOUR", "0"))              # วันใหม่เริ่มกี่โมง (เวลา MT5) 0 = เที่ยงคืนของ MT5
DAILY_WINDOW_HOURS = int(os.environ.get("DAILY_WINDOW_HOURS", "3"))      # ส่งสรุปประจำวันภายในกี่ชม.แรกของวันใหม่
WEEKLY_WINDOW_HOURS = int(os.environ.get("WEEKLY_WINDOW_HOURS", "12"))   # ส่งสรุปสัปดาห์ภายในกี่ชม.หลังตลาดปิด


def r_part(r):
    return f"{r['r']:+.2f}R" if r["r"] is not None else "ไม่คิด R"


def trade_alert_msg(t, r):
    side_u = "BUY" if t["side"] == "buy" else "SELL"
    icon, word = {"win": ("✅", "ชนะ"), "loss": ("❌", "แพ้"), "be": ("➖", "เสมอ")}[r["result"]]
    if r["how"] == "tp":
        title = f"🎯 <b>TP ชน - {side_u}</b>"
    elif t.get("protected"):
        title = f"🛡 <b>SL กันหน้าไม้ชน - {side_u}</b>"
    else:
        title = f"🛑 <b>SL ชน - {side_u}</b>"
    tail = f"{icon} {word} <b>{r_part(r)}</b> ({r['pts']:+.2f} จุดราคา)"
    if r.get("ambiguous"):
        tail += "\n⚠️ แท่งเดียวกันชนทั้ง SL และ TP นับเป็น SL"
    return "\n".join([
        title,
        f"🕒 เข้าไม้ {fmt(t['time'])} (เวลา MT5)",
        f"เข้า <code>{t['entry']:.2f}</code> | SL <code>{t['sl']:.2f}</code> | TP <code>{t['tp']:.2f}</code>",
        tail,
    ])


def manual_alert_msg(t, r):
    side_u = "BUY" if t["side"] == "buy" else "SELL"
    icon, word = {"win": ("✅", "ชนะ"), "loss": ("❌", "แพ้"), "be": ("➖", "เสมอ")}[r["result"]]
    if t["exit_time"] is not None:
        when = f"🕒 เข้า {fmt(t['time'])} → ปิด {fmt(r['close_time'])} (เวลา MT5)"
    else:
        when = f"🕒 เข้า {fmt(t['time'])} → ปิด (ไม่ได้กรอก ExitTime)"
    lines = [
        f"{icon} <b>ปิดไม้เอง - {side_u} ({word})</b>",
        when,
        f"เข้า <code>{t['entry']:.2f}</code> | ปิด <code>{t['exit']:.2f}</code>",
        f"SL <code>{t['sl']:.2f}</code> | TP <code>{t['tp']:.2f}</code>",
        f"ผล <b>{r_part(r)}</b> ({r['pts']:+.2f} จุดราคา)",
    ]
    if t["exit_time"] is None:
        lines.append("💡 ใส่ ExitTime ในชีต เพื่อให้สถิติรายวันนับถูกวัน")
    return "\n".join(lines)


def day_bounds(ts):
    """ช่วงวัน (เวลา MT5) ที่ ts อยู่ คืน (เริ่ม, สิ้นสุด)"""
    h = pd.Timedelta(hours=DAY_START_HOUR)
    start = (ts - h).floor("D") + h
    return start, start + pd.Timedelta(days=1)


def day_label(start):
    """วันที่ที่แสดง = วันที่ของกึ่งกลางช่วงวัน (DAY_START_HOUR=17 -> แสดงเป็นวันที่ตามเวลาไทย)"""
    return (start + pd.Timedelta(hours=12)).strftime("%d/%m")


def tally_text(results, ts):
    """สถิติสะสมของวันที่ ts อยู่ (นับรวมไม้ที่เพิ่งปิด) เริ่ม 0 ใหม่ทุกวัน"""
    start, end = day_bounds(ts)
    day = [r for r in results if r["status"] == "closed" and start <= r["close_time"] < end]
    s = summarize(day)
    if not s["n"]:
        return ""
    return (f"\n\n📈 สถิติวันที่ {day_label(start)} (รวมไม้นี้): ชนะ {s['w']} | แพ้ {s['l']} | เสมอ {s['be']}\n"
            f"🎯 Win rate {s['wr']:.0f}% | {r_text(s)}\n"
            f"💵 สุทธิวันนี้: {pnl_label(s['sum_pts'])}")


def collect_results(trades, bars_needed_days=MONITOR_DAYS):
    """ประเมินไม้ทุกไม้ที่ตรวจได้ (ปิดมือ + ไม้ที่เปิดไม่เกิน MONITOR_DAYS วัน) คืน (รายการ (t, r), ดึงข้อมูลสำเร็จหรือไม่)"""
    now = now_server()
    live_ids = {t["id"] for t in trades
                if t["exit"] is None and timedelta(0) <= now - t["time"] <= timedelta(days=bars_needed_days)}
    bars = fetch_5m(bars_needed_days + 1) if live_ids else None
    ok = bars is not None or not live_ids
    out = []
    for t in trades:
        if t["exit"] is not None:
            out.append((t, evaluate_trade(t, None)))
        elif t["id"] in live_ids:
            out.append((t, evaluate_trade(t, bars)))
    return out, ok


LOSS_STREAK_ALERT = int(os.environ.get("LOSS_STREAK_ALERT", "3"))              # แพ้ติดกันกี่ไม้ถึงเตือน (0 = ปิด)
STREAK_ALERT_WINDOW_MIN = int(os.environ.get("STREAK_ALERT_WINDOW_MIN", "120"))  # เตือนเฉพาะถ้าไม้ล่าสุดปิดภายในกี่นาที


def check_loss_streak(results):
    """เตือนเมื่อแพ้ติดกันถึงเกณฑ์ (ไม้เสมอไม่ตัดและไม่นับ, ไม้ชนะตัดสถิติ) เตือนอีกเมื่อแพ้เพิ่ม"""
    if LOSS_STREAK_ALERT <= 0:
        return
    closed = sorted([r for r in merge_with_ledger(results) if r["status"] == "closed"],
                    key=lambda r: r["close_time"])
    streak = []
    for r in reversed(closed):
        if r["result"] == "loss":
            streak.append(r)
        elif r["result"] == "be":
            continue
        else:
            break
    n = len(streak)
    if n < LOSS_STREAK_ALERT:
        return
    last = streak[0]
    if now_server() - last["close_time"] > timedelta(minutes=STREAK_ALERT_WINDOW_MIN):
        return
    total = sum(r["pts"] for r in streak)
    fire("streak", f"{n}|{last['id']}",
         f"🚨 <b>แพ้ติดกัน {n} ไม้</b>\n"
         f"📉 รวม <b>{total:+.2f}</b> จุดราคา\n"
         f"🕒 ไม้ล่าสุดปิด {fmt(last['close_time'])} (เวลา MT5)\n"
         f"💡 พักสักครู่ ทบทวนแผน หรือลดขนาดไม้ก่อนเข้าไม้ถัดไป")


def notify_recovered(levels, cur_bad, cur_future):
    """แจ้งเมื่อแถวที่เคยเตือนว่ามีปัญหา ถูกแก้ไขแล้ว"""
    valid_rows = {t["row"] for t in levels.get("trades", [])}
    changed = False
    for key in [k for k in sent if k.startswith("bad:")]:
        row = key.split(":", 1)[1]
        if row in cur_bad:
            continue
        if row.isdigit() and int(row) in valid_rows:
            txt = f"✅ <b>ออเดอร์ในชีตแถว {row} แก้ไขแล้ว</b>\nบอทอ่านได้และนับเข้าสถิติแล้ว"
        else:
            txt = f"✅ <b>แถว {row} ในชีตไม่มีปัญหาแล้ว</b>\n(ถูกลบ/เคลียร์ บอทไม่นับแถวนี้)"
        if send_telegram_message(txt):
            sent.pop(key, None)
            changed = True
    for key in [k for k in sent if k.startswith("future:")]:
        if key in cur_future:
            continue
        row = str(sent.get(key, ""))
        where = f"แถว {row} " if row.isdigit() else ""
        if send_telegram_message(f"✅ <b>เวลาเข้าไม้ {where}แก้ไขแล้ว</b>\nไม่อยู่ในอนาคตแล้ว บอทเริ่มนับผลให้แล้ว"):
            sent.pop(key, None)
            changed = True
    if changed:
        save_state()


def monitor_trades(levels):
    """แจ้งผลไม้ที่เพิ่งปิด (TP/SL ชน หรือกรอก Exit) พร้อมสถิติสะสมของวันนั้น"""
    global _known_exits
    now = now_server()

    cur_bad, cur_future = {}, set()
    for row, reason in levels.get("problems", []):
        cur_bad[str(row)] = reason
        fire(f"bad:{row}", reason,
             f"⚠️ <b>ออเดอร์ในชีตแถว {row} ไม่ถูกนับ</b>\n{reason}\nแก้ในชีตแล้วบอทจะนับให้เอง")
    for t in levels.get("trades", []):
        if t["exit"] is None and t["time"] > now + timedelta(minutes=5):
            cur_future.add(f"future:{t['id']}")
            fire(f"future:{t['id']}", str(t["row"]),
                 f"⚠️ <b>ออเดอร์ในชีตแถว {t['row']}</b> มีเวลาเข้าไม้ {fmt(t['time'])} (เวลา MT5) อยู่ในอนาคต\n"
                 f"บอทจึงยังไม่นับผล เช็กวันที่/เวลาที่กรอก")
    if levels.get("trades_ok", True):   # อ่านชีตสำเร็จจริงเท่านั้นถึงประกาศว่าแก้แล้ว
        notify_recovered(levels, cur_bad, cur_future)

    pairs, _ = collect_results(levels.get("trades", []))
    all_r = [r for _, r in pairs]
    window = timedelta(minutes=MANUAL_ALERT_WINDOW_MIN)
    if _known_exits is None:  # เพิ่งสตาร์ทบอท: Exit เก่าที่มีอยู่แล้วถือว่าแจ้งไปแล้ว (กันส่งซ้ำของเก่า)
        _known_exits = {t["id"]: t["exit"] for t, r in pairs
                        if r["status"] == "closed" and r["how"] == "manual" and now - r["close_time"] > window}
    for t, r in pairs:
        if r["status"] != "closed":
            continue
        remember_closed(t, r)   # จดผลลงสมุดบันทึก ลบแถวในชีตทีหลังได้
        age = now - r["close_time"]
        key, val = f"trade:{t['id']}", None
        if r["how"] == "manual":
            is_new = _known_exits.get(t["id"]) != t["exit"]   # เพิ่งกรอก/แก้ Exit (ไม่ขึ้นกับ ExitTime)
            if not is_new and age > window:
                continue
            msg, val = manual_alert_msg(t, r), f"manual:{t['exit']:g}"
        else:
            if age > timedelta(minutes=TRADE_ALERT_WINDOW_MIN):
                continue
            msg, val = trade_alert_msg(t, r), r["how"]
        fire(key, val, msg + tally_text(all_r, r["close_time"]))
        if r["how"] == "manual" and sent.get(key) == val:
            _known_exits[t["id"]] = t["exit"]

    try:
        check_loss_streak(all_r)
    except Exception as e:
        print(f"Loss streak error: {e}")

    try:
        backfill_ledger(levels.get("trades", []))
    except Exception as e:
        print(f"Backfill error: {e}")


def build_daily_msg(day_start, day_res, open_n, skipped=0):
    s = summarize(day_res)
    lines = [
        f"📅 <b>สรุปประจำวัน {day_label(day_start)}</b>",
        "",
        f"📌 ไม้ที่ปิดทั้งวัน: <b>{s['n']}</b> ไม้",
        f"✅ ชนะ {s['w']} | ❌ แพ้ {s['l']} | ➖ เสมอ {s['be']}",
        f"🎯 Win rate: <b>{s['wr']:.0f}%</b>",
        f"📐 RR ที่วางไว้เฉลี่ย: 1:{s['planned']:.2f}" if s["n_pl"] else "📐 RR ที่วางไว้เฉลี่ย: -",
        f"💰 {r_text(s)}",
        f"💵 สุทธิทั้งวัน: <b>{pnl_label(s['sum_pts'])}</b> (เฉลี่ย {s['avg_pts']:+.2f}/ไม้)",
    ]
    lines += risk_lines(s)
    if open_n:
        lines.append(f"⏳ ไม้ที่ยังเปิดอยู่: {open_n}")
    if skipped:
        lines.append(f"⚠️ มี {skipped} แถวในชีตไม่ถูกนับ (ข้อมูลไม่ถูกต้อง)")
    lines += ["", "🔄 เริ่มนับสถิติวันใหม่จาก 0"]
    return "\n".join(lines)


def maybe_send_daily(now=None, df=None):
    """ขึ้นวันใหม่ (ตามเวลา MT5): สรุปไม้ของวันที่ผ่านมา แล้วสถิติเรียลไทม์เริ่มนับ 0 ใหม่"""
    now = now or now_server()
    today_start, _ = day_bounds(now)
    if now - today_start >= pd.Timedelta(hours=DAILY_WINDOW_HOURS):
        return
    yday_start = today_start - pd.Timedelta(days=1)
    day_id = yday_start.strftime("%Y-%m-%d")
    if sent.get("daily") == day_id:
        return

    if df is None:
        df = fetch_sheet_df()
    pairs, ok = collect_results(parse_trades(df))
    skipped = len(parse_trades.last_problems)
    if not ok:
        print("ดึงข้อมูล 5 นาทีไม่ได้ ลองสรุปประจำวันใหม่รอบหน้า")
        return
    day_res = [r for _, r in pairs if r["status"] == "closed" and yday_start <= r["close_time"] < today_start]
    open_n = sum(1 for _, r in pairs if r["status"] == "open")
    if day_res and not send_telegram_message(build_daily_msg(yday_start, day_res, open_n, skipped)):
        return
    sent["daily"] = day_id
    save_state()
    if day_res:
        print(f"✅ ส่งสรุปประจำวัน {day_id} แล้ว")


def week_close_utc(now_utc):
    """เวลาปิดตลาดล่าสุด (ศุกร์ 17:00 เวลานิวยอร์ก) เป็น UTC แบบไม่มี tz"""
    ny_now = now_utc.astimezone(ZoneInfo(NY_TZ))
    fri = (ny_now - timedelta(days=(ny_now.weekday() - 4) % 7)).replace(hour=17, minute=0, second=0, microsecond=0)
    if fri > ny_now:
        fri -= timedelta(days=7)
    return fri.astimezone(timezone.utc).replace(tzinfo=None)


def maybe_send_weekly(now_utc=None, df=None):
    """ตลาดปิดสัปดาห์ (ศุกร์ 17:00 นิวยอร์ก): สรุปไม้ทั้งสัปดาห์ ส่งภายใน WEEKLY_WINDOW_HOURS ชม.แรก"""
    global _weekly_force_done
    now_utc = now_utc or datetime.now(timezone.utc)
    close_utc = week_close_utc(now_utc)
    since = now_utc.replace(tzinfo=None) - close_utc
    in_window = timedelta(0) <= since < timedelta(hours=WEEKLY_WINDOW_HOURS)
    force = WEEKLY_FORCE and not _weekly_force_done
    if not in_window and not force:
        return

    if in_window:
        end_utc = close_utc
        week_id = close_utc.strftime("%Y-%m-%d")
        if sent.get("weekly") == week_id:
            return
    else:
        end_utc = now_utc.replace(tzinfo=None)
        week_id = "force"
    start_utc = end_utc - timedelta(days=7)

    if df is None:
        df = fetch_sheet_df()
    trades = parse_trades(df)

    def mark_done():
        global _weekly_force_done
        if in_window:
            sent["weekly"] = week_id
            save_state()
        else:
            _weekly_force_done = True

    if not trades and not sent.get("ledger"):
        print("ไม่มีออเดอร์ในชีต ข้ามสรุปรายสัปดาห์")
        mark_done()
        return

    replay = [t for t in trades if t["exit"] is None]
    bars = None
    if replay:
        days = min((now_server() - min(t["time"] for t in replay)).days + 2, 59)
        bars = fetch_5m(days)
        if bars is None:
            print("ดึงข้อมูล 5 นาทีไม่ได้ ลองใหม่รอบหน้า")
            return

    results = merge_with_ledger([evaluate_trade(t, bars) for t in trades])
    start_s, end_s = utc_to_server(start_utc), utc_to_server(end_utc)
    week_res = [r for r in results if r["status"] == "closed" and start_s < r["close_time"] <= end_s]
    prev_start_s = utc_to_server(start_utc - timedelta(days=7))
    prev_res = [r for r in results if r["status"] == "closed" and prev_start_s < r["close_time"] <= start_s]
    if send_telegram_message(build_weekly_msg(start_s, end_s, week_res, results,
                                              skipped=len(parse_trades.last_problems), prev_res=prev_res)):
        mark_done()
        print("✅ ส่งสรุปรายสัปดาห์แล้ว")

# ---------- สรุปรายเดือน ----------
MONTHLY_DAY = int(os.environ.get("MONTHLY_DAY", "25"))
MONTHLY_HOUR_TH = int(os.environ.get("MONTHLY_HOUR_TH", "5"))            # เวลาไทย
MONTHLY_WINDOW_HOURS = int(os.environ.get("MONTHLY_WINDOW_HOURS", "24"))  # บอทดับ ส่งย้อนหลังได้ภายในกี่ชม.
MONTHLY_START_TH = os.environ.get("MONTHLY_START_TH", "2026-10-20 00:00")      # จุดเริ่มรอบแรก (เวลาไทย)
MONTHLY_FIRST_END_TH = os.environ.get("MONTHLY_FIRST_END_TH", "2026-11-25 05:00")  # รายงานฉบับแรกส่งเมื่อไหร่ (เวลาไทย)
MONTHLY_FORCE = os.environ.get("MONTHLY_FORCE", "0") == "1"              # ทดสอบ: ส่งทันที 1 ครั้ง
_monthly_force_done = False


def monthly_bounds(now_th):
    """ช่วงสรุป (เวลาไทย): 25 เดือนก่อน 05:00 -> 25 เดือนนี้ 05:00
    รอบแรก (end == MONTHLY_FIRST_END_TH) เริ่มนับที่ MONTHLY_START_TH / ก่อนรอบแรกคืน (None, None)"""
    end = pd.Timestamp(year=now_th.year, month=now_th.month, day=MONTHLY_DAY, hour=MONTHLY_HOUR_TH)
    if now_th < end:
        end = end - pd.DateOffset(months=1)
    start = end - pd.DateOffset(months=1)
    first_end = pd.Timestamp(MONTHLY_FIRST_END_TH)
    if end < first_end:
        return None, None
    if end == first_end:
        start = pd.Timestamp(MONTHLY_START_TH)
    return start, end


def build_monthly_msg(start_th, end_th, start_s, end_s, month_res, all_res, skipped=0, prev_res=None):
    s = summarize(month_res)
    lines = ["🗓 <b>สรุปผลเทรดประจำเดือน</b>",
             f"📆 {start_th:%d/%m/%Y %H:%M} → {end_th:%d/%m/%Y %H:%M} (เวลาไทย)", ""]
    if not s["n"]:
        lines.append("📌 ช่วงนี้ไม่มีไม้ที่ปิด")
    else:
        lines += [
            f"📌 ไม้ที่ปิดทั้งหมด: <b>{s['n']}</b> ไม้",
            f"✅ ชนะ {s['w']} | ❌ แพ้ {s['l']} | ➖ เสมอ {s['be']}",
            f"🎯 Win rate: <b>{s['wr']:.0f}%</b>",
            f"📐 RR ที่วางไว้เฉลี่ย: 1:{s['planned']:.2f}" if s["n_pl"] else "📐 RR ที่วางไว้เฉลี่ย: -",
            f"💰 {r_text(s)}",
            f"💵 สุทธิ: <b>{pnl_label(s['sum_pts'])}</b> (เฉลี่ย {s['avg_pts']:+.2f}/ไม้)",
        ]
        lines += risk_lines(s)
        lines.append(f"🔁 Recovery Factor: <b>{rf_text(s)}</b> (กำไรสุทธิ ÷ Max Drawdown)")
        lines.append("")
        for side, name in (("buy", "BUY"), ("sell", "SELL")):
            x = summarize([r for r in month_res if r["side"] == side])
            if x["n"]:
                lines.append(f"{name}: {x['n']} ไม้ | ชนะ {x['w']} แพ้ {x['l']} เสมอ {x['be']} "
                             f"| WR {x['wr']:.0f}% | {x['sum_pts']:+.2f} จุด")
        lines += ["", "📅 แยกรายสัปดาห์ (เวลา MT5)"]
        b0, i = start_s, 1
        while b0 < end_s:
            b1 = min(b0 + pd.Timedelta(days=7), end_s)
            x = summarize([r for r in month_res if b0 < r["close_time"] <= b1])
            lines.append(f"• สัปดาห์ {i} ({b0:%d/%m}–{b1:%d/%m}): {x['n']} ไม้ | "
                         f"ชนะ {x['w']} แพ้ {x['l']} | {x['sum_pts']:+.2f} จุด")
            b0, i = b1, i + 1
        best = max(month_res, key=lambda r: r["pts"])
        worst = min(month_res, key=lambda r: r["pts"])
        cur = mx = 0
        for r in sorted(month_res, key=lambda r: r["close_time"]):
            cur = cur + 1 if r["result"] == "loss" else 0
            mx = max(mx, cur)
        lines += [
            "",
            f"🏆 ไม้ดีที่สุด: {best['side'].upper()} เข้า {fmt(best['time'])} → {best['pts']:+.2f} จุด",
            f"💥 ไม้แย่ที่สุด: {worst['side'].upper()} เข้า {fmt(worst['time'])} → {worst['pts']:+.2f} จุด",
            f"📉 แพ้ติดต่อกันสูงสุด: {mx} ไม้",
        ]
    open_n = sum(1 for r in all_res if r["status"] == "open")
    unknown_n = sum(1 for r in all_res if r["status"] == "unknown")
    cmp_txt = compare_text(s, summarize(prev_res or []), "เดือน")
    if cmp_txt:
        lines += ["", cmp_txt]
    if open_n:
        lines.append(f"⏳ ไม้ที่ยังเปิดอยู่: {open_n}")
    if unknown_n:
        lines.append(f"❓ ตรวจผลไม่ได้ {unknown_n} ไม้ (เก่าเกินข้อมูลและไม่มีในสมุดบันทึก ใส่ Exit ในชีตได้)")
    if skipped:
        lines.append(f"⚠️ มี {skipped} แถวในชีตไม่ถูกนับ (ข้อมูลไม่ถูกต้อง)")
    lines += journal_lines(start_s, end_s)
    return "\n".join(lines)


def maybe_send_monthly(df=None):
    global _monthly_force_done
    now_th = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=7))
    start_th, end_th = monthly_bounds(now_th)
    in_window = (end_th is not None
                 and pd.Timedelta(0) <= now_th - end_th < pd.Timedelta(hours=MONTHLY_WINDOW_HOURS))
    force = MONTHLY_FORCE and not _monthly_force_done
    if not in_window and not force:
        return
    if in_window:
        month_id = end_th.strftime("%Y-%m-%d")
        if sent.get("monthly") == month_id:
            return
    else:  # โหมดทดสอบ: นับตั้งแต่จุดเริ่มรอบแรก (หรือต้นรอบปัจจุบัน) ถึงตอนนี้
        start_th = start_th if start_th is not None else pd.Timestamp(MONTHLY_START_TH)
        end_th = now_th
        month_id = "force"

    if df is None:
        df = fetch_sheet_df()
    trades = parse_trades(df)
    skipped = len(parse_trades.last_problems)

    def mark_done():
        global _monthly_force_done
        if in_window:
            sent["monthly"] = month_id
            save_state()
        else:
            _monthly_force_done = True

    if not trades and not sent.get("ledger"):
        print("ไม่มีออเดอร์ในชีตและสมุดบันทึก ข้ามสรุปรายเดือน")
        mark_done()
        return

    bars = None
    replay = [t for t in trades if t["exit"] is None]
    if replay:
        days = min((now_server() - min(t["time"] for t in replay)).days + 2, 59)   # Yahoo ให้ 5 นาทีได้ไม่เกิน 60 วัน
        bars = fetch_5m(days)
        if bars is None:
            print("ดึงข้อมูล 5 นาทีไม่ได้ ลองสรุปรายเดือนใหม่รอบหน้า")
            return

    results = merge_with_ledger([evaluate_trade(t, bars) for t in trades])

    start_s = utc_to_server(start_th - pd.Timedelta(hours=7))
    end_s = utc_to_server(end_th - pd.Timedelta(hours=7))
    month_res = [r for r in results if r["status"] == "closed" and start_s < r["close_time"] <= end_s]
    prev_start_th = start_th - pd.DateOffset(months=1)
    prev_start_s = utc_to_server(prev_start_th - pd.Timedelta(hours=7))
    prev_res = [r for r in results if r["status"] == "closed" and prev_start_s < r["close_time"] <= start_s]
    if send_telegram_message(build_monthly_msg(start_th, end_th, start_s, end_s, month_res, results,
                                               skipped, prev_res=prev_res)):
        mark_done()
        print("✅ ส่งสรุปรายเดือนแล้ว")

# ---------- สรุปรายปี (1 ม.ค. 06:00 เวลาไทย) + เคลียร์ข้อมูลปีที่สรุปแล้วใน Upstash ----------
YEARLY_HOUR_TH = int(os.environ.get("YEARLY_HOUR_TH", "6"))                 # เวลาไทย
YEARLY_WINDOW_HOURS = int(os.environ.get("YEARLY_WINDOW_HOURS", "24"))      # บอทดับ ส่งย้อนหลังได้ภายในกี่ชม.
YEARLY_FORCE = os.environ.get("YEARLY_FORCE", "0") == "1"                   # ทดสอบ: ส่งทันที 1 ครั้ง (ไม่ลบข้อมูล)
_yearly_force_done = False


def build_yearly_msg(label, start_th, end_th, year_res, all_res, skipped=0):
    s = summarize(year_res)
    lines = [f"🎊 <b>สรุปผลเทรดประจำปี {label}</b>",
             f"📆 {start_th:%d/%m/%Y} → {end_th:%d/%m/%Y %H:%M} (เวลาไทย)", ""]
    if not s["n"]:
        lines.append("📌 ปีนี้ไม่มีไม้ที่ปิด")
    else:
        lines += [
            f"📌 ไม้ที่ปิดทั้งปี: <b>{s['n']}</b> ไม้",
            f"✅ ชนะ {s['w']} | ❌ แพ้ {s['l']} | ➖ เสมอ {s['be']}",
            f"🎯 Win rate: <b>{s['wr']:.0f}%</b>",
            f"📐 RR ที่วางไว้เฉลี่ย: 1:{s['planned']:.2f}" if s["n_pl"] else "📐 RR ที่วางไว้เฉลี่ย: -",
            f"💰 {r_text(s)}",
            f"💵 สุทธิทั้งปี: <b>{pnl_label(s['sum_pts'])}</b> (เฉลี่ย {s['avg_pts']:+.2f}/ไม้)",
        ]
        lines += risk_lines(s)
        lines.append(f"🔁 Recovery Factor: <b>{rf_text(s)}</b> (กำไรสุทธิ ÷ Max Drawdown)")
        lines.append("")
        for side, name in (("buy", "BUY"), ("sell", "SELL")):
            x = summarize([r for r in year_res if r["side"] == side])
            if x["n"]:
                lines.append(f"{name}: {x['n']} ไม้ | ชนะ {x['w']} แพ้ {x['l']} เสมอ {x['be']} "
                             f"| WR {x['wr']:.0f}% | {x['sum_pts']:+.2f} จุด")
        lines += ["", "📅 แยกรายเดือน"]
        m0 = pd.Timestamp(year=start_th.year, month=start_th.month, day=1)
        month_stats = []
        while m0 < end_th:
            m1 = min(m0 + pd.DateOffset(months=1), end_th)
            bs = utc_to_server(m0 - pd.Timedelta(hours=7))
            be = utc_to_server(m1 - pd.Timedelta(hours=7))
            x = summarize([r for r in year_res if bs < r["close_time"] <= be])
            if x["n"]:   # ข้ามเดือนที่ไม่มีไม้ (ปีแรกเริ่มเทรดกลางปี)
                lines.append(f"• {m0:%m/%Y}: {x['n']} ไม้ | ชนะ {x['w']} แพ้ {x['l']} | {x['sum_pts']:+.2f} จุด")
                month_stats.append((m0, x["sum_pts"]))
            m0 = m0 + pd.DateOffset(months=1)
        if month_stats:
            bm = max(month_stats, key=lambda m: m[1])
            wm = min(month_stats, key=lambda m: m[1])
            lines += ["", f"🥇 เดือนที่ดีที่สุด: {bm[0]:%m/%Y} ({bm[1]:+.2f} จุด)",
                      f"🥀 เดือนที่แย่ที่สุด: {wm[0]:%m/%Y} ({wm[1]:+.2f} จุด)"]
        best = max(year_res, key=lambda r: r["pts"])
        worst = min(year_res, key=lambda r: r["pts"])
        cur = mx = 0
        for r in sorted(year_res, key=lambda r: r["close_time"]):
            cur = cur + 1 if r["result"] == "loss" else 0
            mx = max(mx, cur)
        lines += [
            "",
            f"🏆 ไม้ดีที่สุด: {best['side'].upper()} เข้า {fmt(best['time'])} → {best['pts']:+.2f} จุด",
            f"💥 ไม้แย่ที่สุด: {worst['side'].upper()} เข้า {fmt(worst['time'])} → {worst['pts']:+.2f} จุด",
            f"📉 แพ้ติดต่อกันสูงสุด: {mx} ไม้",
        ]
    open_n = sum(1 for r in all_res if r["status"] == "open")
    if open_n:
        lines.append(f"⏳ ไม้ที่ยังเปิดอยู่: {open_n}")
    if skipped:
        lines.append(f"⚠️ มี {skipped} แถวในชีตไม่ถูกนับ (ข้อมูลไม่ถูกต้อง)")
    lines += journal_lines(utc_to_server(start_th - pd.Timedelta(hours=7)),
                           utc_to_server(end_th - pd.Timedelta(hours=7)))
    return "\n".join(lines)


def purge_after_yearly(end_s):
    """เคลียร์สมุดบันทึกผลไม้ที่สรุปแล้ว (ปิดก่อน end_s) + คีย์เก่าที่ไม่ใช้แล้ว แล้วเขียนสถานะที่เล็กลงทับใน Upstash
    คงสถานะที่ยังต้องใช้ไว้ (ตัวกันส่งซ้ำของสรุปรายวัน/สัปดาห์/เดือน, ไม้ใหม่หลังสิ้นปี)"""
    led = sent.get("ledger", {})
    old = [k for k, v in led.items() if pd.Timestamp(v["ct"]) <= end_s]
    for k in old:
        led.pop(k, None)
        sent.pop(f"trade:{k}", None)
    jr = sent.get("journal", {})
    for k in [k for k, v in jr.items() if pd.Timestamp(v["t"]) <= end_s]:
        jr.pop(k, None)   # เคลียร์สมุดบันทึกสัญญาณของปีที่สรุปแล้ว
    cutoff = now_server() - pd.Timedelta(days=7)
    for k in [k for k in sent if ":cd_" in k]:
        try:
            if pd.Timestamp(sent[k]) < cutoff:
                sent.pop(k, None)
        except Exception:
            sent.pop(k, None)
    pend = sent.get("pending_remove", {})
    for k in [k for k, e in pend.items() if e.get("pushed")]:
        pend.pop(k, None)
    save_state()
    return len(old)


def maybe_send_yearly(df=None):
    global _yearly_force_done
    now_th = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=7))
    send_at = pd.Timestamp(year=now_th.year, month=1, day=1, hour=YEARLY_HOUR_TH)
    in_window = pd.Timedelta(0) <= now_th - send_at < pd.Timedelta(hours=YEARLY_WINDOW_HOURS)
    force = YEARLY_FORCE and not _yearly_force_done
    if not in_window and not force:
        return
    if in_window:
        start_th = pd.Timestamp(year=now_th.year - 1, month=1, day=1)
        end_th = pd.Timestamp(year=now_th.year, month=1, day=1)
        year_id = str(now_th.year - 1)
        label = year_id
        if sent.get("yearly") == year_id:
            return
    else:  # โหมดทดสอบ: ตั้งแต่ 1 ม.ค. ปีนี้ถึงตอนนี้ และไม่ลบข้อมูลใดๆ
        start_th = pd.Timestamp(year=now_th.year, month=1, day=1)
        end_th = now_th
        year_id = "force"
        label = f"{now_th.year} (ทดสอบ)"

    if df is None:
        df = fetch_sheet_df()
    trades = parse_trades(df)
    skipped = len(parse_trades.last_problems)

    bars = None
    replay = [t for t in trades if t["exit"] is None]
    if replay:
        days = min((now_server() - min(t["time"] for t in replay)).days + 2, 59)
        bars = fetch_5m(days)
        if bars is None:
            print("ดึงข้อมูล 5 นาทีไม่ได้ ลองสรุปรายปีใหม่รอบหน้า")
            return

    results = merge_with_ledger([evaluate_trade(t, bars) for t in trades])
    start_s = utc_to_server(start_th - pd.Timedelta(hours=7))
    end_s = utc_to_server(end_th - pd.Timedelta(hours=7))
    year_res = [r for r in results if r["status"] == "closed" and start_s < r["close_time"] <= end_s]

    if not year_res and not trades and not sent.get("ledger"):
        print("ไม่มีข้อมูลสำหรับสรุปรายปี ข้าม")
        if in_window:
            sent["yearly"] = year_id
            save_state()
        else:
            _yearly_force_done = True
        return

    if not send_telegram_message(build_yearly_msg(label, start_th, end_th, year_res, results, skipped)):
        return
    print("✅ ส่งสรุปรายปีแล้ว")
    if not in_window:
        _yearly_force_done = True
        return
    sent["yearly"] = year_id
    save_state()   # จดก่อนว่าส่งแล้ว กันส่งซ้ำถ้าขั้นเคลียร์พลาด
    n = purge_after_yearly(end_s)
    send_telegram_message(f"🧹 <b>เคลียร์ข้อมูลปี {label} ใน Upstash แล้ว</b>\n"
                          f"ลบผลไม้ที่สรุปแล้ว {n} ไม้ และคีย์เก่าที่ไม่ใช้แล้ว เริ่มเก็บสถิติปีใหม่จาก 0")

# ==========================================
# 🕓 แจ้งเตือน 4H ปิดแท่ง (ส่งข้อมูลแท่งนั้นเข้า Telegram)
# ==========================================
H4_CLOSE_ALERT = os.environ.get("H4_CLOSE_ALERT", "1") == "1"
H4_CLOSE_WINDOW_MIN = int(os.environ.get("H4_CLOSE_WINDOW_MIN", "60"))  # แจ้งเฉพาะแท่งที่ปิดไม่เกินกี่นาที
PIN_WICK_RATIO = float(os.environ.get("PIN_WICK_RATIO", "2"))      # Pinbar: ไส้หลักต้องยาว >= กี่เท่าของตัวแท่ง
PIN_WICK_PCT = float(os.environ.get("PIN_WICK_PCT", "0.6"))        # Pinbar: ไส้หลักต้องยาว >= กี่ส่วนของช่วงแท่ง
PIN_OPP_PCT = float(os.environ.get("PIN_OPP_PCT", "0.25"))         # Pinbar: ไส้ฝั่งตรงข้ามต้องสั้น <= กี่ส่วนของช่วงแท่ง
DOJI_BODY_PCT = float(os.environ.get("DOJI_BODY_PCT", "0.10"))     # Doji: ตัวแท่ง <= กี่ส่วนของช่วงแท่ง
MARU_BODY_PCT = float(os.environ.get("MARU_BODY_PCT", "0.85"))     # Marubozu: ตัวแท่ง >= กี่ส่วนของช่วงแท่ง


def candle_patterns(prev, cur, k=None):
    """จำแนกลักษณะแท่ง คืนรายการข้อความ (prev/cur = (O, H, L, C), k = Stoch %K ของแท่งนี้)"""
    po, ph, pl, pc = prev
    o, h, l, c = cur
    rng = h - l
    if rng <= 0:
        return ["แท่งไม่มีช่วงราคา"]
    body = abs(c - o)
    body_s = max(body, 0.01)
    upper = h - max(o, c)
    lower = min(o, c) - l
    has_k = k is not None and not pd.isna(k)
    pats = []

    # Pinbar (ไส้ยาวปฏิเสธราคา)
    bull_pin = lower >= PIN_WICK_RATIO * body_s and lower >= PIN_WICK_PCT * rng and upper <= PIN_OPP_PCT * rng
    bear_pin = upper >= PIN_WICK_RATIO * body_s and upper >= PIN_WICK_PCT * rng and lower <= PIN_OPP_PCT * rng
    if bull_pin:
        t = f"📌 Pinbar ขาขึ้น (Hammer) ไส้ล่างยาว {lower / rng:.0%} ของแท่ง"
        if has_k and k <= 20:
            t += " + ตรงโซน Oversold"
        pats.append(t)
    elif bear_pin:
        t = f"📌 Pinbar ขาลง (Shooting Star) ไส้บนยาว {upper / rng:.0%} ของแท่ง"
        if has_k and k >= 80:
            t += " + ตรงโซน Overbought"
        pats.append(t)

    # Doji / Marubozu / Spinning top
    if body <= DOJI_BODY_PCT * rng:
        if not (bull_pin or bear_pin):
            pats.append("➖ Doji (ตลาดลังเล)")
    elif body >= MARU_BODY_PCT * rng:
        pats.append("🟩 Marubozu ขาขึ้น (ซื้อแรงตลอดแท่ง)" if c > o else "🟥 Marubozu ขาลง (ขายแรงตลอดแท่ง)")
    elif body <= 0.3 * rng and upper >= 0.25 * rng and lower >= 0.25 * rng:
        pats.append("🌀 Spinning Top (แรงซื้อขายสูสี)")

    # Engulfing
    engulf = False
    if pc < po and c > o and c >= po and o <= pc + ENGULF_TOL:
        pats.append("🟢 Bullish Engulfing (กลืนกินขาขึ้น)")
        engulf = True
    elif pc > po and c < o and c <= po and o >= pc - ENGULF_TOL:
        pats.append("🔴 Bearish Engulfing (กลืนกินขาลง)")
        engulf = True

    # Inside / Outside bar
    if h <= ph and l >= pl:
        pats.append("📦 Inside Bar (ราคาพักตัวในกรอบแท่งก่อนหน้า)")
    elif h > ph and l < pl and not engulf:
        pats.append("↕️ Outside Bar (ทำ High/Low ใหม่ทั้งสองฝั่ง)")

    return pats or ["แท่งทั่วไป (ไม่เข้ารูปแบบเด่น)"]


def maybe_send_h4_close(df_1h, df_4h):
    """4H ปิดแท่ง: ส่งข้อมูลแท่งนั้นเข้า Telegram (ครั้งเดียวต่อแท่ง)"""
    if not H4_CLOSE_ALERT or len(df_4h) < 25:
        return
    delta = pd.Timedelta(hours=4)
    now = now_server()
    last_1h_start = df_1h.index[-1]

    closed_i = None
    for i in (-1, -2):
        end = df_4h.index[i] + delta
        if end > now:
            continue  # ยังไม่ปิด
        if i == -1 and last_1h_start < end - pd.Timedelta(hours=1):
            continue  # ข้อมูล 1H ชั่วโมงสุดท้ายยังมาไม่ครบ
        closed_i = i
        break
    if closed_i is None:
        return

    start = df_4h.index[closed_i]
    end = start + delta
    if now - end > pd.Timedelta(minutes=H4_CLOSE_WINDOW_MIN):
        return

    stoch_k = calculate_stoch(df_4h)
    atr = calculate_atr(df_4h)
    o, h, l, c = ohlc(df_4h, closed_i)
    k = float(stoch_k.iloc[closed_i])
    k_prev = float(stoch_k.iloc[closed_i - 1])
    a = float(atr.iloc[closed_i])

    if c > o:
        candle = "🟢 แท่งเขียว (ขึ้น)"
    elif c < o:
        candle = "🔴 แท่งแดง (ลง)"
    else:
        candle = "⚪ โดจิ"
    body = abs(c - o)
    rng = h - l
    upper = h - max(o, c)
    lower = min(o, c) - l

    k_txt = "-" if pd.isna(k) else f"{k:.1f}"
    kp_txt = "-" if pd.isna(k_prev) else f"{k_prev:.1f}"
    if pd.isna(k):
        zone = ""
    elif k >= 80:
        zone = "🔥 Overbought (≥80)"
    elif k <= 20:
        zone = "🧊 Oversold (≤20)"
    else:
        zone = "↔️ อยู่ระหว่างโซน"

    pats = candle_patterns(ohlc(df_4h, closed_i - 1), (o, h, l, c), k)

    lines = [
        "🕓 <b>4H ปิดแท่งแล้ว</b>",
        f"📊 XAUUSD ({SYMBOL}) | แท่ง {fmt(start)} → {fmt(end)} (เวลา MT5)",
        candle,
        f"O <code>{px(o):.2f}</code> | H <code>{px(h):.2f}</code>",
        f"L <code>{px(l):.2f}</code> | C <code>{px(c):.2f}</code>",
        f"📏 ช่วงแท่ง {rng:.2f} | ตัว {body:.2f} | ไส้บน {upper:.2f} | ไส้ล่าง {lower:.2f}",
        "🔎 <b>ลักษณะแท่ง</b>",
        *pats,
        f"📉 Stoch %K: <code>{kp_txt}</code> ➡️ <code>{k_txt}</code> {zone}",
    ]
    if not pd.isna(a):
        lines.append(f"🌊 ATR({ATR_PERIOD}): <code>{a:.2f}</code>")
    if PRICE_OFFSET:
        lines.append(f"ℹ️ ราคาแสดงเป็นระดับ MT5 (GC=F − {PRICE_OFFSET:g})")
    lines.append(data_source_note())

    fire("4h:bar_close", str(start), "\n".join(x for x in lines if x))

# ==========================================
# 💥 ปิดแท่งเบรกแนวต้าน/แนวรับ (H1, H4) + ลบแนวที่เบรกออกจากชีตอัตโนมัติ
# เบรกขึ้น: ปิดแท่งเหนือแนวต้าน (แท่งก่อนหน้าปิดใต้แนว) / เบรกลง: ปิดแท่งใต้แนวรับ (แท่งก่อนหน้าปิดเหนือแนว)
# ไส้ทะลุแนวแต่ปิดกลับ = ยังไม่นับเบรก ไม่แจ้ง ไม่ลบแนว
# แนวที่เบรกแล้วเลิกใช้ทันที (ไม่ใช้เป็นแนวรับ/ต้านเก่า) และสั่งลบออกจากชีตผ่าน Google Apps Script (SHEET_WEBHOOK_URL)
# ลบเฉพาะคอลัมน์ของ TF นั้น (เช่น เบรกต้าน H1 ลบจาก 1H_Res)
# ==========================================
BREAK_ALERT = os.environ.get("BREAK_ALERT", "1") == "1"
BREAK_AUTO_REMOVE = os.environ.get("BREAK_AUTO_REMOVE", "1") == "1"
BREAK_WINDOW_MIN = int(os.environ.get("BREAK_WINDOW_MIN", "90"))   # ตรวจแท่งที่ปิดไม่เกินกี่นาที (กันส่งซ้ำตอนรีสตาร์ท)
BREAK_CLOSE_BUFFER = float(os.environ.get("BREAK_CLOSE_BUFFER", "0"))   # ราคาปิดต้องเลยแนวอย่างน้อยกี่จุด (0 = เลยแนวนิดเดียวก็นับ)
SHEET_WEBHOOK_URL = os.environ.get("SHEET_WEBHOOK_URL", "").strip().strip("\"'“”‘’ ")
SHEET_WEBHOOK_SECRET = os.environ.get("SHEET_WEBHOOK_SECRET", "").strip().strip("\"'“”‘’ ")


def sheet_col(tf, kind):
    return f"{tf.upper()}_{'Res' if kind == 'res' else 'Sup'}"


def register_removal(tf, kind, raw):
    """จดแนวที่เบรกแล้ว (ค่าตามชีต) รอลบออกจากชีต บอทเลิกใช้แนวนี้ทันที"""
    pend = sent.setdefault("pending_remove", {})
    key = f"{sheet_col(tf, kind)}:{raw:.2f}"
    if key in pend:
        return
    pend[key] = {"tf": tf, "kind": kind, "val": raw, "t": str(now_server())}
    save_state()


def apply_removed(levels):
    """ตัดแนวที่เบรกแล้วออกจากรายการที่บอทใช้ / เลิกจำเมื่อค่านั้นหายจากชีตแล้ว (ลบสำเร็จหรือลบเอง)"""
    pend = sent.get("pending_remove")
    if not pend:
        return
    changed = False
    for key, e in list(pend.items()):
        if not any(abs(v - e["val"]) < 0.005 for v in levels[e["tf"]][e["kind"]]):
            pend.pop(key, None)
            changed = True
    for tf in ("1h", "4h"):
        for kind in ("res", "sup"):
            drop = [e["val"] for e in pend.values() if e["tf"] == tf and e["kind"] == kind]
            if drop:
                levels[tf][kind] = [v for v in levels[tf][kind] if not any(abs(v - d) < 0.005 for d in drop)]
    if changed:
        save_state()


def push_removals():
    """สั่งลบแนวที่เบรกแล้วออกจากชีตผ่าน Apps Script (ลองใหม่ทุกรอบถ้ายังไม่สำเร็จ)"""
    pend = sent.get("pending_remove")
    if not pend or not SHEET_WEBHOOK_URL:
        return
    changed = False
    for e in pend.values():
        if e.get("pushed"):
            continue
        col = sheet_col(e["tf"], e["kind"])
        try:
            r = requests.post(SHEET_WEBHOOK_URL,
                              json={"secret": SHEET_WEBHOOK_SECRET, "column": col, "value": e["val"]},
                              timeout=20)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise ValueError(data.get("error", "unknown"))
            e["pushed"] = True
            changed = True
            print(f"🧹 ลบแนว {e['val']:.2f} ออกจาก {col} แล้ว (removed={data.get('removed')})")
        except Exception as ex:
            e["fails"] = e.get("fails", 0) + 1
            changed = True
            print(f"⚠️ ลบแนว {e['val']:.2f} ออกจาก {col} ไม่สำเร็จ ({e['fails']}): {ex}")
            if e["fails"] == 3:
                send_telegram_message(
                    f"⚠️ <b>ลบแนว {e['val']:.2f} ออกจากชีต ({col}) ไม่สำเร็จ</b>\n"
                    f"🔎 {html.escape(str(ex))}\n"
                    f"บอทเลิกใช้แนวนี้แล้ว แต่ต้องลบในชีตเอง หรือตรวจ SHEET_WEBHOOK_URL / SECRET / การ Deploy Apps Script"
                )
    if changed:
        save_state()


def closed_bar_index(tf, df, last_1h_start):
    """ตำแหน่งแท่งที่ปิดแล้วล่าสุด (ตรรกะเดียวกับ check_timeframe) คืน None ถ้าข้อมูลยังไม่พร้อม"""
    delta = pd.Timedelta(hours=1 if tf == "1h" else 4)
    last_end = df.index[-1] + delta
    is_live = last_end + pd.Timedelta(seconds=CLOSE_GRACE_SEC) > now_server()
    if not is_live and tf == "4h" and last_1h_start < last_end - pd.Timedelta(hours=1):
        return None
    return -2 if is_live else -1


def check_breaks(tf, df, levels, last_1h_start):
    if not (BREAK_ALERT or BREAK_AUTO_REMOVE) or len(df) < 25:
        return
    cl_i = closed_bar_index(tf, df, last_1h_start)
    if cl_i is None:
        print(f"[{tf.upper()}] เช็กเบรก: ข้ามรอบนี้ (ข้อมูล 1H ชั่วโมงสุดท้ายยังมาไม่ครบ)")
        return

    delta = pd.Timedelta(hours=1 if tf == "1h" else 4)
    bar_start = df.index[cl_i]
    if now_server() - (bar_start + delta) > pd.Timedelta(minutes=BREAK_WINDOW_MIN):
        print(f"[{tf.upper()}] เช็กเบรก: แท่งปิดล่าสุด {fmt(bar_start)} เก่าเกิน {BREAK_WINDOW_MIN} นาที ข้าม")
        return

    o, h, l, c = ohlc(df, cl_i)
    po, ph, pl, pc = ohlc(df, cl_i - 1)
    res_list = [v + PRICE_OFFSET for v in levels[tf]["res"]]
    sup_list = [v + PRICE_OFFSET for v in levels[tf]["sup"]]

    buf = BREAK_CLOSE_BUFFER
    # เบรกขึ้น: แท่งก่อนหน้ายังปิดไม่เหนือแนว และแท่งนี้ "ปิด" เหนือแนว (+buffer)
    up = [r for r in res_list if pc <= r + buf < c and level_known_before(tf, "res", r, bar_start + delta)]
    # เบรกลง: แท่งก่อนหน้ายังปิดไม่ใต้แนว และแท่งนี้ "ปิด" ใต้แนว (-buffer)
    down = [s for s in sup_list if c < s - buf <= pc and level_known_before(tf, "sup", s, bar_start + delta)]

    # ไส้ทะลุแนวแต่ปิดกลับ = เบรกหลอก ไม่แจ้งเบรก ไม่ลบแนว (แค่ลงล็อก)
    for r in res_list:
        if h > r and r not in up and c <= r + buf and pc <= r + buf:
            print(f"[{tf.upper()}] ไส้ทะลุแนวต้าน {px(r):.2f} (High {px(h):.2f}) แต่ปิด {px(c):.2f} ไม่เหนือแนว → ยังไม่นับเบรก เก็บแนวไว้")
    for s in sup_list:
        if l < s and s not in down and c >= s - buf and pc >= s - buf:
            print(f"[{tf.upper()}] ไส้ทะลุแนวรับ {px(s):.2f} (Low {px(l):.2f}) แต่ปิด {px(c):.2f} ไม่ใต้แนว → ยังไม่นับเบรก เก็บแนวไว้")

    n_res = min((r for r in res_list if r > c), default=None)   # ต้านใกล้สุดเหนือราคาปิด
    n_sup = max((s for s in sup_list if s < c), default=None)   # รับใกล้สุดใต้ราคาปิด
    print(f"[{tf.upper()}] เช็กเบรก แท่งปิด {fmt(bar_start)} | C ก่อนหน้า {px(pc):.2f} → C {px(c):.2f} | "
          f"ต้านถัดไป {'-' if n_res is None else f'{px(n_res):.2f}'} | รับถัดไป {'-' if n_sup is None else f'{px(n_sup):.2f}'} | "
          f"เบรกต้าน={len(up)} เบรกรับ={len(down)}")
    if not up and not down:
        return

    a = float(calculate_atr(df).iloc[cl_i])
    k = float(calculate_stoch(df).iloc[cl_i])
    tf_u = tf.upper()
    cid = str(bar_start)
    clabel = fmt(bar_start)
    risk = risk_notes(levels.get("news", []))
    pats = candle_patterns((po, ph, pl, pc), (o, h, l, c), k)
    k_txt = "-" if pd.isna(k) else f"{k:.1f}"
    rng = h - l
    body_pct = (abs(c - o) / rng) if rng > 0 else 0.0

    for side, hit in (("buy", up), ("sell", down)):
        if not hit:
            continue
        kind = "res" if side == "buy" else "sup"
        key = f"{tf}:break_{'up' if side == 'buy' else 'down'}"
        ok = (sent.get(key) == cid) or not BREAK_ALERT

        if BREAK_ALERT and not ok:
            names = ", ".join(f"{px(v):.2f}" for v in sorted(hit))
            if side == "buy":
                top = max(hit)
                title = f"💥 <b>ปิดแท่งเบรกแนวต้านขึ้น - TF {tf_u}</b>"
                lines = [
                    f"🔓 แนวต้านที่เบรก: <code>{names}</code>",
                    f"💵 ราคาปิด: <code>{px(c):.2f}</code> (เหนือแนว {c - top:.2f}) | ก่อนหน้าปิด <code>{px(pc):.2f}</code>",
                ]
                weak = c < o or body_pct < 0.4
            else:
                top = min(hit)
                title = f"💥 <b>ปิดแท่งเบรกแนวรับลง - TF {tf_u}</b>"
                lines = [
                    f"🔓 แนวรับที่เบรก: <code>{names}</code>",
                    f"💵 ราคาปิด: <code>{px(c):.2f}</code> (ใต้แนว {top - c:.2f}) | ก่อนหน้าปิด <code>{px(pc):.2f}</code>",
                ]
                weak = c > o or body_pct < 0.4
            lines.append(f"📏 ตัวแท่ง {body_pct:.0%} ของช่วงแท่ง | Stoch %K: <code>{k_txt}</code>")
            if weak:
                lines.append("⚠️ แท่งเบรกไม่แข็งแรง (ตัวเล็กหรือสีสวนทาง) ระวังเบรกหลอก")
            lines += ["🔎 <b>ลักษณะแท่ง</b>", *pats]
            if BREAK_AUTO_REMOVE:
                if SHEET_WEBHOOK_URL:
                    lines.append(f"🧹 ลบแนวนี้ออกจากชีต ({sheet_col(tf, kind)}) อัตโนมัติ")
                else:
                    lines.append(f"🧹 บอทเลิกใช้แนวนี้แล้ว (ยังไม่ได้ตั้ง SHEET_WEBHOOK_URL ให้ลบในชีต {sheet_col(tf, kind)} เอง)")
            msg = compose(title, clabel, lines, side, c, a,
                          next_target(side, c, sup_list, res_list), "", risk)
            ok = fire(key, cid, msg)

        if ok and BREAK_AUTO_REMOVE:
            for lvl in hit:
                register_removal(tf, kind, lvl - PRICE_OFFSET)

# ==========================================
# 🧭 4H CHoCH / BOS + ย่อกลับมาแตะโซน 50% (คำนวณจากโครงสร้างราคาเอง ไม่ใช้ชีต)
# CHoCH ขาขึ้น: โครงสร้างขาลง (LH/LL) แล้วปิดแท่งเหนือ LH ล่าสุด -> ขา = LL ถึงจุดสูงสุดหลังเบรก -> ย่อแตะ 50% = BUY
# CHoCH ขาลง : โครงสร้างขาขึ้น (HH/HL) แล้วปิดแท่งใต้ HL ล่าสุด -> ขา = HH ถึงจุดต่ำสุดหลังเบรก -> เด้งแตะ 50% = SELL
# BOS ขาขึ้น : โครงสร้างขาขึ้น (HH/HL) แล้วปิดแท่งเหนือ HH ล่าสุด -> ขา = HL ถึงจุดสูงสุดหลังเบรก -> ย่อแตะ 50% = BUY (ไปต่อ)
# BOS ขาลง  : โครงสร้างขาลง (LH/LL) แล้วปิดแท่งใต้ LL ล่าสุด -> ขา = LH ถึงจุดต่ำสุดหลังเบรก -> เด้งแตะ 50% = SELL (ไปต่อ)
# ==========================================
SMC_ALERT = os.environ.get("SMC_ALERT", "1") == "1"
SMC_BOS = os.environ.get("SMC_BOS", "1") == "1"                      # เปิด/ปิดสัญญาณ BOS
SMC_SWING = int(os.environ.get("SMC_SWING", "2"))                  # จุดสวิงต้องสูง/ต่ำสุดเมื่อเทียบกับกี่แท่งซ้าย-ขวา
SMC_FIB = float(os.environ.get("SMC_FIB", "0.5"))                  # ระดับย่อ (0.5 = 50%)
SMC_TOL_ATR = float(os.environ.get("SMC_TOL_ATR", "0.1"))          # เผื่อแตะโซน (เท่าของ ATR)
SMC_MIN_LEG_ATR = float(os.environ.get("SMC_MIN_LEG_ATR", "1.0"))  # ขาต้องยาวอย่างน้อยกี่เท่า ATR ถึงนับ
SMC_MAX_AGE = int(os.environ.get("SMC_MAX_AGE", "12"))             # CHoCH เก่าสุดกี่แท่ง 4H (12 = 2 วัน)
SMC_SL_ATR = float(os.environ.get("SMC_SL_ATR", "0.2"))            # SL เผื่อเลยจุดเริ่มขาไปกี่เท่า ATR
SMC_FIB_DEEP = float(os.environ.get("SMC_FIB_DEEP", "0.618"))      # ขอบลึกของโซน (โซน = SMC_FIB ถึง SMC_FIB_DEEP = 50%-61.8%)
SMC_M1_CONFIRM = True   # บังคับ: แจ้งเฉพาะตอนราคาชนโซน + มีแท่ง M1 กลับตัวเท่านั้น
SMC_M1_WICK_RATIO = float(os.environ.get("SMC_M1_WICK_RATIO", "2"))   # ไส้ M1 ต้องยาว >= กี่เท่าของตัวแท่ง
SMC_M1_WICK_PCT = float(os.environ.get("SMC_M1_WICK_PCT", "0.4"))     # ไส้ M1 ต้องยาว >= กี่ส่วนของช่วงแท่ง
SMC_M1_MAX_AGE_MIN = int(os.environ.get("SMC_M1_MAX_AGE_MIN", "5"))   # แท่งยืนยันต้องปิดไม่เกินกี่นาทีที่แล้ว
SMC_M1_NEAR_ATR = float(os.environ.get("SMC_M1_NEAR_ATR", "0.5"))     # ดึงข้อมูล M1 เมื่อราคาห่างโซนไม่เกินกี่เท่า ATR 4H
SMC_M1_SL_BUF = float(os.environ.get("SMC_M1_SL_BUF", "0.5"))         # SL แคบ: เผื่อเลยไส้ M1 กี่จุด
SMC_M1_PATTERNS = {"wick", "engulf", "pin", "star"}   # ล็อกเฉพาะ ไส้ปฏิเสธราคา / กลืนกิน / Pinbar / Star
SMC_M1_MIN_BODY = float(os.environ.get("SMC_M1_MIN_BODY", "0.3"))        # ตัวแท่งที่นับเป็นแท่งจริง (จุดราคา)
SMC_M1_ENGULF_TOL = float(os.environ.get("SMC_M1_ENGULF_TOL", "0.1"))    # เผื่อราคาเปิดไม่เป๊ะ (จุดราคา)
SMC_M1_TWEEZER_TOL = float(os.environ.get("SMC_M1_TWEEZER_TOL", "0.3"))  # Tweezer: High/Low สองแท่งต่างกันได้ไม่เกินกี่จุด
SMC_M1_PIN_PCT = float(os.environ.get("SMC_M1_PIN_PCT", "0.6"))          # Pinbar: ไส้หลักยาว >= กี่ส่วนของแท่ง
SMC_M1_TF_MIN = 1          # ความยาวแท่งยืนยัน (นาที) ปกติ 1 / ใช้ 5 ตอน backtest ด้วยข้อมูล M5
SMC_SIGNAL_HOOK = None     # ใช้ตอน backtest เท่านั้น (บอทจริงปล่อยเป็น None)
_m1_cache = {"ts": 0.0, "df": None}

# ---------- 🆕 SMC บน M15: เกิดรูปแบบ CHoCH/BOS ใน M15 แล้วราคาย้อนกลับมาชนโซน 50–61.8% = แจ้งเตือนทันที (ไม่รอ M1) ----------
M15_ALERT = os.environ.get("M15_ALERT", "1") == "1"                    # 0 = ปิดระบบ SMC M15
M15_BOS = os.environ.get("M15_BOS", "1") == "1"                        # รวมสัญญาณ BOS
M15_SWING = int(os.environ.get("M15_SWING", "2"))                      # จุดสวิงของ M15
M15_FIB = float(os.environ.get("M15_FIB", "0.5"))                      # ขอบตื้นของโซน (50%)
M15_FIB_DEEP = float(os.environ.get("M15_FIB_DEEP", "0.618"))          # ขอบลึกของโซน (61.8%)
M15_TOL_ATR = float(os.environ.get("M15_TOL_ATR", "0.1"))              # เผื่อแตะโซน (เท่าของ ATR M15)
M15_MIN_LEG_ATR = float(os.environ.get("M15_MIN_LEG_ATR", "1.0"))      # ขาต้องยาวอย่างน้อยกี่เท่า ATR M15
M15_MAX_AGE = int(os.environ.get("M15_MAX_AGE", "32"))                 # CHoCH/BOS เก่าสุดกี่แท่ง M15 (32 = 8 ชม.)
M15_SL_ATR = float(os.environ.get("M15_SL_ATR", "0.2"))                # SL เผื่อเลยจุดเริ่มขากี่เท่า ATR M15
M15_TOUCH_BARS = int(os.environ.get("M15_TOUCH_BARS", "2"))            # นับการชนโซนจากกี่แท่งล่าสุด (รวมแท่งสด) กันแจ้งของเก่า
M15_LIVE_GRACE_SEC = int(os.environ.get("M15_LIVE_GRACE_SEC", "120"))  # Yahoo ส่งแท่ง M15 ใหม่ช้า ถือว่าแท่งล่าสุดยังวิ่งอยู่ได้กี่วินาที
_m15_cache = {"ts": 0.0, "df": None}


def find_structure(df, n):
    """สแกนแท่งที่ปิดแล้ว คืน {"choch": เหตุการณ์ล่าสุด, "bos": เหตุการณ์ล่าสุด}
    เหตุการณ์ = {kind, side, idx, level, start} จุดสวิงยืนยันหลังผ่านไป n แท่ง และนับเฉพาะ H/L สลับกัน"""
    hi, lo, cl = df["High"].values, df["Low"].values, df["Close"].values
    piv, events = [], {}
    for j in range(2 * n, len(df)):
        i = j - n
        is_h = hi[i] > hi[i - n:i].max() and hi[i] >= hi[i + 1:j + 1].max()
        is_l = lo[i] < lo[i - n:i].min() and lo[i] <= lo[i + 1:j + 1].min()
        if is_h != is_l:
            kind, price = ("H", float(hi[i])) if is_h else ("L", float(lo[i]))
            if piv and piv[-1][1] == kind:
                if (kind == "H" and price > piv[-1][2]) or (kind == "L" and price < piv[-1][2]):
                    piv[-1] = (i, kind, price)   # H/L ติดกัน เก็บตัวที่สุดขอบกว่า
            else:
                piv.append((i, kind, price))

        highs = [p for p in piv if p[1] == "H"]
        lows = [p for p in piv if p[1] == "L"]
        if len(highs) < 2 or len(lows) < 2:
            continue
        h1, h2 = highs[-2], highs[-1]
        l1, l2 = lows[-2], lows[-1]
        if h2[2] > h1[2] and l2[2] > l1[2]:
            trend = "up"      # HH + HL
        elif h2[2] < h1[2] and l2[2] < l1[2]:
            trend = "down"    # LH + LL
        else:
            continue

        if trend == "down" and cl[j] > h2[2]:
            kind, side, level = "choch", "buy", h2[2]
            start = float(lo[l2[0]:j + 1].min())     # LL
        elif trend == "up" and cl[j] < l2[2]:
            kind, side, level = "choch", "sell", l2[2]
            start = float(hi[h2[0]:j + 1].max())     # HH
        elif trend == "up" and cl[j] > h2[2]:
            kind, side, level = "bos", "buy", h2[2]
            start = float(lo[l2[0]:j + 1].min())     # HL
        elif trend == "down" and cl[j] < l2[2]:
            kind, side, level = "bos", "sell", l2[2]
            start = float(hi[h2[0]:j + 1].max())     # LH
        else:
            continue
        prev = events.get(kind)
        if prev and prev["side"] == side and abs(prev["level"] - level) < 0.005:
            continue   # เหตุการณ์ตัวเดิม
        events[kind] = {"kind": kind, "side": side, "idx": j, "level": float(level), "start": start}
    return events


def check_choch_retrace(df, levels, last_1h_start):
    if not SMC_ALERT or len(df) < 40:
        return
    cl_i = closed_bar_index("4h", df, last_1h_start)
    if cl_i is None:
        return
    closed = df if cl_i == -1 else df.iloc[:-1]
    events = find_structure(closed, SMC_SWING)
    atr_val = float(calculate_atr(closed).iloc[-1])
    if pd.isna(atr_val):
        return
    risk = risk_notes(levels.get("news", []))
    for kind, ev in events.items():
        if kind == "bos" and not SMC_BOS:
            continue
        _structure_alert(df, closed, ev, atr_val, risk)


def check_ict_retrace(df, levels, last_1h_start):
    """ระบบ ICT (แยกจาก SMC): Sweep -> MSS/Displacement -> ย้อนเข้า OTE (+FVG) -> M1 กลับตัว"""
    if not ICT_ALERT or len(df) < 40:
        return
    cl_i = closed_bar_index("4h", df, last_1h_start)
    if cl_i is None:
        return
    closed = df if cl_i == -1 else df.iloc[:-1]
    events = find_structure(closed, ICT_SWING)
    atr_val = float(calculate_atr(closed).iloc[-1])
    if pd.isna(atr_val):
        return
    risk = risk_notes(levels.get("news", []))
    for kind, ev in events.items():
        if kind == "bos" and not ICT_BOS:
            continue
        _structure_alert(df, closed, ev, atr_val, risk, mode="ict")


def _fetch_1m_yahoo():
    c = _m1_cache
    if c["df"] is not None and time.time() - c["ts"] < 20:
        return c["df"]
    df = yf.download(SYMBOL, period="2d", interval="1m", progress=False, auto_adjust=False, timeout=20)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()
    df.index = to_server_index(df.index)
    df = df[~df.index.duplicated(keep="last")]
    c.update(ts=time.time(), df=df)
    return df


# ---------- 🆕 SMC M15: ดึงข้อมูล M15 + ตรวจการย้อนชนโซน 50–61.8% แล้วแจ้งทันที ----------
def _fetch_15m_yahoo():
    c = _m15_cache
    if c["df"] is not None and time.time() - c["ts"] < 45:
        return c["df"]
    df = yf.download(SYMBOL, period="10d", interval="15m", progress=False, auto_adjust=False, timeout=20)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()
    df.index = to_server_index(df.index)
    df = df[~df.index.duplicated(keep="last")]
    c.update(ts=time.time(), df=df)
    return df


def check_m15_retrace(levels):
    """SMC บน M15: เกิด CHoCH/BOS (ปิดแท่งเบรก) -> ขา = จุดเริ่มขาถึงจุดสุดขอบหลังเบรก
    -> ราคาย้อนกลับมาชนโซน 50–61.8% ของขา = แจ้งเตือนทันที (ไม่ต้องรอแท่ง M1 ยืนยัน และไม่ต้องรอปิดแท่ง M15)"""
    if not M15_ALERT:
        return
    df = fetch_15m()
    if df is None or len(df) < 40:
        print("[M15 SMC] ดึงข้อมูล M15 ไม่ได้/ไม่พอ ข้ามรอบนี้")
        return

    delta = pd.Timedelta(minutes=15)
    now = now_server()
    is_live = df.index[-1] + delta + pd.Timedelta(seconds=M15_LIVE_GRACE_SEC) > now
    closed = df.iloc[:-1] if is_live else df
    if len(closed) < 40:
        return

    events = find_structure(closed, M15_SWING)
    atr_val = float(calculate_atr(closed).iloc[-1])
    if pd.isna(atr_val) or atr_val <= 0:
        return
    risk = risk_notes(levels.get("news", []))
    if SKIP_RISK_WINDOWS and risk:
        print(f"[M15 SMC] ข้ามรอบนี้ (ช่วงเสี่ยง): {' / '.join(risk)}")
        return

    cur = float(df["Close"].iloc[-1])
    tol = M15_TOL_ATR * atr_val
    for kind, ev in events.items():
        if kind == "bos" and not M15_BOS:
            continue
        age = len(closed) - 1 - ev["idx"]
        if age > M15_MAX_AGE:
            continue
        buy = ev["side"] == "buy"
        start = ev["start"]
        name = "CHoCH" if kind == "choch" else "BOS"
        after = df.iloc[ev["idx"]:]                      # ตั้งแต่แท่งเบรกจนถึงแท่งสด
        if buy:
            extreme = float(after["High"].max())
            leg = extreme - start
            z_top, z_bot = extreme - leg * M15_FIB, extreme - leg * M15_FIB_DEEP
            broken = float(after["Close"].min()) < start
        else:
            extreme = float(after["Low"].min())
            leg = start - extreme
            z_bot, z_top = extreme + leg * M15_FIB, extreme + leg * M15_FIB_DEEP
            broken = float(after["Close"].max()) > start

        if buy:
            ext_pos = int(after["High"].values.argmax())
        else:
            ext_pos = int(after["Low"].values.argmin())
        post = after.iloc[ext_pos + 1:]                  # แท่งหลังทำจุดสุดขอบ = แท่งที่ย้อนกลับมา
        recent = post.iloc[-M15_TOUCH_BARS:] if len(post) else post

        touched, touch_px = False, None
        if len(recent):
            if buy:
                touch_px = float(recent["Low"].min())
                touched = touch_px <= z_top + tol and touch_px > start
            else:
                touch_px = float(recent["High"].max())
                touched = touch_px >= z_bot - tol and touch_px < start

        ev_label = fmt(df.index[ev["idx"]])
        print(f"[M15 SMC] {name} {'BUY' if buy else 'SELL'} แท่ง {ev_label} | ขา {px(start):.2f}→{px(extreme):.2f} "
              f"| โซน {px(z_bot):.2f}–{px(z_top):.2f} | ราคา {px(cur):.2f} | อายุ {age} แท่ง | "
              f"ขาพัง={broken} | ชนโซน={touched}")
        if broken or leg < M15_MIN_LEG_ATR * atr_val or not touched:
            continue

        entry = cur
        if buy:
            sl = start - M15_SL_ATR * atr_val
            rr = (extreme - entry) / (entry - sl) if entry > sl else 0
            s_lbl, e_lbl, brk = ("LL", "จุดสูงสุด", "เหนือ LH") if kind == "choch" else ("HL", "จุดสูงสุด", "เหนือ HH")
        else:
            sl = start + M15_SL_ATR * atr_val
            rr = (entry - extreme) / (sl - entry) if sl > entry else 0
            s_lbl, e_lbl, brk = ("HH", "จุดต่ำสุด", "ใต้ HL") if kind == "choch" else ("LH", "จุดต่ำสุด", "ใต้ LL")

        act = "BUY" if buy else "SELL"
        zone_txt = f"{M15_FIB * 100:g}–{M15_FIB_DEEP * 100:g}%"
        title = (f"⚡ <b>[SMC M15] {name} {'ขาขึ้น' if buy else 'ขาลง'} + ราคาย้อนชนโซน {zone_txt} → {act} ทันที</b>")
        lines = [
            f"🔓 ปิด{brk} <code>{px(ev['level']):.2f}</code> (แท่ง M15 {ev_label})",
            f"📏 ขา: {s_lbl} <code>{px(start):.2f}</code> → {e_lbl} <code>{px(extreme):.2f}</code> ({leg:.2f} จุด)",
            f"🎯 โซน {zone_txt}: <code>{px(z_bot):.2f}</code> – <code>{px(z_top):.2f}</code>",
            f"📍 ราคาย้อนชนโซนที่ <code>{px(touch_px):.2f}</code> | ราคาตอนนี้ <code>{px(cur):.2f}</code>",
            f"🛑 SL (เลยจุดเริ่มขา +{M15_SL_ATR:g}×ATR M15 {atr_val:.2f}): <code>{px(sl):.2f}</code> | R:R 1:{rr:.1f}",
            f"🏁 เป้า ({e_lbl}ของขา): <code>{px(extreme):.2f}</code>",
            "⏳ แจ้งทันทีที่ชนโซน (ไม่รอ M1 ยืนยัน) ดูแท่งกลับตัวบนกราฟก่อนเข้า",
        ]
        parts = [title, f"📊 XAUUSD ({SYMBOL}) | TF M15"] + lines + risk
        if PRICE_OFFSET:
            parts.append(f"ℹ️ ราคาแสดงเป็นระดับ MT5 (GC=F − {PRICE_OFFSET:g})")
        parts.append(data_source_note())
        parts.append("📱 <b>เปิด MT5 เช็กกราฟก่อนตัดสินใจ</b>")
        key = f"m15:{kind}_{ev['side']}"
        sent_ok = fire(key, f"{df.index[ev['idx']]}|{ev['level']:.2f}", "\n".join(p for p in parts if p))
        if sent_ok:
            try:
                record_signal("m15", kind, ev["side"], entry, extreme, sl, None, None)
            except Exception as e:
                print(f"Journal record error: {e}")


def _mir(bar):
    """กลับด้านราคา (O,H,L,C) เพื่อให้ฝั่ง BUY ใช้ตรรกะฝั่ง SELL ชุดเดียวกัน"""
    o, h, l, c = bar
    return (-o, -l, -h, -c)


def _m1_sell_patterns(z, a, b):
    """แพทเทิร์นกลับตัวขาลงบนแท่ง M1 ที่ปิดแล้ว (z, a, b = 3 แท่งล่าสุดเรียงเวลา, b ล่าสุด, z อาจเป็น None)
    คืน [(รหัส, High สุดขอบของแพทเทิร์น)]"""
    out = []
    bo, bh, bl, bc = b
    rng = bh - bl
    if rng <= 0 or a is None:
        return out
    ao, ah, al, ac = a
    body = max(abs(bc - bo), 0.01)
    up = bh - max(bo, bc)
    low_w = min(bo, bc) - bl
    a_green, b_red = ac > ao, bc < bo
    a_body, b_body = ac - ao, bo - bc
    both = a_green and b_red and a_body >= SMC_M1_MIN_BODY and b_body >= SMC_M1_MIN_BODY
    top = max(ah, bh)
    P = SMC_M1_PATTERNS

    # ไส้ปฏิเสธราคา: เขียว->แดง ไส้บนยาว ทะลุเหนือ High แท่งเขียว
    if "wick" in P and a_green and b_red and bh > ah and up >= SMC_M1_WICK_RATIO * body and up >= SMC_M1_WICK_PCT * rng:
        out.append(("wick", bh))
    # กลืนกิน: ตัวแดงคลุมตัวเขียว
    if "engulf" in P and both and bc <= ao and bo >= ac - SMC_M1_ENGULF_TOL:
        out.append(("engulf", top))
    # Pinbar / Shooting Star: ไส้บนยาว ตัวเล็ก ปิดในช่วงล่างของแท่ง
    if "pin" in P and up >= SMC_M1_WICK_RATIO * body and up >= SMC_M1_PIN_PCT * rng \
            and low_w <= 0.25 * rng and (bc - bl) <= 0.4 * rng:
        out.append(("pin", bh))
    # Evening Star: เขียวแรง -> แท่งตัวเล็กอยู่ครึ่งบน -> แดงปิดต่ำกว่ากลางแท่งเขียว
    if "star" in P and z is not None:
        zo, zh, zl, zc = z
        zmid = (zo + zc) / 2
        if (zc - zo) >= SMC_M1_MIN_BODY and abs(ac - ao) <= 0.3 * (zc - zo) and min(ao, ac) >= zmid \
                and b_body >= SMC_M1_MIN_BODY and bc < zmid:
            out.append(("star", max(zh, ah, bh)))
    # Tweezer Top: High ของสองแท่งเท่ากัน เขียว->แดง
    if "tweezer" in P and both and abs(ah - bh) <= SMC_M1_TWEEZER_TOL:
        out.append(("tweezer", top))
    # Dark Cloud Cover: แดงเปิดที่/เหนือราคาปิดเขียว ปิดลึกกว่าครึ่งตัวเขียวแต่ไม่ถึงราคาเปิด
    if "cloud" in P and both and bo >= ac - SMC_M1_ENGULF_TOL and ao < bc < (ao + ac) / 2:
        out.append(("cloud", top))
    return out


_M1_NAMES = {
    "sell": {"wick": "ไส้บนปฏิเสธราคา", "engulf": "Bearish Engulfing (กลืนกิน)", "pin": "Shooting Star (Pinbar)",
             "star": "Evening Star", "tweezer": "Tweezer Top", "cloud": "Dark Cloud Cover"},
    "buy": {"wick": "ไส้ล่างปฏิเสธราคา", "engulf": "Bullish Engulfing (กลืนกิน)", "pin": "Hammer (Pinbar)",
            "star": "Morning Star", "tweezer": "Tweezer Bottom", "cloud": "Piercing Line"},
}


def find_m1_confirm(side, m1, z_lo, z_hi, tol):
    """หาแท่งยืนยัน M1 (เฉพาะแท่งที่ปิดแล้ว 3 แท่งล่าสุดเป็นแท่งสุดท้ายของแพทเทิร์น)
    แพทเทิร์นต้องชนโซน [z_lo, z_hi] และแท่งสุดท้ายปิดไม่เลยโซนไปไกล คืน dict หรือ None"""
    now = now_server()
    closed = m1[m1.index + pd.Timedelta(minutes=SMC_M1_TF_MIN) <= now]
    n = len(closed)
    if n < 2:
        return None
    sell = side == "sell"
    zl, zh = (z_lo, z_hi) if sell else (-z_hi, -z_lo)   # BUY ใช้ราคากลับด้าน
    conv = (lambda x: x) if sell else _mir
    for k in range(n - 1, max(n - 4, 0), -1):
        t_close = closed.index[k] + pd.Timedelta(minutes=SMC_M1_TF_MIN)
        if now - t_close > pd.Timedelta(minutes=SMC_M1_MAX_AGE_MIN):
            break
        b, a = conv(ohlc(closed, k)), conv(ohlc(closed, k - 1))
        z = conv(ohlc(closed, k - 2)) if k >= 2 else None
        hits = [(nm, ext) for nm, ext in _m1_sell_patterns(z, a, b) if ext >= zl - tol and b[3] <= zh + tol]
        if hits:
            ext = max(e for _, e in hits)
            return {"time": closed.index[k], "b": ohlc(closed, k), "ext": ext if sell else -ext,
                    "names": [_M1_NAMES[side][nm] for nm, _ in hits]}
    return None


# ---------- 🏷 ป้าย ICT Confluence (แสดงต่อท้ายข้อความ ไม่ตัดสัญญาณใดๆ) ----------
# ระบบ ICT แยกจาก SMC: แจ้งเตือนคนละชุด (key/หัวข้อ/โซนต่างกัน) ใช้ M1 ยืนยันกลับตัวชุดเดียวกัน
ICT_ALERT = os.environ.get("ICT_ALERT", "1") == "1"                    # 0 = ปิดระบบ ICT ทั้งชุด
ICT_BOS = os.environ.get("ICT_BOS", "0") == "1"                        # 1 = รวมสัญญาณ BOS (ค่าเริ่มต้นใช้เฉพาะ MSS/CHoCH)
ICT_SWING = int(os.environ.get("ICT_SWING", "2"))                      # จุดสวิงของ ICT
ICT_MAX_AGE = int(os.environ.get("ICT_MAX_AGE", "12"))                 # อายุ MSS สูงสุด (แท่ง 4H)
ICT_FIB = float(os.environ.get("ICT_FIB", "0.62"))                     # ขอบตื้นของ OTE
ICT_FIB_DEEP = float(os.environ.get("ICT_FIB_DEEP", "0.79"))           # ขอบลึกของ OTE
ICT_REQUIRE_SWEEP = os.environ.get("ICT_REQUIRE_SWEEP", "1") == "1"    # บังคับ: ต้องกวาดสภาพคล่องก่อน MSS
ICT_REQUIRE_DISP = os.environ.get("ICT_REQUIRE_DISP", "1") == "1"      # บังคับ: แท่งเบรกต้องวิ่งแรง
ICT_REQUIRE_FVG = os.environ.get("ICT_REQUIRE_FVG", "1") == "1"        # บังคับ: ต้องมี FVG ซ้อนโซน OTE
ICT_REQUIRE_PD = os.environ.get("ICT_REQUIRE_PD", "0") == "1"          # บังคับ: Discount (BUY) / Premium (SELL)
ICT_REQUIRE_KZ = os.environ.get("ICT_REQUIRE_KZ", "0") == "1"          # บังคับ: อยู่ใน Kill Zone
ICT_SWEEP_LOOKBACK = int(os.environ.get("ICT_SWEEP_LOOKBACK", "10"))   # เทียบสวิงกี่แท่งก่อนหน้าจุดเริ่มขา
ICT_DISP_ATR = float(os.environ.get("ICT_DISP_ATR", "1.0"))            # แท่งเบรกต้องมีตัวแท่ง >= กี่เท่า ATR ถึงนับว่าวิ่งแรง
ICT_PD_BARS = int(os.environ.get("ICT_PD_BARS", "60"))                 # กรอบ Premium/Discount ย้อนหลังกี่แท่ง 4H (60 = 10 วัน)
ICT_KILL_ZONES = os.environ.get("ICT_KILL_ZONES", "London=02:00-05:00,NY AM=07:00-10:00")  # เวลานิวยอร์ก


def _parse_kz():
    out = []
    for part in ICT_KILL_ZONES.split(","):
        try:
            name, rng = part.split("=")
            a, b = rng.strip().split("-")
            ah, am = a.split(":")
            bh, bm = b.split(":")
            out.append((name.strip(), int(ah) * 60 + int(am), int(bh) * 60 + int(bm)))
        except Exception:
            continue
    return out


def ict_confluence(closed, ev, buy, atr_val, z_bot, z_top, start):
    """คืนบรรทัดป้าย ICT 5 ข้อ: Sweep / Displacement / FVG ซ้อนโซน / Premium-Discount / Kill Zone"""
    hi, lo = closed["High"].values, closed["Low"].values
    op, cl = closed["Open"].values, closed["Close"].values
    n, j = len(closed), ev["idx"]
    score, rows = 0, []

    # 1) Liquidity Sweep: จุดเริ่มขากวาดสวิงก่อนหน้า แล้วปิดกลับ
    a = max(0, j - 15)
    win = lo[a:j + 1] if buy else hi[a:j + 1]
    k = a + int(abs(win - start).argmin())
    b0 = max(0, k - ICT_SWEEP_LOOKBACK)
    swept = False
    if k > b0:
        k2 = min(k + 1, n - 1)
        if buy:
            ref = float(lo[b0:k].min())
            swept = lo[k] < ref and max(cl[k], cl[k2]) > ref
        else:
            ref = float(hi[b0:k].max())
            swept = hi[k] > ref and min(cl[k], cl[k2]) < ref
    if swept:
        score += 1
        rows.append(f"✅ Sweep: กวาด{'Low' if buy else 'High'}เดิม <code>{px(ref):.2f}</code> แล้วปิดกลับ")
    else:
        rows.append(f"⚠️ Sweep: ไม่พบการกวาด{'Low' if buy else 'High'}ก่อนขานี้")

    # 2) Displacement: แท่งเบรกวิ่งแรงตามทิศ
    body = abs(cl[j] - op[j])
    disp = body >= ICT_DISP_ATR * atr_val and ((cl[j] > op[j]) if buy else (cl[j] < op[j]))
    if disp:
        score += 1
        rows.append(f"✅ Displacement: แท่งเบรกตัว {body / atr_val:.1f}×ATR")
    else:
        rows.append(f"⚠️ Displacement: แท่งเบรกตัว {body / atr_val:.1f}×ATR (อ่อน)")

    # 3) FVG ที่ซ้อนกับโซน Fib
    fvg = None
    for i in range(n - 2, k, -1):
        g_lo, g_hi = (hi[i - 1], lo[i + 1]) if buy else (hi[i + 1], lo[i - 1])
        if g_lo < g_hi and g_lo <= z_top and g_hi >= z_bot:
            fvg = (float(g_lo), float(g_hi))
            break
    if fvg:
        score += 1
        rows.append(f"✅ FVG ซ้อนโซน: <code>{px(fvg[0]):.2f}</code> – <code>{px(fvg[1]):.2f}</code>")
    else:
        rows.append("⚠️ FVG: ไม่มีช่องว่างซ้อนโซน")

    # 4) Premium / Discount ของกรอบใหญ่
    w = max(0, n - ICT_PD_BARS)
    mid = (float(hi[w:].max()) + float(lo[w:].min())) / 2
    zmid = (z_bot + z_top) / 2
    ok_pd = zmid < mid if buy else zmid > mid
    zone_name = "Discount" if zmid < mid else "Premium"
    if ok_pd:
        score += 1
        rows.append(f"✅ {zone_name}: โซนอยู่{'ต่ำ' if buy else 'สูง'}กว่าครึ่งกรอบ <code>{px(mid):.2f}</code> (ตรงทิศ {'BUY' if buy else 'SELL'})")
    else:
        rows.append(f"⚠️ {zone_name}: โซนอยู่{'สูง' if buy else 'ต่ำ'}กว่าครึ่งกรอบ <code>{px(mid):.2f}</code> (สวนทิศ {'BUY' if buy else 'SELL'})")

    # 5) Kill Zone (เวลานิวยอร์ก)
    ny = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ))
    m = ny.hour * 60 + ny.minute
    kz = next((nm for nm, st, en in _parse_kz() if st <= m < en), None)
    if kz:
        score += 1
        rows.append(f"✅ Kill Zone: {kz}")
    else:
        rows.append("⚠️ นอก Kill Zone")

    flags = {"sweep": bool(swept), "disp": bool(disp), "fvg": fvg is not None, "pd": bool(ok_pd), "kz": kz is not None}
    return [f"🧩 <b>ICT Checklist {score}/5</b>"] + rows, flags


def _structure_alert(df, closed, ev, atr_val, risk, mode="smc"):
    ict = mode == "ict"
    tag = "ICT" if ict else "SMC"
    f_lo, f_hi = (ICT_FIB, ICT_FIB_DEEP) if ict else (SMC_FIB, SMC_FIB_DEEP)
    age = len(closed) - 1 - ev["idx"]
    if age > (ICT_MAX_AGE if ict else SMC_MAX_AGE):
        return
    kind, buy, start = ev["kind"], ev["side"] == "buy", ev["start"]
    name = "CHoCH" if kind == "choch" else "BOS"
    after = df.iloc[ev["idx"]:]          # ตั้งแต่แท่งเบรกจนถึงแท่งสด
    cur = float(df["Close"].iloc[-1])
    tol = SMC_TOL_ATR * atr_val
    near = max(tol, SMC_M1_NEAR_ATR * atr_val)
    if buy:
        extreme = float(after["High"].max())
        leg = extreme - start
        z_top, z_bot = extreme - leg * f_lo, extreme - leg * f_hi
        broken = float(after["Close"].min()) < start
        near_ok = start < cur <= z_top + near
    else:
        extreme = float(after["Low"].min())
        leg = start - extreme
        z_bot, z_top = extreme + leg * f_lo, extreme + leg * f_hi
        broken = float(after["Close"].max()) > start
        near_ok = z_bot - near <= cur < start

    ev_label = fmt(df.index[ev["idx"]])
    print(f"[4H {tag}] {name} {'BUY' if buy else 'SELL'} แท่ง {ev_label} | ขา {px(start):.2f}→{px(extreme):.2f} "
          f"| โซน {px(z_bot):.2f}–{px(z_top):.2f} | ราคา {px(cur):.2f} | อายุ {age} แท่ง | ขาพัง={broken} | ใกล้โซน={near_ok}")
    if broken or leg < SMC_MIN_LEG_ATR * atr_val or not near_ok:
        return
    if SKIP_RISK_WINDOWS and risk:
        return

    ict_lines = []
    if ict:   # เงื่อนไขบังคับของระบบ ICT (ก่อนไปขอข้อมูล M1)
        ict_lines, fl = ict_confluence(closed, ev, buy, atr_val, z_bot, z_top, start)
        need = [("sweep", ICT_REQUIRE_SWEEP), ("disp", ICT_REQUIRE_DISP), ("fvg", ICT_REQUIRE_FVG),
                ("pd", ICT_REQUIRE_PD), ("kz", ICT_REQUIRE_KZ)]
        missing = [k for k, req in need if req and not fl[k]]
        if missing:
            print(f"[4H ICT] {name} {'BUY' if buy else 'SELL'}: ไม่ผ่านเงื่อนไขบังคับ {missing} (ยังไม่แจ้ง)")
            return

    side = ev["side"]
    m1 = None
    if SMC_M1_CONFIRM:
        m1df = fetch_1m()
        if m1df is None:
            print(f"[4H {tag}] {name}: ดึงข้อมูล M1 ไม่ได้ ลองใหม่รอบหน้า")
            return
        m1 = find_m1_confirm(side, m1df, z_bot, z_top, tol)
        if m1 is None:
            print(f"[4H {tag}] {name} {side}: ราคาใกล้โซน รอแท่ง M1 กลับตัวที่ปิดแล้ว (ยังไม่แจ้ง)")
            return
        entry = m1["b"][3]
    else:
        if not (z_bot - tol <= cur <= z_top + tol):
            return
        entry = cur

    if kind == "choch":
        s_lbl, e_lbl, brk = ("LL", "จุดสูงสุด", "เหนือ LH") if buy else ("HH", "จุดต่ำสุด", "ใต้ HL")
        icon, what = ("🎯", f"MSS {'ขาขึ้น' if buy else 'ขาลง'}") if ict else ("🧭", f"CHoCH {'ขาขึ้น' if buy else 'ขาลง'}")
    else:
        s_lbl, e_lbl, brk = ("HL", "จุดสูงสุด", "เหนือ HH") if buy else ("LH", "จุดต่ำสุด", "ใต้ LL")
        icon, what = ("🎯", f"BOS {'ขาขึ้น' if buy else 'ขาลง'}") if ict else ("📐", f"BOS {'ขาขึ้น' if buy else 'ขาลง'}")
    act = "BUY" if buy else "SELL"
    zone_txt = f"{f_lo * 100:g}–{f_hi * 100:g}%"
    title = f"{icon} <b>[{tag}] 4H {what} + {'M1 ยืนยัน' if m1 else 'เข้าโซน'} {'OTE ' if ict else ''}{zone_txt} → {act}</b>"

    if buy:
        sl = start - SMC_SL_ATR * atr_val
        rr = (extreme - entry) / (entry - sl) if entry > sl else 0
    else:
        sl = start + SMC_SL_ATR * atr_val
        rr = (entry - extreme) / (sl - entry) if sl > entry else 0
    lines = [
        f"🔓 ปิด{brk} <code>{px(ev['level']):.2f}</code> (แท่ง {ev_label})",
        f"📏 ขา: {s_lbl} <code>{px(start):.2f}</code> → {e_lbl} <code>{px(extreme):.2f}</code> ({leg:.2f} จุด)",
        f"🎯 โซน {'OTE ' if ict else ''}{zone_txt}: <code>{px(z_bot):.2f}</code> – <code>{px(z_top):.2f}</code>",
    ]
    sl_t = None
    if m1:
        sl_t = m1["ext"] - SMC_M1_SL_BUF if buy else m1["ext"] + SMC_M1_SL_BUF
        if buy:
            rr_t = (extreme - entry) / (entry - sl_t) if entry > sl_t else 0
        else:
            rr_t = (entry - extreme) / (sl_t - entry) if sl_t > entry else 0
        lines.append(f"🕯 M1 {fmt(m1['time'])} ({len(m1['names'])} รูปแบบ): " + " + ".join(m1["names"]))
        lines.append(f"💵 เข้า (ราคาปิด M1): <code>{px(entry):.2f}</code> | จุดสุดขอบแพทเทิร์น <code>{px(m1['ext']):.2f}</code>")
        lines.append(f"🛑 SL แคบ (เลยแพทเทิร์น {SMC_M1_SL_BUF:g} จุด): <code>{px(sl_t):.2f}</code> | R:R 1:{rr_t:.1f}")
        lines.append(f"🛡 SL โครงสร้าง (เลยจุดเริ่มขา +{SMC_SL_ATR:g}×ATR {atr_val:.2f}): <code>{px(sl):.2f}</code> | R:R 1:{rr:.1f}")
    else:
        lines.append(f"💵 ราคาตอนนี้: <code>{px(cur):.2f}</code>")
        lines.append(f"🛑 SL (เลยจุดเริ่มขา +{SMC_SL_ATR:g}×ATR {atr_val:.2f}): <code>{px(sl):.2f}</code> | R:R 1:{rr:.1f}")
    lines.append(f"🏁 เป้า ({e_lbl}ของขา): <code>{px(extreme):.2f}</code>")
    lines += ict_lines
    parts = [title, f"📊 XAUUSD ({SYMBOL}) | TF 4H"] + lines + risk
    if PRICE_OFFSET:
        parts.append(f"ℹ️ ราคาแสดงเป็นระดับ MT5 (GC=F − {PRICE_OFFSET:g})")
    parts.append(data_source_note())
    parts.append("📱 <b>เปิด MT5 เช็กกราฟก่อนตัดสินใจ</b>")
    key = f"{'ict' if ict else '4h'}:{kind}_{side}" + ("_m1" if m1 else "")
    sent_ok = fire(key, f"{df.index[ev['idx']]}|{ev['level']:.2f}", "\n".join(p for p in parts if p))
    if sent_ok:
        try:
            record_signal(tag.lower(), kind, side, entry, extreme, sl, sl_t,
                          sum(1 for v in fl.values() if v) if ict else None)
        except Exception as e:
            print(f"Journal record error: {e}")
    if sent_ok and SMC_SIGNAL_HOOK:
        SMC_SIGNAL_HOOK({"system": tag, "kind": kind, "side": side, "entry": entry, "tp": extreme, "sl": sl, "sl_t": sl_t,
                         "names": list(m1["names"]) if m1 else [], "z_bot": z_bot, "z_top": z_top,
                         "event_time": df.index[ev["idx"]]})

# ==========================================
# 🧪 สมุดบันทึกสัญญาณ (Signal Journal): จดสัญญาณ SMC/ICT/SMC M15 ที่บอทส่ง แล้วตามดูเองว่าชนเป้าหรือ SL ก่อน
# วัดผล 2 แบบต่อสัญญาณ: SL แคบ (เลยแพทเทิร์น M1) และ SL โครงสร้าง / ถ้า SL กับเป้าอยู่แท่ง 5 นาทีเดียวกัน นับเป็น SL
# ใช้ราคา GC=F ตรงๆ (เหมือนส่วนอื่น) แสดงผลเป็นสถิติท้ายสรุปรายสัปดาห์/เดือน/ปี
# ==========================================
JOURNAL_ENABLED = os.environ.get("JOURNAL_ENABLED", "1") == "1"
JOURNAL_MAX_DAYS = int(os.environ.get("JOURNAL_MAX_DAYS", "3"))   # สัญญาณที่ยังไม่ชนทั้งเป้า/SL ภายในกี่วัน ถือว่าหมดอายุ (ไม่นับ)


def record_signal(system, kind, side, entry, tp, sl, sl_t, score):
    if not JOURNAL_ENABLED:
        return
    j = sent.setdefault("journal", {})
    t = now_server().floor("min")
    jid = f"{system}|{kind}|{side}|{t:%Y%m%d%H%M}"
    j[jid] = {"sy": system, "k": kind, "d": side, "t": str(t),
              "e": round(entry, 2), "tp": round(tp, 2), "s": round(sl, 2),
              "st": round(sl_t, 2) if sl_t is not None else None,
              "sc": score, "rs": None, "rt": None}
    cutoff = now_server() - pd.Timedelta(days=LEDGER_KEEP_DAYS)
    for k in [k for k, v in j.items() if pd.Timestamp(v["t"]) < cutoff]:
        j.pop(k, None)
    save_state()


def _journal_pending(r):
    return r["rs"] is None or (r["st"] is not None and r["rt"] is None)


def monitor_journal():
    """ตามผลสัญญาณที่ยังไม่จบ (ใช้ข้อมูล 5 นาที ดึงเฉพาะเมื่อมีสัญญาณค้าง)"""
    if not JOURNAL_ENABLED:
        return
    j = sent.get("journal")
    pend = [(k, r) for k, r in (j or {}).items() if _journal_pending(r)]
    if not pend:
        return
    now = now_server()
    days = min((now - min(pd.Timestamp(r["t"]) for _, r in pend)).days + 2, 59)
    bars = fetch_5m(days)
    if bars is None:
        return
    changed = False
    for _, r in pend:
        t = pd.Timestamp(r["t"])
        sub = bars[bars.index >= t.floor("5min") + pd.Timedelta(minutes=5)]
        buy = r["d"] == "buy"
        for key, slv in (("rs", r["s"]), ("rt", r["st"])):
            if r[key] is not None or slv is None:
                continue
            if sub.empty:
                continue
            hit_sl = (sub["Low"] <= slv) if buy else (sub["High"] >= slv)
            hit_tp = (sub["High"] >= r["tp"]) if buy else (sub["Low"] <= r["tp"])
            anyh = hit_sl | hit_tp
            if anyh.any():
                first = anyh.idxmax()
                r[key] = "sl" if bool(hit_sl.loc[first]) else "tp"
                changed = True
            elif now - t > pd.Timedelta(days=JOURNAL_MAX_DAYS):
                r[key] = "exp"
                changed = True
    if changed:
        save_state()


def _jr(r, key, res):
    """ผลเป็นหน่วย R: SL = -1R / ถึงเป้า = ระยะเป้า ÷ ระยะ SL"""
    if res == "sl":
        return -1.0
    slv = r["s"] if key == "rs" else r["st"]
    risk = abs(r["e"] - slv)
    return abs(r["tp"] - r["e"]) / risk if risk > 0 else 0.0


def _jline(label, items):
    """items = [(record, key)] นับเฉพาะที่จบแล้ว (tp/sl)"""
    done = [(r, r[k], k) for r, k in items if r[k] in ("tp", "sl")]
    if not done:
        return f"  • {label}: ยังไม่มีผลที่จบ"
    w = sum(1 for _, res, _ in done if res == "tp")
    sr = sum(_jr(r, k, res) for r, res, k in done)
    return f"  • {label}: ชนะ {w}/{len(done)} ({w / len(done) * 100:.0f}%) | รวม {sr:+.2f}R"


def journal_lines(start_s, end_s):
    """บรรทัดสถิติสัญญาณบอทของช่วง (start_s, end_s] เวลา MT5 (นับตามเวลาที่บอทส่งสัญญาณ)"""
    recs = [r for r in sent.get("journal", {}).values() if start_s < pd.Timestamp(r["t"]) <= end_s]
    if not recs:
        return []
    out = ["", "🧪 <b>สถิติสัญญาณบอท</b> (วัดตามแผน SL/เป้าที่แจ้ง)"]
    for sy, name in (("smc", "SMC"), ("ict", "ICT"), ("m15", "SMC M15")):
        rs = [r for r in recs if r["sy"] == sy]
        if not rs:
            continue
        pend = sum(1 for r in rs if _journal_pending(r))
        out.append(f"{name}: {len(rs)} สัญญาณ" + (f" (รอผล {pend})" if pend else ""))
        if sy != "m15":   # SMC M15 ไม่มี SL แคบจาก M1
            out.append(_jline("SL แคบ", [(r, "rt") for r in rs if r["st"] is not None]))
        out.append(_jline("SL โครงสร้าง", [(r, "rs") for r in rs]))
        if sy == "ict":
            hi = [(r, "rt") for r in rs if r["st"] is not None and (r["sc"] or 0) >= 4]
            lo = [(r, "rt") for r in rs if r["st"] is not None and (r["sc"] or 0) < 4]
            if hi:
                out.append(_jline("คะแนน ICT ≥4 (SL แคบ)", hi))
            if lo:
                out.append(_jline("คะแนน ICT &lt;4 (SL แคบ)", lo))
    return out


# ==========================================
# 🔍 วนตรวจตลาด
# ==========================================
def send_heartbeat(df_1h):
    price_txt = f"{px(float(df_1h['Close'].iloc[-1])):.2f}" if df_1h is not None else "ไม่ทราบ"
    send_telegram_message(
        f"🤖 <b>บอทรันปกติ (Heartbeat)</b>\n"
        f"📊 XAUUSD ({SYMBOL})\n"
        f"💰 ราคาล่าสุด (ระดับ MT5): <code>{price_txt}</code>\n"
        f"🕒 เวลา MT5 ตอนนี้: <code>{fmt(now_server())}</code>\n"
        f"🟢 ระบบสแตนด์บายเฝ้าจอ"
    )


def feed_time_check():
    """เทียบเวลาเซิร์ฟเวอร์ MT5 (จาก EA) กับ "เวลา MT5" ที่บอทคำนวณ ถ้าเพี้ยน (เช่น โบรกเปลี่ยน DST) เตือนครั้งเดียวต่อค่า"""
    global _feed_warned_off
    off = _feed["srv_off"]
    if off is None or not feed_fresh():
        return
    bot_off = (now_server() - pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))).total_seconds()
    if abs(bot_off - off) <= 300 or _feed_warned_off == off:
        return
    _feed_warned_off = off
    send_telegram_message(
        f"⚠️ <b>เวลา MT5 ไม่ตรงกับที่บอทคำนวณ</b>\n"
        f"MT5 = UTC{off / 3600:+g} ชม. / บอทใช้ UTC{round(bot_off / 3600, 2):+g} ชม.\n"
        f"ตั้ง <code>SERVER_UTC_OFFSET_HOURS={off / 3600:g}</code> ใน Render (โบรกแบบ NY close ค่านี้เปลี่ยนตาม DST ปีละ 2 ครั้ง)")


def check_market():
    global last_heartbeat_time, sheet_errors, data_errors, sheet_alerted, data_alerted, sheet_last_err

    market_open = is_market_open()
    heartbeat_due = time.time() - last_heartbeat_time >= HEARTBEAT_INTERVAL

    if FEED_ENABLED:
        try:
            feed_time_check()
        except Exception as e:
            print(f"Feed time check error: {e}")

    df_1h = None
    if market_open or heartbeat_due:
        try:
            df_1h = fetch_1h()
        except Exception as e:
            print(f"ดึงข้อมูลกราฟไม่สำเร็จ: {e}")

    if FEED_ENABLED and market_open:
        try:
            feed_source_notice()
        except Exception as e:
            print(f"Feed notice error: {e}")

    if heartbeat_due:
        send_heartbeat(df_1h)
        last_heartbeat_time = time.time()

    try:
        maybe_send_daily()
    except Exception as e:
        print(f"Daily summary error: {e}")
    try:
        maybe_send_weekly()
    except Exception as e:
        print(f"Weekly summary error: {e}")
    try:
        maybe_send_monthly()
    except Exception as e:
        print(f"Monthly summary error: {e}")
    try:
        maybe_send_yearly()
    except Exception as e:
        print(f"Yearly summary error: {e}")

    if not market_open:
        print("💤 ตลาดปิด ข้ามรอบนี้")
        return

    if df_1h is None or len(df_1h) < 25:
        data_errors += 1
        if data_errors >= ERROR_ALERT_AT and not data_alerted:
            send_telegram_message("⚠️ <b>คำเตือนระบบ</b>: บอทดึงข้อมูลกราฟไม่ได้ติดต่อกันหลายรอบ กรุณาตรวจสอบ"
                                  + ("\n📡 ไม่ได้รับราคาจาก EA ใน MT5 ตรวจว่า MT5 เปิดอยู่ EA ยังทำงาน และอนุญาต WebRequest แล้ว" if FEED_ENABLED else ""))
            data_alerted = True
        return
    if data_alerted:
        send_telegram_message("✅ ดึงข้อมูลกราฟกลับมาปกติแล้ว")
    data_errors, data_alerted = 0, False

    # แจ้งเตือน 4H ปิดแท่ง (ไม่ขึ้นกับ Google Sheets)
    try:
        maybe_send_h4_close(df_1h, to_4h(df_1h))
    except Exception as e:
        print(f"H4 close alert error: {e}")

    levels = get_levels_from_google_sheet()
    if not levels:
        sheet_errors += 1
        sheet_last_err = getattr(get_levels_from_google_sheet, "last_error", "")
        if sheet_errors >= ERROR_ALERT_AT and not sheet_alerted:
            send_telegram_message(
                "⚠️ <b>คำเตือนระบบ</b>: อ่าน Google Sheets ไม่ได้ติดต่อกันหลายรอบ\n"
                f"🔎 สาเหตุล่าสุด: {html.escape(sheet_last_err)}\nตรวจสอบลิงก์/สิทธิ์/ช่องที่กรอก"
            )
            sheet_alerted = True
        return
    if sheet_alerted:
        prev = f"\n🔎 ปัญหาที่เคยแจ้ง: {html.escape(sheet_last_err)}" if sheet_last_err else ""
        send_telegram_message("✅ <b>แก้ไขแล้ว</b> อ่าน Google Sheets กลับมาปกติ ช่องที่เคยมีปัญหาอ่านได้แล้ว" + prev)
    sheet_errors, sheet_alerted, sheet_last_err = 0, False, ""

    try:
        update_first_seen(levels)
    except Exception as e:
        print(f"First-seen error: {e}")

    try:
        monitor_trades(levels)
    except Exception as e:
        print(f"Trade monitor error: {e}")
    try:
        monitor_journal()
    except Exception as e:
        print(f"Journal monitor error: {e}")

    df_4h = to_4h(df_1h)
    k4 = calculate_stoch(df_4h)
    k4_now = float(k4.iloc[-1]) if len(k4) else None
    last_1h_start = df_1h.index[-1]

    for tf, df in (("1h", df_1h), ("4h", df_4h)):
        try:
            check_timeframe(tf, df, levels, last_1h_start, k4_now)
        except Exception as e:
            print(f"Error in TF {tf}: {e}")
        try:
            check_breaks(tf, df, levels, last_1h_start)
        except Exception as e:
            print(f"Break alert error TF {tf}: {e}")

    # 🧭 4H CHoCH / BOS + ย่อแตะโซน 50–61.8% (+ M1 ยืนยัน)
    try:
        check_choch_retrace(df_4h, levels, last_1h_start)
    except Exception as e:
        print(f"CHoCH alert error: {e}")
    try:
        check_ict_retrace(df_4h, levels, last_1h_start)
    except Exception as e:
        print(f"ICT alert error: {e}")

    # 🆕 SMC บน M15: เกิด CHoCH/BOS แล้วราคาย้อนชนโซน 50–61.8% = แจ้งทันที
    try:
        check_m15_retrace(levels)
    except Exception as e:
        print(f"M15 SMC alert error: {e}")

    try:
        push_removals()
    except Exception as e:
        print(f"Push removals error: {e}")

# ==========================================
# 🐶 Watchdog: ถ้าลูปหลักค้างเกิน WATCHDOG_SEC วินาที ให้ปิดโปรเซส Render จะสตาร์ทใหม่ให้เอง
# ==========================================
def watchdog():
    while True:
        time.sleep(30)
        if time.time() - last_cycle_time > WATCHDOG_SEC:
            print("🐶 watchdog: ลูปหลักค้าง กำลังรีสตาร์ท")
            send_telegram_message("⚠️ <b>บอทค้างเกินกำหนด กำลังรีสตาร์ทอัตโนมัติ</b>")
            os._exit(1)

# ==========================================
# 🚀 เริ่มโปรแกรม
# ==========================================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()

    print("🤖 บอท XAUUSD เริ่มทำงานแล้ว...")
    store = "☁️ Upstash" if REMOTE_ENABLED else "💾 ไฟล์ในเครื่อง (อาจหายตอน redeploy)"
    send_telegram_message(
        f"🚀 <b>บอท XAUUSD เริ่มทำงานแล้ว</b>\n🕒 เวลา MT5 ตอนนี้: <code>{fmt(now_server())}</code>\n"
        f"(ถ้าไม่ตรงกับนาฬิกาใน MT5 ให้ปรับ SERVER_UTC_OFFSET_HOURS)\n"
        f"🗂 ที่เก็บสถานะ: {store}\n"
        f"📡 แหล่งราคา: {'MT5 EA (POST /feed)' if FEED_ENABLED else 'Yahoo Finance'}"
    )

    while True:
        print(f"🔄 รอบตรวจ เวลา MT5 {fmt(now_server())}")
        try:
            check_market()
        except Exception as e:
            print(f"Unhandled error: {e}")
        last_cycle_time = time.time()
        time.sleep(CHECK_INTERVAL)
