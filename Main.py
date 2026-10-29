import io
import os
import sys
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from http.server import HTTPServer, BaseHTTPRequestHandler

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

SYMBOL = os.environ.get("SYMBOL", "GC=F")
# ส่วนต่างราคา futures - spot (GC=F สูงกว่า MT5 12 จุด ใส่ 12) ข้อความแจ้งเตือนจะแสดงเป็นราคาระดับ MT5
PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "0"))

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

# ==========================================
# 💾 สถานะ
# ==========================================
def load_state():
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


sent = load_state()  # "1h:live_sell" -> id แท่งที่แจ้งแล้ว / "1h:cd_sell" -> แท่งที่เตือนทันทีล่าสุด
last_heartbeat_time = time.time()
last_cycle_time = time.time()
WATCHDOG_SEC = int(os.environ.get("WATCHDOG_SEC", "600"))
sheet_errors = 0
data_errors = 0
sheet_alerted = False
data_alerted = False

# ==========================================
# 🌐 เว็บเซิร์ฟเวอร์จำลองให้ Render ผ่านพอร์ต
# ==========================================
class SimpleHandler(BaseHTTPRequestHandler):
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
    HTTPServer(("0.0.0.0", port), SimpleHandler).serve_forever()

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
    vals = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").dropna()
    if vals.empty:
        raise ValueError(f"คอลัมน์ {col} ไม่มีตัวเลข")
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
        except Exception as e:
            print(f"⚠️ อ่านออเดอร์จากชีตไม่สำเร็จ: {e}")
            trades = []
        problems = list(getattr(parse_trades, "last_problems", []))
        return {
            "1h": {"res": read_levels(df, "1H_Res"), "sup": read_levels(df, "1H_Sup")},
            "4h": {"res": read_levels(df, "4H_Res"), "sup": read_levels(df, "4H_Sup")},
            "news": parse_news(df),
            "trades": trades,
            "problems": problems,
        }
    except Exception as e:
        print(f"⚠️ อ่าน Google Sheets ไม่สำเร็จ: {e}")
        return None

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
def fetch_1h():
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
                hit = [r for r in res_list if l_high >= r]
                level = max(hit) if hit else None   # แนวต้านสูงสุดที่ชนแล้ว
                stoch_ok = (not need_stoch) or (not pd.isna(k_live) and k_live >= 80)
            else:
                hit = [s for s in sup_list if l_low <= s]
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

    sell_hit = [r for r in res_list if c_high >= r]
    buy_hit = [s for s in sup_list if c_low <= s]
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
# 🧾 ออเดอร์จากชีต + สรุปผลรายสัปดาห์
# คอลัมน์ในชีต: Time, Side, Entry, SL, TP (บังคับ) | Exit, ExitTime (ไม่บังคับ ใช้เมื่อปิดมือ/ขยับ SL)
# ราคาในชีตเป็นระดับ MT5, เวลาเป็นเวลา MT5 รูปแบบ 2026-10-27 14:30
# ==========================================
BE_R = float(os.environ.get("BE_R", "0.05"))                       # |R| ไม่เกินค่านี้ นับเป็นเสมอ
BE_PTS = float(os.environ.get("BE_PTS", "0.5"))                       # ไม้ที่ไม่มี SL เดิม: |กำไร| ไม่เกินกี่จุด นับเป็นเสมอ
MONITOR_DAYS = 4                                                   # เฝ้าไม้ที่เปิดไม่เกินกี่วัน
TRADE_ALERT_WINDOW_MIN = int(os.environ.get("TRADE_ALERT_WINDOW_MIN", "20"))
WEEKLY_FORCE = os.environ.get("WEEKLY_FORCE", "0") == "1"          # ทดสอบ: ส่งสรุปทันที 1 ครั้งต่อการรัน
_weekly_force_done = False
# แจ้งผลเมื่อกรอก Exit (ปิดมือ) ถ้าเวลาปิดอยู่ภายในกี่นาทีที่ผ่านมา (กันส่งซ้ำของเก่าเมื่อบอทรีสตาร์ท)
MANUAL_ALERT_WINDOW_MIN = int(os.environ.get("MANUAL_ALERT_WINDOW_MIN", "120"))
# เวลาในคอลัมน์ Time/ExitTime ของชีต: ค่าว่าง = เวลา MT5 / ตั้ง 7 = เวลาไทย (UTC+7)
_sheet_off = os.environ.get("SHEET_TIME_UTC_OFFSET_HOURS", "").strip()
SHEET_TIME_UTC_OFFSET_HOURS = float(_sheet_off) if _sheet_off != "" else None
_bars5_cache = {"ts": 0.0, "period": None, "df": None}


def fetch_5m(days):
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
    if not all(c in df.columns for c in need):
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
    return {
        "n": n, "w": w, "l": l, "be": n - w - l,
        "wr": (w / n * 100) if n else 0.0,
        "n_r": len(rs), "sum_r": sum_r,
        "avg_r": (sum_r / len(rs)) if rs else 0.0,
        "n_pl": len(pl), "planned": (sum(pl) / len(pl)) if pl else 0.0,
        "sum_pts": sum_pts,
        "avg_pts": (sum_pts / n) if n else 0.0,
    }


def r_text(s):
    """ข้อความ R เฉลี่ย/รวม (นับเฉพาะไม้ที่มี SL เดิม)"""
    if s["n_r"] == 0:
        return "R: ไม่มีข้อมูล (ไม้ที่ขยับ SL ใส่ SL0 ในชีตจึงจะคิด R ได้)"
    txt = f"เฉลี่ย {s['avg_r']:+.2f}R/ไม้ | รวม {s['sum_r']:+.2f}R"
    if s["n_r"] < s["n"]:
        txt += f" (คิดจาก {s['n_r']}/{s['n']} ไม้)"
    return txt


def pnl_label(pts):
    """กำไร/ติดลบสุทธิเป็นจุดราคา (ราคาทอง 1 ดอลลาร์ = 1 จุดราคา)"""
    if pts > 0.005:
        return f"🟢 กำไร {pts:+.2f} จุดราคา"
    if pts < -0.005:
        return f"🔴 ติดลบ {pts:+.2f} จุดราคา"
    return "⚪ เสมอตัว 0.00 จุดราคา"


def build_weekly_msg(start_s, end_s, week_res, all_res, skipped=0):
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
    else:
        lines.append("📌 สัปดาห์นี้ไม่มีไม้ที่ปิด")
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
    diff = r["pts"]
    icon, word = {"win": ("✅", "ชนะ"), "loss": ("❌", "แพ้"), "be": ("➖", "เสมอ")}[r["result"]]
    return "\n".join([
        f"{icon} <b>ปิดไม้เอง - {side_u} ({word})</b>",
        f"🕒 เข้า {fmt(t['time'])} → ปิด {fmt(r['close_time'])} (เวลา MT5)",
        f"เข้า <code>{t['entry']:.2f}</code> | ปิด <code>{t['exit']:.2f}</code>",
        f"SL <code>{t['sl']:.2f}</code> | TP <code>{t['tp']:.2f}</code>",
        f"ผล <b>{r_part(r)}</b> ({diff:+.2f} จุดราคา)",
    ])


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


def monitor_trades(levels):
    """แจ้งผลไม้ที่เพิ่งปิด (TP/SL ชน หรือกรอก Exit) พร้อมสถิติสะสมของวันนั้น"""
    now = now_server()

    for row, reason in levels.get("problems", []):
        fire(f"bad:{row}", reason,
             f"⚠️ <b>ออเดอร์ในชีตแถว {row} ไม่ถูกนับ</b>\n{reason}\nแก้ในชีตแล้วบอทจะนับให้เอง")
    for t in levels.get("trades", []):
        if t["exit"] is None and t["time"] > now + timedelta(minutes=5):
            fire(f"future:{t['id']}", "1",
                 f"⚠️ <b>ออเดอร์ในชีตแถว {t['row']}</b> มีเวลาเข้าไม้ {fmt(t['time'])} (เวลา MT5) อยู่ในอนาคต\n"
                 f"บอทจึงยังไม่นับผล เช็กวันที่/เวลาที่กรอก")

    pairs, _ = collect_results(levels.get("trades", []))
    all_r = [r for _, r in pairs]
    for t, r in pairs:
        if r["status"] != "closed":
            continue
        age = now - r["close_time"]
        if r["how"] == "manual":
            if age > timedelta(minutes=MANUAL_ALERT_WINDOW_MIN):
                continue
            msg, val = manual_alert_msg(t, r), f"manual:{t['exit']:g}"
        else:
            if age > timedelta(minutes=TRADE_ALERT_WINDOW_MIN):
                continue
            msg, val = trade_alert_msg(t, r), r["how"]
        fire(f"trade:{t['id']}", val, msg + tally_text(all_r, r["close_time"]))


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

    if not trades:
        print("ไม่มีออเดอร์ในชีต ข้ามสรุปรายสัปดาห์")
        mark_done()
        return

    replay = [t for t in trades if t["exit"] is None]
    bars = None
    if replay:
        days = (now_server() - min(t["time"] for t in replay)).days + 2
        bars = fetch_5m(days)
        if bars is None:
            print("ดึงข้อมูล 5 นาทีไม่ได้ ลองใหม่รอบหน้า")
            return

    results = [evaluate_trade(t, bars) for t in trades]
    start_s, end_s = utc_to_server(start_utc), utc_to_server(end_utc)
    week_res = [r for r in results if r["status"] == "closed" and start_s < r["close_time"] <= end_s]
    if send_telegram_message(build_weekly_msg(start_s, end_s, week_res, results,
                                                      skipped=len(parse_trades.last_problems))):
        mark_done()
        print("✅ ส่งสรุปรายสัปดาห์แล้ว")

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


def check_market():
    global last_heartbeat_time, sheet_errors, data_errors, sheet_alerted, data_alerted

    market_open = is_market_open()
    heartbeat_due = time.time() - last_heartbeat_time >= HEARTBEAT_INTERVAL

    df_1h = None
    if market_open or heartbeat_due:
        try:
            df_1h = fetch_1h()
        except Exception as e:
            print(f"ดึงข้อมูลกราฟไม่สำเร็จ: {e}")

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

    if not market_open:
        print("💤 ตลาดปิด ข้ามรอบนี้")
        return

    if df_1h is None or len(df_1h) < 25:
        data_errors += 1
        if data_errors >= ERROR_ALERT_AT and not data_alerted:
            send_telegram_message("⚠️ <b>คำเตือนระบบ</b>: บอทดึงข้อมูลกราฟไม่ได้ติดต่อกันหลายรอบ กรุณาตรวจสอบ")
            data_alerted = True
        return
    if data_alerted:
        send_telegram_message("✅ ดึงข้อมูลกราฟกลับมาปกติแล้ว")
    data_errors, data_alerted = 0, False

    levels = get_levels_from_google_sheet()
    if not levels:
        sheet_errors += 1
        if sheet_errors >= ERROR_ALERT_AT and not sheet_alerted:
            send_telegram_message("⚠️ <b>คำเตือนระบบ</b>: อ่าน Google Sheets ไม่ได้ติดต่อกันหลายรอบ ตรวจสอบลิงก์/สิทธิ์การเข้าถึง")
            sheet_alerted = True
        return
    if sheet_alerted:
        send_telegram_message("✅ อ่าน Google Sheets กลับมาปกติแล้ว")
    sheet_errors, sheet_alerted = 0, False

    try:
        monitor_trades(levels)
    except Exception as e:
        print(f"Trade monitor error: {e}")

    df_4h = to_4h(df_1h)
    k4 = calculate_stoch(df_4h)
    k4_now = float(k4.iloc[-1]) if len(k4) else None
    last_1h_start = df_1h.index[-1]

    for tf, df in (("1h", df_1h), ("4h", df_4h)):
        try:
            check_timeframe(tf, df, levels, last_1h_start, k4_now)
        except Exception as e:
            print(f"Error in TF {tf}: {e}")

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
    send_telegram_message(
        f"🚀 <b>บอท XAUUSD เริ่มทำงานแล้ว</b>\n🕒 เวลา MT5 ตอนนี้: <code>{fmt(now_server())}</code>\n"
        f"(ถ้าไม่ตรงกับนาฬิกาใน MT5 ให้ปรับ SERVER_UTC_OFFSET_HOURS)"
    )

    while True:
        print(f"🔄 รอบตรวจ เวลา MT5 {fmt(now_server())}")
        try:
            check_market()
        except Exception as e:
            print(f"Unhandled error: {e}")
        last_cycle_time = time.time()
        time.sleep(CHECK_INTERVAL)
