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

# เวลา MT5 (โบรกแบบ NY close) = เวลานิวยอร์ก + 7 ชม.
NY_TZ = "America/New_York"
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

# 6) ช่วงเสี่ยง (เวลา MT5): รอยต่อวัน 23:00-01:00 + เวลาข่าวจากคอลัมน์ "News" ในชีต
ROLLOVER_START = int(os.environ.get("ROLLOVER_START", "23"))
ROLLOVER_END = int(os.environ.get("ROLLOVER_END", "1"))
NEWS_BEFORE_MIN = int(os.environ.get("NEWS_BEFORE_MIN", "30"))
NEWS_AFTER_MIN = int(os.environ.get("NEWS_AFTER_MIN", "30"))
SKIP_RISK_WINDOWS = os.environ.get("SKIP_RISK_WINDOWS", "0") == "1"  # 1 = ไม่ส่งสัญญาณช่วงเสี่ยง

CHECK_INTERVAL = 60
CLOSE_GRACE_SEC = 60
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
        return True
    return False


def cooldown_ok(tf, side, bar_start, delta):
    last = sent.get(f"{tf}:cd_{side}")
    if not last:
        return True
    try:
        gap = (bar_start - pd.Timestamp(last)) / delta
    except Exception:
        return True
    return gap >= COOLDOWN_BARS[tf]


def mark_cooldown(tf, side, bar_start):
    opp = "buy" if side == "sell" else "sell"
    sent[f"{tf}:cd_{side}"] = str(bar_start)
    sent.pop(f"{tf}:cd_{opp}", None)  # เปลี่ยนทิศ = ปลดล็อกฝั่งตรงข้าม
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


def get_levels_from_google_sheet():
    try:
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"
        df = pd.read_csv(url)
        return {
            "1h": {"res": float(df["1H_Res"].iloc[0]), "sup": float(df["1H_Sup"].iloc[0])},
            "4h": {"res": float(df["4H_Res"].iloc[0]), "sup": float(df["4H_Sup"].iloc[0])},
            "news": parse_news(df),
        }
    except Exception as e:
        print(f"⚠️ อ่าน Google Sheets ไม่สำเร็จ: {e}")
        return None

# ==========================================
# 🕒 เวลา MT5
# ==========================================
def now_server():
    ny = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ)).replace(tzinfo=None)
    return pd.Timestamp(ny + timedelta(hours=SERVER_SHIFT_HOURS))


def fmt(ts):
    return ts.strftime("%d/%m %H:%M")


def px(v):
    """แปลงราคา GC=F เป็นระดับราคา MT5 สำหรับแสดงผล"""
    return v - PRICE_OFFSET

# ==========================================
# 📈 ข้อมูลราคา + อินดิเคเตอร์
# ==========================================
def fetch_1h():
    df = yf.download(SYMBOL, period="30d", interval="1h", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()

    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    df.index = idx.tz_convert(NY_TZ).tz_localize(None) + pd.Timedelta(hours=SERVER_SHIFT_HOURS)
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
        sl, reward, tp_ok = entry + risk, entry - target, target < entry
        tp_name = "แนวรับ"
    else:
        sl, reward, tp_ok = entry - risk, target - entry, target > entry
        tp_name = "แนวต้าน"
    text = f"🛑 SL ({ATR_MULT:g}×ATR {atr_val:.2f}): <code>{px(sl):.2f}</code>"
    if tp_ok and risk > 0:
        text += f"\n🎯 เป้า ({tp_name}): <code>{px(target):.2f}</code> | R:R 1:{reward / risk:.1f}"
    return text


def risk_notes(news_times):
    notes = []
    now = now_server()
    if now.hour >= ROLLOVER_START or now.hour < ROLLOVER_END:
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
    now = datetime.now(timezone.utc)
    wd, hr = now.weekday(), now.hour
    if wd == 5:
        return False
    if wd == 4 and hr >= 22:
        return False
    if wd == 6 and hr < 22:
        return False
    return True

# ==========================================
# 🔔 ตรวจสัญญาณ
# ==========================================
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

    res = levels[tf]["res"] + PRICE_OFFSET
    sup = levels[tf]["sup"] + PRICE_OFFSET
    tf_u = tf.upper()
    atr_val = float(atr.iloc[cl_i])
    risk = risk_notes(levels.get("news", []))

    print(f"[{tf_u}] แนวต้าน {res:.2f} | แนวรับ {sup:.2f} (ระดับ GC=F รวม offset {PRICE_OFFSET:g}) | แท่งสด={is_live}")

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
                touched = l_high >= res
                stoch_ok = (not need_stoch) or (not pd.isna(k_live) and k_live >= 80)
            else:
                touched = l_low <= sup
                stoch_ok = (not need_stoch) or (not pd.isna(k_live) and k_live <= 20)
            if not (touched and stoch_ok):
                continue
            if not cooldown_ok(tf, side, df.index[live_i], delta):
                continue
            zone_txt, zone_ok = zone_label(side, k4_now) if tf == "1h" else ("", True)
            if H4_ZONE_FILTER and not zone_ok:
                continue

            if side == "sell":
                title = f"⚡ <b>เตือนทันที SELL (ชนแนวต้าน{stoch_part}) - TF {tf_u}</b>"
                lines = [
                    f"📈 High: <code>{px(l_high):.2f}</code> ชนแนวต้าน <code>{px(res):.2f}</code>",
                    f"💵 ราคาตอนนี้: <code>{px(l_close):.2f}</code> | Stoch %K: <code>{k_txt}</code>",
                ]
                target = sup
            else:
                title = f"⚡ <b>เตือนทันที BUY (ชนแนวรับ{stoch_part}) - TF {tf_u}</b>"
                lines = [
                    f"📉 Low: <code>{px(l_low):.2f}</code> ชนแนวรับ <code>{px(sup):.2f}</code>",
                    f"💵 ราคาตอนนี้: <code>{px(l_close):.2f}</code> | Stoch %K: <code>{k_txt}</code>",
                ]
                target = res

            msg = compose(title, llabel, lines, side, l_close, atr_val, target, zone_txt, risk, note)
            if fire(f"{tf}:live_{side}", lid, msg):
                mark_cooldown(tf, side, df.index[live_i])

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

    # ถ้าเคยเตือนทันที (1H) ในแท่งนี้แล้ว ถือว่าชนแนว + Stoch เกิดขึ้นแล้ว
    sell_pre_alerted = tf == "1h" and sent.get("1h:live_sell") == cid
    buy_pre_alerted = tf == "1h" and sent.get("1h:live_buy") == cid

    sell_pat = reversal_pattern("sell", p_open, p_close, c_open, c_high, c_low, c_close)
    buy_pat = reversal_pattern("buy", p_open, p_close, c_open, c_high, c_low, c_close)

    z_sell_txt, z_sell_ok = zone_label("sell", k4_now) if tf == "1h" else ("", True)
    z_buy_txt, z_buy_ok = zone_label("buy", k4_now) if tf == "1h" else ("", True)
    sell_zone_ok = z_sell_ok or not H4_ZONE_FILTER
    buy_zone_ok = z_buy_ok or not H4_ZONE_FILTER

    print(f"[{tf_u}] ปิด {clabel} H:{c_high:.2f} L:{c_low:.2f} C:{c_close:.2f} | %K {k_c:.1f} (ก่อนหน้า {k_p:.1f}) | ATR {atr_val:.2f}")

    if c_high >= res and (k_c >= 80 or sell_pre_alerted) and sell_pat and sell_zone_ok:
        lines = [
            f"📈 High: <code>{px(c_high):.2f}</code> ชนแนวต้าน <code>{px(res):.2f}</code>",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code> (ห่างแนวต้าน {res - c_close:.2f})",
            f"📉 แท่งคู่: เขียว ➡️ แดง ({sell_pat}) | Stoch %K: <code>{k_c:.2f}</code>",
        ]
        fire(f"{tf}:close_sell", cid,
             compose(f"🚨 <b>ยืนยัน SELL (ชนแนวต้าน + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>",
                     clabel, lines, "sell", c_close, atr_val, sup, z_sell_txt, risk))
    elif tf == "4h" and k_p >= 80 and k_c < 80:
        lines = [
            f"📉 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&lt; 80)",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code>",
        ]
        fire("4h:cross_sell", cid,
             compose("🚨 <b>สัญญาณ SELL (Stoch ตัดกลับ) - TF 4H</b>",
                     clabel, lines, "sell", c_close, atr_val, sup, "", risk))
    elif c_low <= sup and (k_c <= 20 or buy_pre_alerted) and buy_pat and buy_zone_ok:
        lines = [
            f"📉 Low: <code>{px(c_low):.2f}</code> ชนแนวรับ <code>{px(sup):.2f}</code>",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code> (ห่างแนวรับ {c_close - sup:.2f})",
            f"📈 แท่งคู่: แดง ➡️ เขียว ({buy_pat}) | Stoch %K: <code>{k_c:.2f}</code>",
        ]
        fire(f"{tf}:close_buy", cid,
             compose(f"🟢 <b>ยืนยัน BUY (ชนแนวรับ + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>",
                     clabel, lines, "buy", c_close, atr_val, res, z_buy_txt, risk))
    elif tf == "4h" and k_p <= 20 and k_c > 20:
        lines = [
            f"📈 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&gt; 20)",
            f"💵 ราคาปิด: <code>{px(c_close):.2f}</code>",
        ]
        fire("4h:cross_buy", cid,
             compose("🟢 <b>สัญญาณ BUY (Stoch ตัดกลับ) - TF 4H</b>",
                     clabel, lines, "buy", c_close, atr_val, res, "", risk))

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
# 🚀 เริ่มโปรแกรม
# ==========================================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()

    print("🤖 บอท XAUUSD เริ่มทำงานแล้ว...")
    send_telegram_message(
        f"🚀 <b>บอท XAUUSD เริ่มทำงานแล้ว</b>\n🕒 เวลา MT5 ตอนนี้: <code>{fmt(now_server())}</code>\n"
        f"(ถ้าไม่ตรงกับนาฬิกาใน MT5 ให้ปรับ SERVER_SHIFT_HOURS)"
    )

    while True:
        try:
            check_market()
        except Exception as e:
            print(f"Unhandled error: {e}")
        time.sleep(CHECK_INTERVAL)
