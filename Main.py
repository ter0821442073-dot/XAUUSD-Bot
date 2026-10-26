import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import yfinance as yf

# ==========================================
# ⚙️ ตั้งค่า (อ่านจาก Environment Variable บน Render)
# ==========================================
TOKEN = os.environ["TG_TOKEN"]
CHAT_ID = os.environ["TG_CHAT_ID"]
SHEET_ID = os.environ["SHEET_ID"]

SYMBOL = os.environ.get("SYMBOL", "GC=F")
# ส่วนต่างราคา futures - spot (เช่น GC=F สูงกว่า MT5 อยู่ 12 จุด ใส่ 12)
PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "0"))

CHECK_INTERVAL = 120          # วินาที
HEARTBEAT_INTERVAL = 43200    # 12 ชม.
ERROR_ALERT_AT = 3
STATE_FILE = "alert_state.json"
LOCAL_TZ = timezone(timedelta(hours=7))  # เวลาไทย

# ==========================================
# 💾 สถานะ (เก็บลงไฟล์ กันส่งซ้ำเมื่อรีสตาร์ท)
# ==========================================
def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"1h": None, "4h": None}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(last_alert_bar, f)
    except Exception as e:
        print(f"⚠️ บันทึกสถานะไม่สำเร็จ: {e}")


last_alert_bar = load_state()
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

    def do_HEAD(self):  # UptimeRobot บางโหมดใช้ HEAD
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def run_web():
    port = int(os.environ.get("PORT", "10000"))
    HTTPServer(("0.0.0.0", port), SimpleHandler).serve_forever()

# ==========================================
# 📨 Telegram (HTML mode ปลอดภัยกว่า Markdown)
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

# ==========================================
# 📊 Google Sheets
# ==========================================
def get_levels_from_google_sheet():
    try:
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"
        df = pd.read_csv(url)
        return {
            "1h": {"res": float(df["1H_Res"].iloc[0]), "sup": float(df["1H_Sup"].iloc[0])},
            "4h": {"res": float(df["4H_Res"].iloc[0]), "sup": float(df["4H_Sup"].iloc[0])},
        }
    except Exception as e:
        print(f"⚠️ อ่าน Google Sheets ไม่สำเร็จ: {e}")
        return None

# ==========================================
# 📈 ข้อมูลราคา + Stochastic
# ==========================================
def fetch_1h():
    df = yf.download(SYMBOL, period="30d", interval="1h", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df[["Open", "High", "Low", "Close"]].dropna()


def to_4h(df_1h):
    return (
        df_1h.resample("4h")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
        .dropna()
    )


def calculate_stoch(df, k_period=14, smooth_k=3):
    low_min = df["Low"].rolling(k_period).min()
    high_max = df["High"].rolling(k_period).max()
    k_raw = 100 * (df["Close"] - low_min) / (high_max - low_min)
    return k_raw.rolling(smooth_k).mean()

# ==========================================
# 🕒 ตลาดเปิดหรือไม่ (ประมาณการเป็น UTC)
# ==========================================
def is_market_open():
    now = datetime.now(timezone.utc)
    wd, hr = now.weekday(), now.hour  # Mon=0 ... Sun=6
    if wd == 5:                 # เสาร์
        return False
    if wd == 4 and hr >= 22:    # ศุกร์หลังปิด
        return False
    if wd == 6 and hr < 22:     # อาทิตย์ก่อนเปิด
        return False
    return True

# ==========================================
# 🔔 ตรวจสัญญาณ (ใช้เฉพาะแท่งที่ปิดแล้ว)
# ==========================================
def check_timeframe(tf, df, levels):
    if len(df) < 25:
        return

    stoch_k = calculate_stoch(df)

    # iloc[-1] = แท่งที่ยังวิ่งอยู่ (ไม่ใช้) / -2 = แท่งปิดล่าสุด / -3 = แท่งก่อนหน้า
    cur, prev = df.iloc[-2], df.iloc[-3]
    k_curr, k_prev = float(stoch_k.iloc[-2]), float(stoch_k.iloc[-3])
    if pd.isna(k_curr) or pd.isna(k_prev):
        return

    bar_time = df.index[-2]
    bar_id = str(bar_time)
    bar_label = bar_time.tz_convert(LOCAL_TZ).strftime("%d/%m %H:%M") if bar_time.tzinfo else str(bar_time)

    if last_alert_bar.get(tf) == bar_id:
        return

    res = levels[tf]["res"] + PRICE_OFFSET
    sup = levels[tf]["sup"] + PRICE_OFFSET

    c_open, c_high, c_low, c_close = (float(cur[x]) for x in ("Open", "High", "Low", "Close"))
    p_open, p_close = float(prev["Open"]), float(prev["Close"])

    prev_green, prev_red = p_close > p_open, p_close < p_open
    cur_green, cur_red = c_close > c_open, c_close < c_open

    print(f"[{tf.upper()}] แท่ง {bar_label} H:{c_high:.2f} L:{c_low:.2f} C:{c_close:.2f} | %K {k_curr:.1f} (ก่อนหน้า {k_prev:.1f})")

    header_tail = f"\n📊 XAUUSD ({SYMBOL}) | 🕒 แท่ง {bar_label} (เวลาไทย)\n"
    footer = "\n📱 <b>เปิด MT5 เช็กกราฟก่อนตัดสินใจ</b>"

    msg = None

    # --- SELL ---
    if c_high >= res and k_curr >= 80 and prev_green and cur_red:
        msg = (
            f"🚨 <b>สัญญาณ SELL (แท่งคู่ + แนวต้าน) - TF {tf.upper()}</b>{header_tail}"
            f"📈 High: <code>{c_high:.2f}</code> ชนแนวต้าน <code>{res:.2f}</code>\n"
            f"💵 ราคาปิดแท่ง: <code>{c_close:.2f}</code> (ห่างแนวต้าน {res - c_close:.2f})\n"
            f"📉 แท่งคู่: เขียว ➡️ แดง | Stoch %K: <code>{k_curr:.2f}</code>\n"
            f"🛑 SL อ้างอิง: เหนือ High <code>{c_high:.2f}</code>{footer}"
        )
    elif tf == "4h" and k_prev >= 80 and k_curr < 80:
        msg = (
            f"🚨 <b>สัญญาณ SELL (Stoch ตัดกลับ) - TF 4H</b>{header_tail}"
            f"📉 %K: <code>{k_prev:.1f}</code> ➡️ <code>{k_curr:.1f}</code> (&lt; 80)\n"
            f"💵 ราคาปิดแท่ง: <code>{c_close:.2f}</code>{footer}"
        )
    # --- BUY ---
    elif c_low <= sup and k_curr <= 20 and prev_red and cur_green:
        msg = (
            f"🟢 <b>สัญญาณ BUY (แท่งคู่ + แนวรับ) - TF {tf.upper()}</b>{header_tail}"
            f"📉 Low: <code>{c_low:.2f}</code> ชนแนวรับ <code>{sup:.2f}</code>\n"
            f"💵 ราคาปิดแท่ง: <code>{c_close:.2f}</code> (ห่างแนวรับ {c_close - sup:.2f})\n"
            f"📈 แท่งคู่: แดง ➡️ เขียว | Stoch %K: <code>{k_curr:.2f}</code>\n"
            f"🛑 SL อ้างอิง: ใต้ Low <code>{c_low:.2f}</code>{footer}"
        )
    elif tf == "4h" and k_prev <= 20 and k_curr > 20:
        msg = (
            f"🟢 <b>สัญญาณ BUY (Stoch ตัดกลับ) - TF 4H</b>{header_tail}"
            f"📈 %K: <code>{k_prev:.1f}</code> ➡️ <code>{k_curr:.1f}</code> (&gt; 20)\n"
            f"💵 ราคาปิดแท่ง: <code>{c_close:.2f}</code>{footer}"
        )

    # บันทึกว่าแท่งนี้เช็กแล้วเฉพาะเมื่อส่งสำเร็จ (ถ้าส่งไม่ผ่านจะลองใหม่รอบหน้า)
    if msg and send_telegram_message(msg):
        last_alert_bar[tf] = bar_id
        save_state()

# ==========================================
# 🔍 วนตรวจตลาด
# ==========================================
def send_heartbeat(df_1h):
    price = float(df_1h["Close"].iloc[-1]) if df_1h is not None else None
    price_txt = f"{price:.2f}" if price else "ไม่ทราบ"
    send_telegram_message(
        f"🤖 <b>บอทรันปกติ (Heartbeat)</b>\n"
        f"📊 XAUUSD ({SYMBOL})\n"
        f"💰 ราคาล่าสุด: <code>{price_txt}</code>\n"
        f"🟢 ระบบสแตนด์บายเฝ้าจอ"
    )


def check_market():
    global last_heartbeat_time, sheet_errors, data_errors, sheet_alerted, data_alerted

    market_open = is_market_open()

    df_1h = None
    if market_open or time.time() - last_heartbeat_time >= HEARTBEAT_INTERVAL:
        try:
            df_1h = fetch_1h()
        except Exception as e:
            print(f"ดึงข้อมูลกราฟไม่สำเร็จ: {e}")

    # Heartbeat
    if time.time() - last_heartbeat_time >= HEARTBEAT_INTERVAL:
        send_heartbeat(df_1h)
        last_heartbeat_time = time.time()

    if not market_open:
        print("💤 ตลาดปิด ข้ามรอบนี้")
        return

    # กราฟ
    if df_1h is None or len(df_1h) < 25:
        data_errors += 1
        if data_errors >= ERROR_ALERT_AT and not data_alerted:
            send_telegram_message("⚠️ <b>คำเตือนระบบ</b>: บอทดึงข้อมูลกราฟไม่ได้ติดต่อกันหลายรอบ กรุณาตรวจสอบ")
            data_alerted = True
        return
    if data_alerted:
        send_telegram_message("✅ ดึงข้อมูลกราฟกลับมาปกติแล้ว")
    data_errors, data_alerted = 0, False

    # Google Sheets
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

    # ตรวจ 1H และ 4H จากข้อมูลชุดเดียวกัน
    for tf, df in (("1h", df_1h), ("4h", to_4h(df_1h))):
        try:
            check_timeframe(tf, df, levels)
        except Exception as e:
            print(f"Error in TF {tf}: {e}")

# ==========================================
# 🚀 เริ่มโปรแกรม
# ==========================================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()

    print("🤖 บอท XAUUSD เริ่มทำงานแล้ว...")
    send_telegram_message("🚀 <b>บอท XAUUSD เริ่มทำงานแล้ว</b>")

    while True:
        try:
            check_market()
        except Exception as e:
            print(f"Unhandled error: {e}")
        time.sleep(CHECK_INTERVAL)
