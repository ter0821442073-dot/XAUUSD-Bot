import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import yfinance as yf

# ==========================================
# ⚙️ ตั้งค่า (ค่าลับอ่านจาก Environment Variable บน Render)
# ==========================================
TOKEN = os.environ["TG_TOKEN"]
CHAT_ID = os.environ["TG_CHAT_ID"]
SHEET_ID = os.environ["SHEET_ID"]

SYMBOL = os.environ.get("SYMBOL", "GC=F")
# ส่วนต่างราคา futures - spot (เช่น GC=F สูงกว่า MT5 12 จุด ใส่ 12)
PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "0"))

# เวลา MT5 (โบรกแบบ NY close) = เวลานิวยอร์ก + 7 ชม. (GMT+3 ช่วงร้อน / GMT+2 ช่วงหนาว สลับตาม DST สหรัฐอัตโนมัติ)
NY_TZ = "America/New_York"
SERVER_SHIFT_HOURS = int(os.environ.get("SERVER_SHIFT_HOURS", "7"))
# เลื่อนจุดเริ่มแท่ง 4H (0 = แท่งเริ่ม 00:00, 04:00, 08:00 ... เวลา MT5 / 1 = 01:00, 05:00, 09:00 ...)
H4_OFFSET_HOURS = int(os.environ.get("H4_OFFSET_HOURS", "0"))
# 4H: แจ้งเตือนทันทีเมื่อชนแนว (ตั้งเป็น 1 ถ้าอยากให้ต้อง Stoch 80/20 ด้วย)
H4_LIVE_REQUIRE_STOCH = os.environ.get("H4_LIVE_REQUIRE_STOCH", "0") == "1"

# พารามิเตอร์ Stochastic ให้ตรงกับที่ตั้งใน MT5 (K period, Slowing)
STOCH_K = int(os.environ.get("STOCH_K", "14"))
STOCH_SLOW = int(os.environ.get("STOCH_SLOW", "3"))

CHECK_INTERVAL = 60           # วินาที
CLOSE_GRACE_SEC = 60          # รอหลังแท่งปิด ให้ข้อมูลล่าสุดมาครบก่อนสรุป
HEARTBEAT_INTERVAL = 43200    # 12 ชม.
ERROR_ALERT_AT = 3
STATE_FILE = "alert_state.json"

# ==========================================
# 💾 สถานะ (เก็บลงไฟล์ กันส่งซ้ำเมื่อรีสตาร์ท)
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


sent = load_state()  # key เช่น "1h:live_sell" -> id ของแท่งที่แจ้งไปแล้ว
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
    """ส่งแจ้งเตือนครั้งเดียวต่อแท่งต่อ key"""
    if sent.get(key) == bar_id:
        return
    if send_telegram_message(msg):
        sent[key] = bar_id
        save_state()

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
# 🕒 เวลา MT5 (server time, ไม่มี timezone)
# ==========================================
def now_server():
    ny = datetime.now(timezone.utc).astimezone(ZoneInfo(NY_TZ)).replace(tzinfo=None)
    return pd.Timestamp(ny + timedelta(hours=SERVER_SHIFT_HOURS))


def fmt(ts):
    return ts.strftime("%d/%m %H:%M")

# ==========================================
# 📈 ข้อมูลราคา + Stochastic
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
    # แปลงเป็นเวลานิวยอร์ก แล้วบวก 7 ชม. = เวลา MT5
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


def ohlc(df, i):
    r = df.iloc[i]
    return float(r["Open"]), float(r["High"]), float(r["Low"]), float(r["Close"])

# ==========================================
# 🕒 ตลาดเปิดหรือไม่ (ประมาณการเป็น UTC)
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
def check_timeframe(tf, df, levels, last_1h_start):
    if len(df) < 25:
        return

    delta = pd.Timedelta(hours=1 if tf == "1h" else 4)
    stoch_k = calculate_stoch(df)
    now = now_server()

    last_start = df.index[-1]
    last_end = last_start + delta
    # แท่งสุดท้ายยังวิ่งอยู่หรือไม่ (รวมช่วงรอข้อมูลครบหลังปิด)
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
    footer = "\n📱 <b>เปิด MT5 เช็กกราฟก่อนตัดสินใจ</b>"

    # ---------- แท่งที่กำลังวิ่ง: แจ้งเตือนทันที ----------
    if live_i is not None:
        _, l_high, l_low, l_close = ohlc(df, live_i)
        k_live = float(stoch_k.iloc[live_i])
        lid = str(df.index[live_i])
        llabel = fmt(df.index[live_i])
        need_stoch = (tf == "1h") or H4_LIVE_REQUIRE_STOCH
        k_txt = "-" if pd.isna(k_live) else f"{k_live:.1f}"

        if need_stoch and pd.isna(k_live):
            pass
        else:
            stoch_part = " + Stoch" if need_stoch else ""
            note = "\n⏳ แท่งยังไม่ปิด รอสัญญาณยืนยันตอนปิดแท่ง" if tf == "1h" else "\n⏳ แท่งยังไม่ปิด"

            if l_high >= res and (not need_stoch or k_live >= 80):
                fire(f"{tf}:live_sell", lid,
                     f"⚡ <b>เตือนทันที SELL (ชนแนวต้าน{stoch_part}) - TF {tf_u}</b>\n"
                     f"📊 XAUUSD ({SYMBOL}) | 🕒 แท่ง {llabel} (เวลา MT5)\n"
                     f"📈 High: <code>{l_high:.2f}</code> ชนแนวต้าน <code>{res:.2f}</code>\n"
                     f"💵 ราคาตอนนี้: <code>{l_close:.2f}</code> | Stoch %K: <code>{k_txt}</code>"
                     f"{note}{footer}")

            if l_low <= sup and (not need_stoch or k_live <= 20):
                fire(f"{tf}:live_buy", lid,
                     f"⚡ <b>เตือนทันที BUY (ชนแนวรับ{stoch_part}) - TF {tf_u}</b>\n"
                     f"📊 XAUUSD ({SYMBOL}) | 🕒 แท่ง {llabel} (เวลา MT5)\n"
                     f"📉 Low: <code>{l_low:.2f}</code> ชนแนวรับ <code>{sup:.2f}</code>\n"
                     f"💵 ราคาตอนนี้: <code>{l_close:.2f}</code> | Stoch %K: <code>{k_txt}</code>"
                     f"{note}{footer}")

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
    head = f"📊 XAUUSD ({SYMBOL}) | 🕒 แท่ง {clabel} (เวลา MT5)\n"

    prev_green, prev_red = p_close > p_open, p_close < p_open
    cur_green, cur_red = c_close > c_open, c_close < c_open

    # 1H: ถ้าเคยเตือนทันทีในแท่งนี้แล้ว ถือว่าเงื่อนไขชนแนว + Stoch เกิดขึ้นแล้ว
    sell_pre_alerted = tf == "1h" and sent.get("1h:live_sell") == cid
    buy_pre_alerted = tf == "1h" and sent.get("1h:live_buy") == cid

    print(f"[{tf_u}] ปิด {clabel} H:{c_high:.2f} L:{c_low:.2f} C:{c_close:.2f} | %K {k_c:.1f} (ก่อนหน้า {k_p:.1f}) | live={is_live}")

    if c_high >= res and (k_c >= 80 or sell_pre_alerted) and prev_green and cur_red:
        fire(f"{tf}:close_sell", cid,
             f"🚨 <b>ยืนยัน SELL (ชนแนวต้าน + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>\n{head}"
             f"📈 High: <code>{c_high:.2f}</code> ชนแนวต้าน <code>{res:.2f}</code>\n"
             f"💵 ราคาปิด: <code>{c_close:.2f}</code> (ห่างแนวต้าน {res - c_close:.2f})\n"
             f"📉 แท่งคู่: เขียว ➡️ แดง | Stoch %K: <code>{k_c:.2f}</code>\n"
             f"🛑 SL อ้างอิง: เหนือ High <code>{c_high:.2f}</code>{footer}")
    elif tf == "4h" and k_p >= 80 and k_c < 80:
        fire("4h:cross_sell", cid,
             f"🚨 <b>สัญญาณ SELL (Stoch ตัดกลับ) - TF 4H</b>\n{head}"
             f"📉 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&lt; 80)\n"
             f"💵 ราคาปิด: <code>{c_close:.2f}</code>{footer}")
    elif c_low <= sup and (k_c <= 20 or buy_pre_alerted) and prev_red and cur_green:
        fire(f"{tf}:close_buy", cid,
             f"🟢 <b>ยืนยัน BUY (ชนแนวรับ + Stoch + ปิดแท่งคู่) - TF {tf_u}</b>\n{head}"
             f"📉 Low: <code>{c_low:.2f}</code> ชนแนวรับ <code>{sup:.2f}</code>\n"
             f"💵 ราคาปิด: <code>{c_close:.2f}</code> (ห่างแนวรับ {c_close - sup:.2f})\n"
             f"📈 แท่งคู่: แดง ➡️ เขียว | Stoch %K: <code>{k_c:.2f}</code>\n"
             f"🛑 SL อ้างอิง: ใต้ Low <code>{c_low:.2f}</code>{footer}")
    elif tf == "4h" and k_p <= 20 and k_c > 20:
        fire("4h:cross_buy", cid,
             f"🟢 <b>สัญญาณ BUY (Stoch ตัดกลับ) - TF 4H</b>\n{head}"
             f"📈 %K: <code>{k_p:.1f}</code> ➡️ <code>{k_c:.1f}</code> (&gt; 20)\n"
             f"💵 ราคาปิด: <code>{c_close:.2f}</code>{footer}")

# ==========================================
# 🔍 วนตรวจตลาด
# ==========================================
def send_heartbeat(df_1h):
    price_txt = f"{float(df_1h['Close'].iloc[-1]):.2f}" if df_1h is not None else "ไม่ทราบ"
    send_telegram_message(
        f"🤖 <b>บอทรันปกติ (Heartbeat)</b>\n"
        f"📊 XAUUSD ({SYMBOL})\n"
        f"💰 ราคาล่าสุด: <code>{price_txt}</code>\n"
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

    df_4h = to_4h(df_1h)
    last_1h_start = df_1h.index[-1]
    for tf, df in (("1h", df_1h), ("4h", df_4h)):
        try:
            check_timeframe(tf, df, levels, last_1h_start)
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
