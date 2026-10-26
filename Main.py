import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import pandas as pd
import yfinance as yf

# ==========================================
# ⚙️ ตั้งค่าการเชื่อมต่อ
# ==========================================
TOKEN = "8696072748:AAGgKHI5ODOSZqldqETioZXKowULg8AjQ-c"
CHAT_ID = "-5386954768"
SHEET_ID = "10aE1bClTH6Ru1vu8rWZwA_yWg4EnMAnWJtdVeMrFYmo"

SYMBOL = "GC=F"

last_alert_time = {"1h": None, "4h": None}

# ==========================================
# 🌐 จำลองเว็บเซิร์ฟเวอร์เพื่อให้ Render ผ่านพอร์ต
# ==========================================
class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"XAUUSD Bot is running!")
    def log_message(self, format, *args):
        pass

def run_web():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHandler)
    server.serve_forever()

# ==========================================
# 📊 ฟังก์ชันดึง Google Sheets
# ==========================================
def get_levels_from_google_sheet():
    try:
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv"
        sheet_df = pd.read_csv(url)
        levels = {
            "1h": {
                "res": float(sheet_df['1H_Res'].iloc[0]),
                "sup": float(sheet_df['1H_Sup'].iloc[0])
            },
            "4h": {
                "res": float(sheet_df['4H_Res'].iloc[0]),
                "sup": float(sheet_df['4H_Sup'].iloc[0])
            }
        }
        return levels
    except Exception as e:
        print(f"⚠️ อ่าน Google Sheets ไม่สำเร็จ: {e}")
        return None

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Telegram Error: {e}")

# ==========================================
# 📈 คำนวณ Stochastic (Pure Pandas)
# ==========================================
def calculate_stoch(df, k_period=14, d_period=3, smooth_k=3):
    low_min = df['Low'].rolling(window=k_period).min()
    high_max = df['High'].rolling(window=k_period).max()
    k_raw = 100 * ((df['Close'] - low_min) / (high_max - low_min))
    stoch_k = k_raw.rolling(window=smooth_k).mean()
    stoch_d = stoch_k.rolling(window=d_period).mean()
    return stoch_k, stoch_d

# ==========================================
# 🔍 ตรวจสอบตลาด (เพิ่มเงื่อนไขแท่งคู่ เขียว/แดง)
# ==========================================
def check_market():
    global last_alert_time
    levels = get_levels_from_google_sheet()
    if not levels:
        return

    for tf in ["1h", "4h"]:
        try:
            df = yf.download(SYMBOL, period="5d", interval=tf, progress=False)
            if df.empty or len(df) < 20:
                continue

            stoch_k, _ = calculate_stoch(df)
            current_time = str(df.index[-1])
            
            # ข้อมูลแท่งปัจจุบัน (Index -1)
            curr_high = float(df['High'].iloc[-1].item() if hasattr(df['High'].iloc[-1], 'item') else df['High'].iloc[-1])
            curr_low = float(df['Low'].iloc[-1].item() if hasattr(df['Low'].iloc[-1], 'item') else df['Low'].iloc[-1])
            curr_open = float(df['Open'].iloc[-1].item() if hasattr(df['Open'].iloc[-1], 'item') else df['Open'].iloc[-1])
            curr_close = float(df['Close'].iloc[-1].item() if hasattr(df['Close'].iloc[-1], 'item') else df['Close'].iloc[-1])
            k_value = float(stoch_k.iloc[-1].item() if hasattr(stoch_k.iloc[-1], 'item') else stoch_k.iloc[-1])

            # ข้อมูลแท่งก่อนหน้า (Index -2) สำหรับเช็กสีแท่งคู่
            prev_open = float(df['Open'].iloc[-2].item() if hasattr(df['Open'].iloc[-2], 'item') else df['Open'].iloc[-2])
            prev_close = float(df['Close'].iloc[-2].item() if hasattr(df['Close'].iloc[-2], 'item') else df['Close'].iloc[-2])

            res_level = levels[tf]["res"]
            sup_level = levels[tf]["sup"]

            print(f"[{tf.upper()}] High: {curr_high:.2f} | Low: {curr_low:.2f} | Stoch: {k_value:.2f}")

            if last_alert_time[tf] == current_time:
                continue

            # เช็กสถานะสีแท่งเทียน
            is_prev_green = prev_close > prev_open
            is_prev_red = prev_close < prev_open
            is_curr_green = curr_close > curr_open
            is_curr_red = curr_close < curr_open

            # 🚨 เงื่อนไข SELL: ชนต้าน + Stoch >= 80 + แท่งคู่ (เขียว แล้วต่อด้วย แดง)
            if curr_high >= res_level and k_value >= 80 and is_prev_green and is_curr_red:
                msg = (
                    f"🚨 *สัญญาณ SELL (แท่งคู่กลับตัว) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD` (Spot/GC)\n"
                    f"📈 High ชนต้าน: `{curr_high:.2f}` (แนวต้าน: {res_level})\n"
                    f"📉 แท่งคู่: เขียว ➡️ แดง (Stoch %K: `{k_value:.2f}`)\n"
                    f"📱 *เปิด MT5 เช็กกราฟแล้วลุยเลย!*"
                )
                send_telegram_message(msg)
                last_alert_time[tf] = current_time
                continue

            # 🟢 เงื่อนไข BUY: ชนรับ + Stoch <= 20 + แท่งคู่ (แดง แล้วต่อด้วย เขียว)
            if curr_low <= sup_level and k_value <= 20 and is_prev_red and is_curr_green:
                msg = (
                    f"🟢 *สัญญาณ BUY (แท่งคู่กลับตัว) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD` (Spot/GC)\n"
                    f"📉 Low ชนรับ: `{curr_low:.2f}` (แนวรับ: {sup_level})\n"
                    f"📈 แท่งคู่: แดง ➡️ เขียว (Stoch %K: `{k_value:.2f}`)\n"
                    f"📱 *เปิด MT5 เช็กกราฟแล้วลุยเลย!*"
                )
                send_telegram_message(msg)
                last_alert_time[tf] = current_time
                continue

        except Exception as e:
            print(f"Error in TF {tf}: {e}")

# ==========================================
# 🚀 จุดเริ่มต้นรันโปรแกรม
# ==========================================
if __name__ == "__main__":
    t = threading.Thread(target=run_web)
    t.daemon = True
    t.start()
    
    print("🤖 บอท XAUUSD เริ่มทำงานแล้ว...")
    while True:
        check_market()
        time.sleep(300)
