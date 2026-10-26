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

# ใช้ราคา Spot ทองคำจริง (XAUUSD=X) ให้ตรงกับตลาดจริงมากที่สุด
SYMBOL = "XAUUSD=X"

last_alert_time = {"1h": None, "4h": None}

# ==========================================
# 🌐 จำลองเว็บเซิร์ฟเวอร์เพื่อให้ Render ผ่านพอร์ต (สำหรับ Web Service ฟรี)
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
# 🔍 ตรวจสอบตลาด
# ==========================================
def check_market():
    global last_alert_time
    levels = get_levels_from_google_sheet()
    if not levels:
        return

    for tf in ["1h", "4h"]:
        try:
            # ดึงข้อมูลย้อนหลังพอประมาณสำหรับคำนวณอินดิเคเตอร์
            df = yf.download(SYMBOL, period="5d", interval=tf, progress=False)
            if df.empty or len(df) < 20:
                continue

            stoch_k, _ = calculate_stoch(df)
            current_time = str(df.index[-1])
            
            # ดึงค่าแบบแปลงเป็น float เพื่อป้องกันปัญหาข้อมูลของ Series/DataFrame
            current_high = float(df['High'].iloc[-1].item() if hasattr(df['High'].iloc[-1], 'item') else df['High'].iloc[-1])
            current_low = float(df['Low'].iloc[-1].item() if hasattr(df['Low'].iloc[-1], 'item') else df['Low'].iloc[-1])
            k_value = float(stoch_k.iloc[-1].item() if hasattr(stoch_k.iloc[-1], 'item') else stoch_k.iloc[-1])

            res_level = levels[tf]["res"]
            sup_level = levels[tf]["sup"]

            # แสดงผลราคาและอินดิเคเตอร์ออกทาง Logs ชัดเจนทุกรอบ
            print(f"[{tf.upper()}] ราคา High: {current_high:.2f} | Low: {current_low:.2f} | Stoch %K: {k_value:.2f}")

            if last_alert_time[tf] == current_time:
                continue

            # เงื่อนไข SELL
            if current_high >= res_level and k_value >= 80:
                msg = (
                    f"🚨 *สัญญาณ SELL (Short) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD` (Spot)\n"
                    f"📈 High ชนต้าน: `{current_high:.2f}` (แนวต้าน: {res_level})\n"
                    f"📉 Stochastic %K: `{k_value:.2f}` (Overbought >= 80)\n"
                    f"📱 *เปิด MT5 เช็กกราฟแล้วลุยเลย!*"
                )
                send_telegram_message(msg)
                last_alert_time[tf] = current_time
                continue

            # เงื่อนไข BUY
            if current_low <= sup_level and k_value <= 20:
                msg = (
                    f"🟢 *สัญญาณ BUY (Long) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD` (Spot)\n"
                    f"📉 Low ชนรับ: `{current_low:.2f}` (แนวรับ: {sup_level})\n"
                    f"📈 Stochastic %K: `{k_value:.2f}` (Oversold <= 20)\n"
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
    
    print("🤖 บอท XAUUSD (Spot Realtime) เริ่มทำงานแล้ว...")
    while True:
        check_market()
        # เช็กราคาและอินดิเคเตอร์ทุกๆ 5 นาที (300 วินาที)
        time.sleep(300)
