import time
import requests
import pandas as pd
import pandas_ta as ta
import yfinance as yf

# ==========================================
# ⚙️ ตั้งค่าการเชื่อมต่อ
# ==========================================
TOKEN = "ใส่_BOT_TOKEN_ของคุณที่นี่"
CHAT_ID = "ใส่_CHAT_ID_ของคุณที่นี่"
SHEET_ID = "10aE1bClTH6Ru1vu8rWZwA_yWg4EnMAnWJtdVeMrFYmo"

SYMBOL = "GC=F"  # ราคาทองคำ XAUUSD (Gold Futures)

# ตัวแปรบันทึกเวลาแจ้งเตือนล่าสุดแยกตาม Timeframe เพื่อป้องกันเตือนซ้ำในแท่งเดิม
last_alert_time = {"1h": None, "4h": None}

def get_levels_from_google_sheet():
    """ดึงแนวรับ-แนวต้านแยกตาม TF 1H และ 4H จาก Google Sheets"""
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

def check_market():
    global last_alert_time
    
    levels = get_levels_from_google_sheet()
    if not levels:
        return

    timeframes = ["1h", "4h"]
    
    for tf in timeframes:
        try:
            df = yf.download(SYMBOL, period="10d", interval=tf, progress=False)
            if df.empty or len(df) < 20:
                continue

            stoch = ta.stoch(high=df['High'], low=df['Low'], close=df['Close'], k=14, d=3, smooth_k=3)
            
            current_time = str(df.index[-2])
            current_high = float(df['High'].iloc[-2])
            current_low = float(df['Low'].iloc[-2])
            k_value = float(stoch['STOCHk_14_3_3'].iloc[-2])

            res_level = levels[tf]["res"]
            sup_level = levels[tf]["sup"]

            print(f"[{tf.upper()}] Time: {current_time} | High: {current_high:.2f} | Low: {current_low:.2f} | Stoch %K: {k_value:.2f}")

            if last_alert_time[tf] == current_time:
                continue

            # เงื่อนไข SELL: ราคาชนแนวต้าน + Stoch >= 80 พร้อมกัน
            if current_high >= res_level and k_value >= 80:
                msg = (
                    f"🚨 *สัญญาณ SELL (Short) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD`\n"
                    f"📈 ราคา High ชนแนวต้าน: `{current_high:.2f}` (แนวต้าน {tf.upper()}: {res_level})\n"
                    f"📉 Stochastic %K: `{k_value:.2f}` (Overbought >= 80)\n"
                    f"📱 *เงื่อนไขครบถ้วน! เปิด MT5 เช็กกราฟแล้วกดเทรดได้เลย!*"
                )
                send_telegram_message(msg)
                last_alert_time[tf] = current_time
                continue

            # เงื่อนไข BUY: ราคาชนแนวรับ + Stoch <= 20 พร้อมกัน
            if current_low <= sup_level and k_value <= 20:
                msg = (
                    f"🟢 *สัญญาณ BUY (Long) - TF {tf.upper()}*\n"
                    f"📊 คู่เงิน: `XAUUSD`\n"
                    f"📉 ราคา Low ชนแนวรับ: `{current_low:.2f}` (แนวรับ {tf.upper()}: {sup_level})\n"
                    f"📈 Stochastic %K: `{k_value:.2f}` (Oversold <= 20)\n"
                    f"📱 *เงื่อนไขครบถ้วน! เปิด MT5 เช็กกราฟแล้วเตรียมเทรดได้เลย!*"
                )
                send_telegram_message(msg)
                last_alert_time[tf] = current_time
                continue

        except Exception as e:
            print(f"เกิดข้อผิดพลาดใน TF {tf}: {e}")

if __name__ == "__main__":
    print("🤖 บอท XAUUSD (แยก 1H/4H + เงื่อนไขพร้อมกัน) เริ่มทำงานแล้ว...")
    while True:
        check_market()
        time.sleep(900)
