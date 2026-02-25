"""
TCE Bot Monitor - Telegram /status komutu
Bot yaninda calisir, /status yazinca durumu atar.
Sadece DM'e cevap verir, kanalda/grupta cevap vermez.
"""
import os, json, asyncio, subprocess
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

async def get_balance():
    try:
        import ccxt
        exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY'),
            'secret': os.getenv('BYBIT_API_SECRET'),
            'options': {'defaultType': 'swap'}
        })
        if os.getenv('EXEC_TESTNET', 'false').lower() == 'true':
            exchange.set_sandbox_mode(True)
        bal = exchange.fetch_balance()
        free = float(bal.get('USDT', {}).get('free', 0) or 0)
        total = float(bal.get('USDT', {}).get('total', 0) or 0)
        return free, total
    except Exception as e:
        return None, None

def bot_running():
    try:
        r = subprocess.run(['pgrep', '-f', 'python3 bot.py'], capture_output=True, text=True)
        return r.returncode == 0
    except:
        return False

def get_positions():
    try:
        with open('exec_positions.json', 'r') as f:
            data = json.load(f)
        return data.get('positions', {})
    except:
        return {}

async def build_status():
    running = bot_running()
    positions = get_positions()
    free, total = await get_balance()
    max_pos = os.getenv('EXEC_MAX_POS', '10')
    status = "✅ Bot çalışıyor" if running else "❌ Bot DURMUŞ!"
    msg = f"📊 TCE Bot Durumu\n"
    msg += f"─────────────────────────\n"
    msg += f"{status}\n"
    if free is not None:
        msg += f"💰 Bakiye: {free:.2f} USDT (toplam: {total:.2f})\n"
    else:
        msg += f"💰 Bakiye: okunamadı\n"
    msg += f"📈 Açık pozisyon: {len(positions)}/{max_pos}\n"
    if positions:
        msg += f"─────────────────────────\n"
        for sym, pos in positions.items():
            side = pos.get('side', '?').upper()
            entry = pos.get('entry_price', 0)
            tp1_hit = "✅" if pos.get('tp1_hit') else "⏳"
            tp2_hit = "✅" if pos.get('tp2_hit') else "⏳"
            emoji = "🟢" if side == "LONG" else "🔴"
            msg += f"{emoji} {sym} {side} @ {entry}\n"
            msg += f"   TP1:{tp1_hit} TP2:{tp2_hit}\n"
    msg += f"─────────────────────────\n"
    msg += f"🕐 {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
    return msg

async def main():
    from telegram import Bot
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connect_timeout=20, read_timeout=20)
    bot = Bot(token=BOT_TOKEN, request=request)
    print("📡 Monitor başlatıldı, /status komutunu bekliyorum...")
    offset = None
    while True:
        try:
            updates = await bot.get_updates(offset=offset, timeout=30)
            for update in updates:
                offset = update.update_id + 1
                if update.message and update.message.text:
                    cmd = update.message.text.strip().lower()
                    chat_id = update.message.chat_id
                    chat_type = update.message.chat.type
                    # Sadece ozel mesajlara (DM) cevap ver
                    if chat_type != "private":
                        continue
                    if cmd == '/status':
                        msg = await build_status()
                        await bot.send_message(chat_id=chat_id, text=msg)
                    elif cmd == '/ping':
                        await bot.send_message(chat_id=chat_id, text="🏓 Pong! Bot aktif.")
                    elif cmd == '/help':
                        help_msg = ("🤖 TCE Monitor Komutları:\n"
                            "/status - Bot durumu + bakiye + pozisyonlar\n"
                            "/ping - Bot yaşıyor mu kontrol\n"
                            "/help - Bu mesaj")
                        await bot.send_message(chat_id=chat_id, text=help_msg)
        except Exception as e:
            print(f"Monitor hata: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
