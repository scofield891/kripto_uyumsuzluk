"""
TCE Criteria Tracker
Screen logunu okur, kriter FALSE verilerini criteria_history.json'a kaydeder.
Her 10 dakikada bir calisir, tekrar kaydetmez.
"""
import json, os, subprocess, re, time, hashlib
from datetime import datetime

HISTORY_FILE = "criteria_history.json"

def load_history():
    try:
        with open(HISTORY_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"scans": []}

def save_history(data):
    # Son 2000 kayit tut (yaklasik 2 hafta)
    data["scans"] = data["scans"][-2000:]
    with open(HISTORY_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def get_screen_log():
    try:
        subprocess.run(['bash', '-c', 'screen -S tcebot -X hardcopy /tmp/screenlog.txt'],
                       capture_output=True, timeout=3)
        if os.path.exists('/tmp/screenlog.txt'):
            with open('/tmp/screenlog.txt', 'r', errors='replace') as f:
                return f.read()
    except:
        pass
    return ""

def parse_all_criteria(log_text):
    """Log'dan tum kriter bloklarini parse et"""
    results = []
    lines = log_text.split('\n')
    current_block = None
    current_time = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Kriter FALSE basligini bul (Turkce karakter bozuk olabilir)
        if 'Kriter' in line and ('FALSE' in line or 'k' in line.lower()):
            # Timestamp'i al
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if ts_match:
                current_time = ts_match.group(1)
                current_block = []
            continue
        
        # Kriter satirlarini parse et
        if current_block is not None:
            m = re.search(r'- (\w+): (\d+)/(\d+) \((\d+\.?\d*)%\)', line)
            if m:
                current_block.append({
                    'name': m.group(1),
                    'count': int(m.group(2)),
                    'total': int(m.group(3)),
                    'pct': float(m.group(4))
                })
            elif current_block:
                # Blok bitti
                if current_time and current_block:
                    results.append({
                        'time': current_time,
                        'criteria': current_block
                    })
                current_block = None
                current_time = None
    
    # Son blok
    if current_time and current_block:
        results.append({
            'time': current_time,
            'criteria': current_block
        })
    
    return results

def main():
    print(f"📊 Criteria Tracker baslatildi...")
    
    while True:
        try:
            log_text = get_screen_log()
            if not log_text:
                time.sleep(120)
                continue
            
            blocks = parse_all_criteria(log_text)
            if not blocks:
                time.sleep(120)
                continue
            
            history = load_history()
            existing_times = {s['time'] for s in history['scans']}
            
            new_count = 0
            for block in blocks:
                if block['time'] not in existing_times:
                    history['scans'].append(block)
                    existing_times.add(block['time'])
                    new_count += 1
            
            if new_count > 0:
                save_history(history)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {new_count} yeni tarama kaydedildi. Toplam: {len(history['scans'])}")
            
        except Exception as e:
            print(f"Hata: {e}")
        
        time.sleep(120)  # 2 dakikada bir kontrol

if __name__ == "__main__":
    main()
