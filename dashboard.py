"""
TCE Bot Dashboard v2 - Full Trading Panel
Shard tarama, kriter dokumu, pozisyonlar, trade gecmisi, aylik rapor
"""
import os, json, subprocess, re
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, Response

app = Flask(__name__)

# ─── HELPER FUNCTIONS ───

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

def get_balance():
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
    except:
        return None, None

def get_trade_history():
    """Bybit'ten kapanmis trade gecmisini cek"""
    try:
        import ccxt
        exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY'),
            'secret': os.getenv('BYBIT_API_SECRET'),
            'options': {'defaultType': 'swap'}
        })
        if os.getenv('EXEC_TESTNET', 'false').lower() == 'true':
            exchange.set_sandbox_mode(True)
        
        # Son 3 aylik PnL
        now = int(datetime.now().timestamp() * 1000)
        since = int((datetime.now() - timedelta(days=90)).timestamp() * 1000)
        
        trades = []
        try:
            # Bybit closed PnL endpoint
            result = exchange.private_get_v5_position_closed_pnl({
                'category': 'linear',
                'limit': 100,
            })
            pnl_list = result.get('result', {}).get('list', [])
            for t in pnl_list:
                trades.append({
                    'symbol': t.get('symbol', ''),
                    'side': t.get('side', ''),
                    'entry': float(t.get('avgEntryPrice', 0)),
                    'exit': float(t.get('avgExitPrice', 0)),
                    'pnl': float(t.get('closedPnl', 0)),
                    'qty': float(t.get('qty', 0)),
                    'closed_at': t.get('updatedTime', ''),
                    'leverage': t.get('leverage', ''),
                })
        except Exception as e:
            print(f"Trade history error: {e}")
        
        return trades
    except:
        return []

def get_screen_log():
    """Screen'den son log satirlarini al"""
    try:
        subprocess.run(['bash', '-c', 'screen -S tcebot -X hardcopy /tmp/screenlog.txt'], 
                       capture_output=True, timeout=3)
        if os.path.exists('/tmp/screenlog.txt'):
            with open('/tmp/screenlog.txt', 'r') as f:
                content = f.read()
            return content.strip()
    except:
        pass
    return ""

def parse_shard_info(log_text):
    """Log'dan shard tarama bilgisini parse et"""
    shards = []
    lines = log_text.split('\n')
    for line in reversed(lines):
        m = re.search(r'Shard (\d+)/(\d+) -> (\d+) sembol', line)
        if m:
            shard_num = int(m.group(1))
            total = int(m.group(2))
            symbols = int(m.group(3))
            ts = line[:19] if len(line) >= 19 else ''
            shards.append({'shard': shard_num, 'total': total, 'symbols': symbols, 'time': ts})
            if len(shards) >= 5:
                break
    shards.reverse()
    return shards

def parse_criteria(log_text):
    """Log'dan kriter FALSE dokumunu parse et"""
    criteria = []
    lines = log_text.split('\n')
    in_criteria = False
    for line in reversed(lines):
        if 'Kriter FALSE' in line:
            in_criteria = True
            continue
        if in_criteria:
            m = re.search(r'- (\w+): (\d+)/(\d+) \((\d+\.?\d*)%\)', line)
            if m:
                criteria.append({
                    'name': m.group(1),
                    'count': int(m.group(2)),
                    'total': int(m.group(3)),
                    'pct': float(m.group(4))
                })
            elif criteria:
                break
    criteria.reverse()
    return criteria

def calc_monthly_stats(trades):
    """Aylik R raporu ve winrate hesapla"""
    months = {}
    for t in trades:
        try:
            ts = int(t.get('closed_at', '0'))
            if ts > 0:
                dt = datetime.fromtimestamp(ts / 1000)
            else:
                continue
        except:
            continue
        
        key = dt.strftime('%Y-%m')
        if key not in months:
            months[key] = {'wins': 0, 'losses': 0, 'total_pnl': 0, 'trades': 0, 'total_r': 0}
        
        months[key]['trades'] += 1
        months[key]['total_pnl'] += t['pnl']
        
        if t['pnl'] > 0:
            months[key]['wins'] += 1
        else:
            months[key]['losses'] += 1
    
    result = []
    for month, stats in sorted(months.items(), reverse=True):
        wr = (stats['wins'] / stats['trades'] * 100) if stats['trades'] > 0 else 0
        result.append({
            'month': month,
            'trades': stats['trades'],
            'wins': stats['wins'],
            'losses': stats['losses'],
            'winrate': round(wr, 1),
            'pnl': round(stats['total_pnl'], 2),
        })
    return result

# ─── API ROUTES ───

@app.route('/')
def dashboard():
    return HTML_PAGE

@app.route('/api/status')
def api_status():
    running = bot_running()
    positions = get_positions()
    free, total = get_balance()
    max_pos = os.getenv('EXEC_MAX_POS', '10')
    
    log_text = get_screen_log()
    shards = parse_shard_info(log_text)
    criteria = parse_criteria(log_text)
    
    pos_list = []
    for sym, pos in positions.items():
        pos_list.append({
            'symbol': sym,
            'side': pos.get('side', '?').upper(),
            'entry': pos.get('entry_price', 0),
            'sl': pos.get('sl_price', 0),
            'tp1_price': pos.get('tp1_price', 0),
            'tp2_price': pos.get('tp2_price', 0),
            'tp1_hit': pos.get('tp1_hit', False),
            'tp2_hit': pos.get('tp2_hit', False),
            'margin': pos.get('margin_used', 0),
            'opened': pos.get('opened_at', ''),
        })
    
    return jsonify({
        'running': running,
        'balance_free': free,
        'balance_total': total,
        'positions': pos_list,
        'position_count': len(positions),
        'max_positions': int(max_pos),
        'time': datetime.now().strftime('%H:%M:%S %d.%m.%Y'),
        'exec_enabled': os.getenv('EXEC_ENABLED', 'false'),
        'leverage': os.getenv('EXEC_LEVERAGE', '5'),
        'size_pct': os.getenv('EXEC_SIZE_PCT', '10'),
        'shards': shards,
        'criteria': criteria,
    })

@app.route('/api/trades')
def api_trades():
    trades = get_trade_history()
    monthly = calc_monthly_stats(trades)
    
    total_pnl = sum(t['pnl'] for t in trades)
    total_wins = sum(1 for t in trades if t['pnl'] > 0)
    total_trades = len(trades)
    total_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0
    
    return jsonify({
        'trades': trades[:50],
        'monthly': monthly,
        'summary': {
            'total_trades': total_trades,
            'total_pnl': round(total_pnl, 2),
            'total_winrate': round(total_wr, 1),
            'total_wins': total_wins,
            'total_losses': total_trades - total_wins,
        }
    })

@app.route('/api/logs')
def api_logs():
    log = get_screen_log()
    if not log:
        for fname in ['nohup.out', 'bot.log']:
            if os.path.exists(fname):
                with open(fname, 'r') as f:
                    lines = f.readlines()
                log = ''.join(lines[-80:]).strip()
                break
    return Response(log or "Log bulunamadi", mimetype='text/plain')

# ─── HTML PAGE ───

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#0a0e17">
<title>TCE Bot</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚡</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e17;--card:#111827;--card2:#0f172a;--border:#1e293b;--green:#10b981;--red:#ef4444;--amber:#f59e0b;--blue:#3b82f6;--purple:#8b5cf6;--text:#e2e8f0;--muted:#64748b}
body{font-family:'Outfit',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding:0 0 80px 0;-webkit-tap-highlight-color:transparent}
.top{background:linear-gradient(180deg,#111827 0%,var(--bg) 100%);padding:16px 16px 0;position:sticky;top:0;z-index:10}
.header{display:flex;align-items:center;justify-content:space-between;padding-bottom:12px}
.logo{font-size:1.5rem;font-weight:700;background:linear-gradient(135deg,var(--green),var(--blue));-webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-.5px}
.status-badge{display:flex;align-items:center;font-size:.8rem;font-weight:600;padding:4px 10px;border-radius:20px;gap:6px}
.status-badge.on{background:rgba(16,185,129,.12);color:var(--green)}
.status-badge.off{background:rgba(239,68,68,.12);color:var(--red)}
.dot{width:8px;height:8px;border-radius:50%;animation:pulse 2s infinite}
.dot.on{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot.off{background:var(--red);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

.nav{display:flex;gap:2px;background:var(--card);border-radius:12px;padding:3px;margin-bottom:12px;border:1px solid var(--border)}
.nav-item{flex:1;text-align:center;padding:8px 4px;font-size:.72rem;font-weight:600;border-radius:10px;cursor:pointer;color:var(--muted);transition:.2s;white-space:nowrap}
.nav-item.active{background:var(--green);color:#000}

.content{padding:0 16px}
.page{display:none}.page.active{display:block}

/* Cards */
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px 14px}
.card.wide{grid-column:1/-1}
.card-label{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin-bottom:2px}
.card-val{font-family:'JetBrains Mono',monospace;font-size:1.4rem;font-weight:700;line-height:1.2}
.card-sub{font-size:.72rem;color:var(--muted);margin-top:1px;font-family:'JetBrains Mono',monospace}
.g{color:var(--green)}.r{color:var(--red)}.b{color:var(--blue)}.a{color:var(--amber)}.p{color:var(--purple)}

/* Shard Progress */
.shard-grid{display:flex;gap:4px;margin:8px 0}
.shard-bar{flex:1;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-family:'JetBrains Mono',monospace;font-size:.65rem;font-weight:600;transition:.3s}
.shard-bar.done{background:rgba(16,185,129,.15);color:var(--green);border:1px solid rgba(16,185,129,.3)}
.shard-bar.pending{background:rgba(100,116,139,.1);color:var(--muted);border:1px solid var(--border)}

/* Criteria bars */
.crit-item{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.crit-name{font-family:'JetBrains Mono',monospace;font-size:.68rem;width:120px;flex-shrink:0;color:var(--muted)}
.crit-bar-bg{flex:1;height:16px;background:var(--card2);border-radius:4px;overflow:hidden;border:1px solid var(--border)}
.crit-bar-fill{height:100%;border-radius:3px;transition:width .5s}
.crit-pct{font-family:'JetBrains Mono',monospace;font-size:.65rem;width:45px;text-align:right;flex-shrink:0}

/* Positions */
.pos-card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:8px;position:relative;overflow:hidden}
.pos-card::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px}
.pos-card.long::before{background:var(--green)}
.pos-card.short::before{background:var(--red)}
.pos-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.pos-sym{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1rem}
.pos-badge{font-size:.65rem;font-weight:700;padding:3px 8px;border-radius:5px}
.pos-badge.l{background:rgba(16,185,129,.12);color:var(--green)}
.pos-badge.s{background:rgba(239,68,68,.12);color:var(--red)}
.pos-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.pos-item-label{font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.pos-item-val{font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:600}
.tp-row{display:flex;gap:12px;margin-top:10px;padding-top:8px;border-top:1px solid var(--border)}
.tp-item{font-size:.72rem;font-family:'JetBrains Mono',monospace}

/* Trade history */
.trade-row{display:grid;grid-template-columns:1fr auto auto;gap:8px;padding:10px 12px;border-bottom:1px solid var(--border);align-items:center}
.trade-row:last-child{border:none}
.trade-sym{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:600}
.trade-side{font-size:.6rem;color:var(--muted)}
.trade-pnl{font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:700;text-align:right}
.trade-date{font-size:.6rem;color:var(--muted);text-align:right}

/* Monthly report */
.month-card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:8px}
.month-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.month-name{font-weight:700;font-size:.95rem}
.month-pnl{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1rem}
.month-stats{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.month-stat-label{font-size:.6rem;color:var(--muted);text-transform:uppercase}
.month-stat-val{font-family:'JetBrains Mono',monospace;font-size:.85rem;font-weight:600}

/* Logs */
.log-box{background:#000;border:1px solid var(--border);border-radius:14px;padding:12px;font-family:'JetBrains Mono',monospace;font-size:.6rem;line-height:1.6;color:var(--green);max-height:60vh;overflow-y:auto;white-space:pre-wrap;word-break:break-all}

.empty{text-align:center;padding:30px;color:var(--muted);font-size:.85rem}
.section{margin-bottom:16px}
.section-title{font-size:.72rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px}

.refresh-bar{position:fixed;bottom:0;left:0;right:0;padding:12px 16px;background:linear-gradient(0deg,var(--bg) 60%,transparent);z-index:20}
.btn{display:block;width:100%;padding:14px;background:linear-gradient(135deg,#10b981,#3b82f6);color:#fff;border:none;border-radius:14px;font-family:'Outfit',sans-serif;font-size:.95rem;font-weight:600;cursor:pointer;letter-spacing:.3px}
.btn:active{opacity:.7;transform:scale(.98)}.btn:disabled{opacity:.5}
.lu{text-align:center;font-size:.65rem;color:var(--muted);margin-top:8px;font-family:'JetBrains Mono',monospace}
</style>
</head>
<body>

<div class="top">
  <div class="header">
    <div class="logo">⚡ TCE Bot</div>
    <div class="status-badge off" id="sb"><span class="dot off" id="sd"></span><span id="st">Yükleniyor</span></div>
  </div>
  <div class="nav">
    <div class="nav-item active" onclick="go('overview')">Genel</div>
    <div class="nav-item" onclick="go('scan')">Tarama</div>
    <div class="nav-item" onclick="go('pos')">Pozisyon</div>
    <div class="nav-item" onclick="go('trades')">İşlemler</div>
    <div class="nav-item" onclick="go('logs')">Log</div>
  </div>
</div>

<div class="content">
<!-- GENEL -->
<div class="page active" id="p-overview">
  <div class="grid">
    <div class="card"><div class="card-label">Serbest Bakiye</div><div class="card-val g" id="bal">--</div><div class="card-sub" id="balt"></div></div>
    <div class="card"><div class="card-label">Toplam Bakiye</div><div class="card-val b" id="tbal">--</div><div class="card-sub" id="pnlToday"></div></div>
    <div class="card"><div class="card-label">Açık Pozisyon</div><div class="card-val a" id="pc">--</div><div class="card-sub" id="pm"></div></div>
    <div class="card"><div class="card-label">Mod</div><div class="card-val" id="es">--</div><div class="card-sub" id="em"></div></div>
    <div class="card"><div class="card-label">Kaldıraç</div><div class="card-val a" id="lev">--</div><div class="card-sub">margin: <span id="sp">--</span>%</div></div>
    <div class="card"><div class="card-label">Toplam PnL</div><div class="card-val" id="tpnl">--</div><div class="card-sub" id="twr"></div></div>
  </div>
</div>

<!-- TARAMA -->
<div class="page" id="p-scan">
  <div class="section">
    <div class="section-title">Shard Durumu</div>
    <div id="shardGrid" class="shard-grid"></div>
    <div class="card-sub" id="shardTime" style="margin-top:4px"></div>
  </div>
  <div class="section">
    <div class="section-title">Kriter FALSE Dökümü</div>
    <div id="critList"></div>
  </div>
</div>

<!-- POZISYONLAR -->
<div class="page" id="p-pos">
  <div id="posList"><div class="empty">Yükleniyor...</div></div>
</div>

<!-- İŞLEMLER -->
<div class="page" id="p-trades">
  <div id="monthlySummary"></div>
  <div class="section">
    <div class="section-title">Son İşlemler</div>
    <div class="card" style="padding:0;overflow:hidden" id="tradeList"><div class="empty">Yükleniyor...</div></div>
  </div>
</div>

<!-- LOGLAR -->
<div class="page" id="p-logs">
  <div class="log-box" id="lg">Yükleniyor...</div>
</div>
</div>

<div class="refresh-bar">
  <button class="btn" onclick="rf()">🔄 Yenile</button>
  <div class="lu" id="lu"></div>
</div>

<script>
var D={};
function go(n){
  document.querySelectorAll('.nav-item').forEach(function(t,i){
    t.classList.toggle('active',['overview','scan','pos','trades','logs'][i]===n)
  });
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active')});
  document.getElementById('p-'+n).classList.add('active');
  if(n==='logs')ll();
  if(n==='trades'&&!D.trades)lt();
}

async function ls(){
  try{
    var r=await fetch('/api/status');var d=await r.json();D.status=d;
    var sb=document.getElementById('sb');
    var sd=document.getElementById('sd');
    var st=document.getElementById('st');
    if(d.running){sb.className='status-badge on';sd.className='dot on';st.textContent='Aktif'}
    else{sb.className='status-badge off';sd.className='dot off';st.textContent='Durmuş!'}
    
    document.getElementById('bal').textContent=d.balance_free!==null?d.balance_free.toFixed(2)+' $':'N/A';
    document.getElementById('tbal').textContent=d.balance_total!==null?d.balance_total.toFixed(2)+' $':'N/A';
    document.getElementById('pc').textContent=d.position_count+'/'+d.max_positions;
    document.getElementById('pm').textContent=d.position_count>0?'aktif':'boş';
    document.getElementById('lev').textContent=d.leverage+'x';
    document.getElementById('sp').textContent=d.size_pct;
    
    var ev=document.getElementById('es');var em=document.getElementById('em');
    if(d.exec_enabled==='true'){ev.textContent='CANLI';ev.className='card-val g';em.textContent='oto trade aktif'}
    else{ev.textContent='KAPALI';ev.className='card-val r';em.textContent='sadece sinyal'}
    
    // Shards
    var sg=document.getElementById('shardGrid');
    if(d.shards&&d.shards.length>0){
      sg.innerHTML=d.shards.map(function(s){return '<div class="shard-bar done">'+s.shard+'/'+s.total+' ('+s.symbols+')</div>'}).join('');
      document.getElementById('shardTime').textContent='Son: '+d.shards[d.shards.length-1].time;
    }else{sg.innerHTML='<div class="shard-bar pending">Tarama bekleniyor...</div>'}
    
    // Criteria
    var cl=document.getElementById('critList');
    if(d.criteria&&d.criteria.length>0){
      cl.innerHTML=d.criteria.map(function(c){
        var color=c.pct>90?'var(--red)':c.pct>70?'var(--amber)':c.pct>40?'var(--blue)':'var(--green)';
        return '<div class="crit-item"><span class="crit-name">'+c.name+'</span><div class="crit-bar-bg"><div class="crit-bar-fill" style="width:'+c.pct+'%;background:'+color+'"></div></div><span class="crit-pct" style="color:'+color+'">'+c.pct+'%</span></div>'
      }).join('')
    }else{cl.innerHTML='<div class="empty">Kriter verisi bekleniyor...</div>'}
    
    // Positions
    var pl=document.getElementById('posList');
    if(d.positions.length===0){pl.innerHTML='<div class="empty">Açık pozisyon yok</div>'}
    else{pl.innerHTML=d.positions.map(function(p){
      var s=p.side==='SHORT'?'short':'long';
      return '<div class="pos-card '+s+'"><div class="pos-top"><span class="pos-sym">'+p.symbol.replace('/USDT:USDT','')+'</span><span class="pos-badge '+(s==='long'?'l':'s')+'">'+p.side+'</span></div><div class="pos-grid"><div><div class="pos-item-label">Giriş</div><div class="pos-item-val">'+p.entry+'</div></div><div><div class="pos-item-label">Stop Loss</div><div class="pos-item-val r">'+p.sl+'</div></div><div><div class="pos-item-label">Margin</div><div class="pos-item-val">'+(p.margin?p.margin.toFixed(2)+'$':'-')+'</div></div></div><div class="tp-row"><span class="tp-item">TP1: '+(p.tp1_hit?'✅ Kapandı':'⏳ '+p.tp1_price)+'</span><span class="tp-item">TP2: '+(p.tp2_hit?'✅ Kapandı':'⏳ '+(p.tp2_price||'-'))+'</span></div></div>'
    }).join('')}
    
    document.getElementById('lu').textContent='Son: '+d.time;
  }catch(e){document.getElementById('st').textContent='Bağlantı hatası'}
}

async function lt(){
  try{
    var r=await fetch('/api/trades');var d=await r.json();D.trades=d;
    
    // Summary
    var s=d.summary;
    document.getElementById('tpnl').textContent=(s.total_pnl>=0?'+':'')+s.total_pnl.toFixed(2)+' $';
    document.getElementById('tpnl').className='card-val '+(s.total_pnl>=0?'g':'r');
    document.getElementById('twr').textContent='WR: '+s.total_winrate+'% ('+s.total_trades+' trade)';
    
    // Monthly
    var ms=document.getElementById('monthlySummary');
    if(d.monthly.length>0){
      ms.innerHTML='<div class="section"><div class="section-title">Aylık Rapor</div>'+d.monthly.map(function(m){
        var c=m.pnl>=0?'g':'r';
        return '<div class="month-card"><div class="month-header"><span class="month-name">'+m.month+'</span><span class="month-pnl '+c+'">'+(m.pnl>=0?'+':'')+m.pnl.toFixed(2)+' $</span></div><div class="month-stats"><div><div class="month-stat-label">İşlem</div><div class="month-stat-val">'+m.trades+'</div></div><div><div class="month-stat-label">Kazanç</div><div class="month-stat-val g">'+m.wins+'W / '+m.losses+'L</div></div><div><div class="month-stat-label">Winrate</div><div class="month-stat-val '+(m.winrate>=50?'g':'a')+'">'+m.winrate+'%</div></div></div></div>'
      }).join('')+'</div>'
    }
    
    // Recent trades
    var tl=document.getElementById('tradeList');
    if(d.trades.length===0){tl.innerHTML='<div class="empty">İşlem geçmişi yok</div>'}
    else{tl.innerHTML=d.trades.slice(0,20).map(function(t){
      var c=t.pnl>=0?'g':'r';
      var dt='';
      try{var dd=new Date(parseInt(t.closed_at));dt=dd.toLocaleDateString('tr-TR',{day:'2-digit',month:'short'})}catch(e){}
      return '<div class="trade-row"><div><div class="trade-sym">'+t.symbol.replace('USDT','')+'</div><div class="trade-side">'+t.side+' '+t.leverage+'x</div></div><div class="trade-pnl '+c+'">'+(t.pnl>=0?'+':'')+t.pnl.toFixed(3)+' $</div><div class="trade-date">'+dt+'</div></div>'
    }).join('')}
  }catch(e){console.log(e)}
}

async function ll(){
  try{var r=await fetch('/api/logs');var t=await r.text();var b=document.getElementById('lg');b.textContent=t;b.scrollTop=b.scrollHeight}catch(e){}}

function rf(){
  var b=document.querySelector('.btn');b.disabled=true;b.textContent='⏳';
  Promise.all([ls(),lt()]).then(function(){b.disabled=false;b.textContent='🔄 Yenile'})
}

ls();lt();setInterval(ls,60000);setInterval(lt,300000);
</script>
</body>
</html>"""

if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"TCE Dashboard: http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
