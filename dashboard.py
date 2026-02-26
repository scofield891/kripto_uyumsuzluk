"""
TCE Bot Dashboard v3 - Full Trading Panel + Criteria Analysis
"""
import os, json, subprocess, re
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, Response

app = Flask(__name__)

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
        return {k: v for k, v in data.items() if k != '_meta'}
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
    try:
        import ccxt
        exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY'),
            'secret': os.getenv('BYBIT_API_SECRET'),
            'options': {'defaultType': 'swap'}
        })
        if os.getenv('EXEC_TESTNET', 'false').lower() == 'true':
            exchange.set_sandbox_mode(True)
        trades = []
        try:
            result = exchange.private_get_v5_position_closed_pnl({'category': 'linear', 'limit': 100})
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
    try:
        subprocess.run(['bash', '-c', 'screen -S tcebot -X hardcopy /tmp/screenlog.txt'],
                       capture_output=True, timeout=3)
        if os.path.exists('/tmp/screenlog.txt'):
            with open('/tmp/screenlog.txt', 'r', errors='replace') as f:
                return f.read().strip()
    except:
        pass
    for fname in ['nohup.out', 'bot.log']:
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                lines = f.readlines()
            return ''.join(lines[-80:]).strip()
    return ""

def calc_monthly_stats(trades):
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
            months[key] = {'wins': 0, 'losses': 0, 'total_pnl': 0, 'trades': 0}
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
            'month': month, 'trades': stats['trades'],
            'wins': stats['wins'], 'losses': stats['losses'],
            'winrate': round(wr, 1), 'pnl': round(stats['total_pnl'], 2),
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
    pos_list = []
    for sym, pos in positions.items():
        pos_list.append({
            'symbol': sym, 'side': pos.get('side', '?').upper(),
            'entry': pos.get('entry_price', 0), 'sl': pos.get('sl_price', 0),
            'tp1_price': pos.get('tp1_price', 0), 'tp2_price': pos.get('tp2_price', 0),
            'tp1_hit': pos.get('tp1_hit', False), 'tp2_hit': pos.get('tp2_hit', False),
            'margin': pos.get('margin_used', 0), 'opened': pos.get('opened_at', ''),
        })
    return jsonify({
        'running': running, 'balance_free': free, 'balance_total': total,
        'positions': pos_list, 'position_count': len(positions),
        'max_positions': int(max_pos),
        'time': datetime.now().strftime('%H:%M:%S %d.%m.%Y'),
        'exec_enabled': os.getenv('EXEC_ENABLED', 'false'),
        'leverage': os.getenv('EXEC_LEVERAGE', '5'),
        'size_pct': os.getenv('EXEC_SIZE_PCT', '10'),
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
        'trades': trades[:50], 'monthly': monthly,
        'summary': {
            'total_trades': total_trades, 'total_pnl': round(total_pnl, 2),
            'total_winrate': round(total_wr, 1),
            'total_wins': total_wins, 'total_losses': total_trades - total_wins,
        }
    })

@app.route('/api/criteria')
def api_criteria():
    try:
        with open('criteria_history.json', 'r') as f:
            data = json.load(f)
    except:
        return jsonify({'daily': [], 'weekly': [], 'monthly': [], 'total_scans': 0})
    
    scans = data.get('scans', [])
    if not scans:
        return jsonify({'daily': [], 'weekly': [], 'monthly': [], 'total_scans': 0})
    
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    week_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    month_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    
    def aggregate(scans_list):
        agg = {}
        for scan in scans_list:
            for c in scan.get('criteria', []):
                name = c['name']
                if name not in agg:
                    agg[name] = {'total_pct': 0, 'count': 0}
                agg[name]['total_pct'] += c['pct']
                agg[name]['count'] += 1
        result = []
        for name, vals in agg.items():
            avg_pct = vals['total_pct'] / vals['count'] if vals['count'] > 0 else 0
            result.append({'name': name, 'avg_pct': round(avg_pct, 1), 'scans': vals['count']})
        result.sort(key=lambda x: x['avg_pct'], reverse=True)
        return result
    
    daily_scans = [s for s in scans if s['time'].startswith(today)]
    weekly_scans = [s for s in scans if s['time'] >= week_ago]
    monthly_scans = [s for s in scans if s['time'] >= month_ago]
    
    return jsonify({
        'daily': aggregate(daily_scans), 'daily_scans': len(daily_scans),
        'weekly': aggregate(weekly_scans), 'weekly_scans': len(weekly_scans),
        'monthly': aggregate(monthly_scans), 'monthly_scans': len(monthly_scans),
        'total_scans': len(scans)
    })

@app.route('/api/logs')
def api_logs():
    log = get_screen_log()
    return Response(log or "Log bulunamadi", mimetype='text/plain')

# ─── HTML ───
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
.logo{font-size:1.5rem;font-weight:700;background:linear-gradient(135deg,var(--green),var(--blue));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.sb{display:flex;align-items:center;font-size:.8rem;font-weight:600;padding:4px 10px;border-radius:20px;gap:6px}
.sb.on{background:rgba(16,185,129,.12);color:var(--green)}.sb.off{background:rgba(239,68,68,.12);color:var(--red)}
.dot{width:8px;height:8px;border-radius:50%;animation:pulse 2s infinite}
.dot.on{background:var(--green);box-shadow:0 0 6px var(--green)}.dot.off{background:var(--red);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.nav{display:flex;gap:2px;background:var(--card);border-radius:12px;padding:3px;margin-bottom:12px;border:1px solid var(--border)}
.ni{flex:1;text-align:center;padding:8px 4px;font-size:.72rem;font-weight:600;border-radius:10px;cursor:pointer;color:var(--muted);transition:.2s;white-space:nowrap}
.ni.a{background:var(--green);color:#000}
.content{padding:0 16px}.page{display:none}.page.a{display:block}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px 14px}
.cl{font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin-bottom:2px}
.cv{font-family:'JetBrains Mono',monospace;font-size:1.4rem;font-weight:700;line-height:1.2}
.cs{font-size:.72rem;color:var(--muted);margin-top:1px;font-family:'JetBrains Mono',monospace}
.g{color:var(--green)}.r{color:var(--red)}.b{color:var(--blue)}.am{color:var(--amber)}.p{color:var(--purple)}
/* Criteria bars */
.ci{display:flex;align-items:center;gap:6px;margin-bottom:5px}
.cn{font-family:'JetBrains Mono',monospace;font-size:.62rem;width:110px;flex-shrink:0;color:var(--muted);overflow:hidden;text-overflow:ellipsis}
.cbg{flex:1;height:18px;background:var(--card2);border-radius:4px;overflow:hidden;border:1px solid var(--border)}
.cf{height:100%;border-radius:3px;transition:width .5s;display:flex;align-items:center;justify-content:flex-end;padding-right:4px;font-family:'JetBrains Mono',monospace;font-size:.55rem;color:rgba(255,255,255,.8);min-width:30px}
.cp{font-family:'JetBrains Mono',monospace;font-size:.62rem;width:42px;text-align:right;flex-shrink:0}
/* Period tabs */
.ptabs{display:flex;gap:4px;margin-bottom:12px}
.pt{padding:6px 12px;font-size:.7rem;font-weight:600;border-radius:8px;cursor:pointer;background:var(--card);border:1px solid var(--border);color:var(--muted)}
.pt.a{background:var(--blue);color:#fff;border-color:var(--blue)}
.pinfo{font-size:.68rem;color:var(--muted);margin-bottom:10px;font-family:'JetBrains Mono',monospace}
/* Position cards */
.pc{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:8px;position:relative;overflow:hidden}
.pc::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px}
.pc.long::before{background:var(--green)}.pc.short::before{background:var(--red)}
.pt2{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.psym{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1rem}
.pbdg{font-size:.65rem;font-weight:700;padding:3px 8px;border-radius:5px}
.pbdg.l{background:rgba(16,185,129,.12);color:var(--green)}.pbdg.s{background:rgba(239,68,68,.12);color:var(--red)}
.pg{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.pgl{font-size:.6rem;color:var(--muted);text-transform:uppercase}.pgv{font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:600}
.tpr{display:flex;gap:12px;margin-top:10px;padding-top:8px;border-top:1px solid var(--border)}.tpi{font-size:.72rem;font-family:'JetBrains Mono',monospace}
/* Trade rows */
.tr{display:grid;grid-template-columns:1fr auto auto;gap:8px;padding:10px 12px;border-bottom:1px solid var(--border);align-items:center}
.tr:last-child{border:none}.tsym{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:600}.tsd{font-size:.6rem;color:var(--muted)}
.tpnl{font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:700;text-align:right}.tdt{font-size:.6rem;color:var(--muted);text-align:right}
/* Month cards */
.mc{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:14px;margin-bottom:8px}
.mh{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.mn{font-weight:700;font-size:.95rem}.mpnl{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1rem}
.ms{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.msl{font-size:.6rem;color:var(--muted);text-transform:uppercase}.msv{font-family:'JetBrains Mono',monospace;font-size:.85rem;font-weight:600}
/* Log */
.lb{background:#000;border:1px solid var(--border);border-radius:14px;padding:12px;font-family:'JetBrains Mono',monospace;font-size:.6rem;line-height:1.6;color:var(--green);max-height:60vh;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.empty{text-align:center;padding:30px;color:var(--muted);font-size:.85rem}
.section{margin-bottom:16px}.st{font-size:.72rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px}
.rbar{position:fixed;bottom:0;left:0;right:0;padding:12px 16px;background:linear-gradient(0deg,var(--bg) 60%,transparent);z-index:20}
.btn{display:block;width:100%;padding:14px;background:linear-gradient(135deg,#10b981,#3b82f6);color:#fff;border:none;border-radius:14px;font-family:'Outfit',sans-serif;font-size:.95rem;font-weight:600;cursor:pointer}
.btn:active{opacity:.7;transform:scale(.98)}.btn:disabled{opacity:.5}
.lu{text-align:center;font-size:.65rem;color:var(--muted);margin-top:8px;font-family:'JetBrains Mono',monospace}
</style>
</head>
<body>
<div class="top">
<div class="header"><div class="logo">⚡ TCE Bot</div><div class="sb off" id="sb"><span class="dot off" id="sd"></span><span id="stx">Yükleniyor</span></div></div>
<div class="nav">
<div class="ni a" onclick="go('overview')">Genel</div>
<div class="ni" onclick="go('crit')">Tarama</div>
<div class="ni" onclick="go('pos')">Pozisyon</div>
<div class="ni" onclick="go('trades')">İşlemler</div>
<div class="ni" onclick="go('logs')">Log</div>
</div>
</div>
<div class="content">

<!-- GENEL -->
<div class="page a" id="p-overview">
<div class="grid">
<div class="card"><div class="cl">Serbest Bakiye</div><div class="cv g" id="bal">--</div><div class="cs" id="balt"></div></div>
<div class="card"><div class="cl">Toplam Bakiye</div><div class="cv b" id="tbal">--</div></div>
<div class="card"><div class="cl">Açık Pozisyon</div><div class="cv am" id="pcc">--</div><div class="cs" id="pm"></div></div>
<div class="card"><div class="cl">Mod</div><div class="cv" id="es">--</div><div class="cs" id="em"></div></div>
<div class="card"><div class="cl">Kaldıraç</div><div class="cv am" id="lev">--</div><div class="cs">margin: <span id="sp">--</span>%</div></div>
<div class="card"><div class="cl">Toplam PnL</div><div class="cv" id="tpnl">--</div><div class="cs" id="twr"></div></div>
</div></div>

<!-- TARAMA / KRİTER ANALİZİ -->
<div class="page" id="p-crit">
<div class="ptabs">
<div class="pt a" onclick="sp('daily')">Günlük</div>
<div class="pt" onclick="sp('weekly')">Haftalık</div>
<div class="pt" onclick="sp('monthly')">Aylık</div>
</div>
<div class="pinfo" id="cinfo">Veri bekleniyor...</div>
<div id="critBars"><div class="empty">Kriter verisi toplanıyor...<br><br>Tracker her 2 dakikada veri kaydeder.<br>Biraz bekleyip yenile.</div></div>
</div>

<!-- POZİSYONLAR -->
<div class="page" id="p-pos"><div id="posList"><div class="empty">Yükleniyor...</div></div></div>

<!-- İŞLEMLER -->
<div class="page" id="p-trades">
<div id="monthlySummary"></div>
<div class="section"><div class="st">Son İşlemler</div>
<div class="card" style="padding:0;overflow:hidden" id="tradeList"><div class="empty">Yükleniyor...</div></div>
</div></div>

<!-- LOG -->
<div class="page" id="p-logs"><div class="lb" id="lg">Yükleniyor...</div></div>

</div>
<div class="rbar"><button class="btn" onclick="rf()">🔄 Yenile</button><div class="lu" id="lu"></div></div>

<script>
var CP='daily';
function go(n){
  var tabs=['overview','crit','pos','trades','logs'];
  document.querySelectorAll('.ni').forEach(function(t,i){t.classList.toggle('a',tabs[i]===n)});
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('a')});
  document.getElementById('p-'+n).classList.add('a');
  if(n==='logs')ll();
  if(n==='crit')lc();
  if(n==='trades')lt();
}
function sp(p){
  CP=p;
  document.querySelectorAll('.pt').forEach(function(t){t.classList.toggle('a',t.textContent.toLowerCase().includes(p==='daily'?'gün':p==='weekly'?'hafta':'ay'))});
  renderCrit();
}
var CD={};
async function lc(){
  try{
    var r=await fetch('/api/criteria');CD=await r.json();renderCrit();
  }catch(e){document.getElementById('critBars').innerHTML='<div class="empty">Veri yüklenemedi</div>'}
}
function renderCrit(){
  var d=CD[CP]||[];
  var sc=CD[CP+'_scans']||0;
  var info=document.getElementById('cinfo');
  var pn=CP==='daily'?'Bugün':CP==='weekly'?'Son 7 Gün':'Son 30 Gün';
  info.textContent=pn+' | '+sc+' tarama | Toplam: '+(CD.total_scans||0)+' kayıt';
  var box=document.getElementById('critBars');
  if(!d||d.length===0){box.innerHTML='<div class="empty">Bu dönem için veri yok.<br>Tracker çalışınca dolacak.</div>';return}
  box.innerHTML=d.map(function(c){
    var color=c.avg_pct>90?'var(--red)':c.avg_pct>70?'var(--amber)':c.avg_pct>40?'var(--blue)':'var(--green)';
    return '<div class="ci"><span class="cn">'+c.name+'</span><div class="cbg"><div class="cf" style="width:'+c.avg_pct+'%;background:'+color+'">'+c.avg_pct+'%</div></div></div>'
  }).join('');
}
async function ls(){
  try{
    var r=await fetch('/api/status');var d=await r.json();
    var sb=document.getElementById('sb'),sd=document.getElementById('sd'),st=document.getElementById('stx');
    if(d.running){sb.className='sb on';sd.className='dot on';st.textContent='Aktif'}
    else{sb.className='sb off';sd.className='dot off';st.textContent='Durmuş!'}
    document.getElementById('bal').textContent=d.balance_free!==null?d.balance_free.toFixed(2)+' $':'N/A';
    document.getElementById('tbal').textContent=d.balance_total!==null?d.balance_total.toFixed(2)+' $':'N/A';
    document.getElementById('pcc').textContent=d.position_count+'/'+d.max_positions;
    document.getElementById('pm').textContent=d.position_count>0?'aktif':'boş';
    document.getElementById('lev').textContent=d.leverage+'x';
    document.getElementById('sp').textContent=d.size_pct;
    var ev=document.getElementById('es'),em=document.getElementById('em');
    if(d.exec_enabled==='true'){ev.textContent='CANLI';ev.className='cv g';em.textContent='oto trade aktif'}
    else{ev.textContent='KAPALI';ev.className='cv r';em.textContent='sadece sinyal'}
    var pl=document.getElementById('posList');
    if(d.positions.length===0){pl.innerHTML='<div class="empty">Açık pozisyon yok</div>'}
    else{pl.innerHTML=d.positions.map(function(p){
      var s=p.side==='SHORT'?'short':'long';
      return '<div class="pc '+s+'"><div class="pt2"><span class="psym">'+p.symbol.replace('/USDT:USDT','')+'</span><span class="pbdg '+(s==='long'?'l':'s')+'">'+p.side+'</span></div><div class="pg"><div><div class="pgl">Giriş</div><div class="pgv">'+p.entry+'</div></div><div><div class="pgl">Stop Loss</div><div class="pgv r">'+p.sl+'</div></div><div><div class="pgl">Margin</div><div class="pgv">'+(p.margin?p.margin.toFixed(2)+'$':'-')+'</div></div></div><div class="tpr"><span class="tpi">TP1: '+(p.tp1_hit?'✅':'⏳ '+p.tp1_price)+'</span><span class="tpi">TP2: '+(p.tp2_hit?'✅':'⏳ '+(p.tp2_price||'-'))+'</span></div></div>'
    }).join('')}
    document.getElementById('lu').textContent='Son: '+d.time;
  }catch(e){document.getElementById('stx').textContent='Bağlantı hatası'}
}
async function lt(){
  try{
    var r=await fetch('/api/trades');var d=await r.json();
    var s=d.summary;
    document.getElementById('tpnl').textContent=(s.total_pnl>=0?'+':'')+s.total_pnl.toFixed(2)+' $';
    document.getElementById('tpnl').className='cv '+(s.total_pnl>=0?'g':'r');
    document.getElementById('twr').textContent='WR: '+s.total_winrate+'% ('+s.total_trades+' trade)';
    var ms=document.getElementById('monthlySummary');
    if(d.monthly.length>0){
      ms.innerHTML='<div class="section"><div class="st">Aylık Rapor</div>'+d.monthly.map(function(m){
        var c=m.pnl>=0?'g':'r';
        return '<div class="mc"><div class="mh"><span class="mn">'+m.month+'</span><span class="mpnl '+c+'">'+(m.pnl>=0?'+':'')+m.pnl.toFixed(2)+' $</span></div><div class="ms"><div><div class="msl">İşlem</div><div class="msv">'+m.trades+'</div></div><div><div class="msl">Kazanç</div><div class="msv g">'+m.wins+'W / '+m.losses+'L</div></div><div><div class="msl">Winrate</div><div class="msv '+(m.winrate>=50?'g':'am')+'">'+m.winrate+'%</div></div></div></div>'
      }).join('')+'</div>'
    }
    var tl=document.getElementById('tradeList');
    if(d.trades.length===0){tl.innerHTML='<div class="empty">İşlem geçmişi yok</div>'}
    else{tl.innerHTML=d.trades.slice(0,20).map(function(t){
      var c=t.pnl>=0?'g':'r';
      var dt='';try{var dd=new Date(parseInt(t.closed_at));dt=dd.toLocaleDateString('tr-TR',{day:'2-digit',month:'short'})}catch(e){}
      return '<div class="tr"><div><div class="tsym">'+t.symbol.replace('USDT','')+'</div><div class="tsd">'+t.side+' '+t.leverage+'x</div></div><div class="tpnl '+c+'">'+(t.pnl>=0?'+':'')+t.pnl.toFixed(3)+' $</div><div class="tdt">'+dt+'</div></div>'
    }).join('')}
  }catch(e){}
}
async function ll(){
  try{var r=await fetch('/api/logs');var t=await r.text();var b=document.getElementById('lg');b.textContent=t;b.scrollTop=b.scrollHeight}catch(e){}}
function rf(){var b=document.querySelector('.btn');b.disabled=true;b.textContent='⏳';Promise.all([ls(),lt(),lc()]).then(function(){b.disabled=false;b.textContent='🔄 Yenile'})}
ls();lt();lc();setInterval(ls,60000);setInterval(function(){lt();lc()},300000);
</script>
</body>
</html>"""

if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"TCE Dashboard: http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
