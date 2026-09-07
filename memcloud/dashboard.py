"""Single-file dashboard. Same design tokens as the OpenFabric UI
(dark #0D1117, teal #00C9A7 accent, JetBrains Mono) but with no build step,
so it ships as one Python string and works offline on both laptops."""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>MemCloud</title>
<style>
:root{
  --bg-primary:#0D1117; --bg-secondary:#161B22; --bg-tertiary:#1C2128;
  --bg-card:#1C2128; --bg-card-hover:#21262D;
  --accent:#00C9A7; --accent-dim:rgba(0,201,167,.15); --accent-glow:rgba(0,201,167,.4);
  --text-primary:#E6EDF3; --text-secondary:#A8B2C1; --text-muted:#6E7681;
  --border:rgba(240,246,252,.1); --border-accent:rgba(0,201,167,.3);
  --online:#00C9A7; --offline:#4A5568; --warning:#F6C90E; --danger:#FF6B6B;
  --peer:#7C9CF5; --disk:#F6C90E;
  --radius-md:10px; --radius-lg:16px; --radius-full:9999px;
  --mono:'JetBrains Mono','Fira Code',ui-monospace,SFMono-Regular,Menlo,monospace;
  --sans:'Inter',system-ui,-apple-system,sans-serif;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg-primary);color:var(--text-primary);font-family:var(--sans);
     font-size:14px;line-height:1.5;padding:20px 24px 60px}
h1{font-size:20px;letter-spacing:-.4px}
h2{font-size:12px;text-transform:uppercase;letter-spacing:1.2px;color:var(--text-muted);
   font-weight:600;margin-bottom:12px}
.mono{font-family:var(--mono)}
header{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:22px;
       padding-bottom:16px;border-bottom:1px solid var(--border)}
.logo{width:30px;height:30px;border-radius:8px;background:var(--accent-dim);
      border:1px solid var(--border-accent);display:grid;place-items:center;
      color:var(--accent);font-family:var(--mono);font-weight:600;font-size:15px}
.sub{color:var(--text-muted);font-size:12px;font-family:var(--mono)}
.spacer{flex:1}
.pill{font-family:var(--mono);font-size:11px;padding:4px 10px;border-radius:var(--radius-full);
      border:1px solid var(--border);color:var(--text-secondary);background:var(--bg-tertiary)}
.pill.ok{color:var(--accent);border-color:var(--border-accent);background:var(--accent-dim)}
.pill.warn{color:var(--warning);border-color:rgba(246,201,14,.3);background:rgba(246,201,14,.12)}

.grid{display:grid;gap:16px}
.g3{grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.g2{grid-template-columns:repeat(auto-fit,minmax(380px,1fr))}
.card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-lg);
      padding:16px 18px}
.card.accent{border-color:var(--border-accent);box-shadow:0 0 24px rgba(0,201,167,.08)}

.node{display:flex;flex-direction:column;gap:8px}
.node-top{display:flex;align-items:center;gap:9px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--offline);flex:none}
.dot.on{background:var(--online);box-shadow:0 0 8px var(--accent-glow)}
.node-name{font-weight:600;font-size:14px}
.tag{font-family:var(--mono);font-size:10px;padding:2px 7px;border-radius:var(--radius-full);
     background:var(--bg-tertiary);border:1px solid var(--border);color:var(--text-muted)}
.tag.self{color:var(--accent);border-color:var(--border-accent)}

.bar{height:9px;background:var(--bg-primary);border-radius:var(--radius-full);
     overflow:hidden;display:flex;border:1px solid var(--border)}
.bar span{height:100%;display:block;transition:width .4s ease}
.seg-used{background:var(--text-muted)}
.seg-mc{background:var(--accent)}
.legend{display:flex;gap:14px;font-family:var(--mono);font-size:11px;color:var(--text-muted);
        flex-wrap:wrap}
.legend i{width:8px;height:8px;border-radius:2px;display:inline-block;margin-right:5px}

.kv{display:flex;justify-content:space-between;font-family:var(--mono);font-size:12px;
    padding:3px 0}
.kv b{color:var(--text-primary);font-weight:500}
.kv span{color:var(--text-muted)}

.big{font-family:var(--mono);font-size:26px;font-weight:600;letter-spacing:-1px}
.big.local{color:var(--accent)} .big.peer{color:var(--peer)} .big.disk{color:var(--disk)}
.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;text-align:center}
.metric{background:var(--bg-tertiary);border:1px solid var(--border);border-radius:var(--radius-md);
        padding:12px 8px}
.metric small{display:block;color:var(--text-muted);font-size:10px;text-transform:uppercase;
              letter-spacing:.8px;margin-top:4px}

table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:12px}
th{text-align:left;color:var(--text-muted);font-weight:500;font-size:10px;
   text-transform:uppercase;letter-spacing:.8px;padding:6px 8px;border-bottom:1px solid var(--border)}
td{padding:5px 8px;border-bottom:1px solid rgba(240,246,252,.04)}
tr:hover td{background:var(--bg-card-hover)}
.src{font-weight:600}
.src.LOCAL{color:var(--accent)} .src.PEER{color:var(--peer)}
.src.DISK{color:var(--disk)} .src.ERROR{color:var(--danger)}
.scroll{max-height:340px;overflow:auto}
.scroll::-webkit-scrollbar{width:8px} .scroll::-webkit-scrollbar-thumb{background:#30363d;border-radius:4px}

button{font-family:var(--sans);font-size:13px;font-weight:500;padding:8px 14px;
       border-radius:var(--radius-md);border:1px solid var(--border-accent);
       background:var(--accent-dim);color:var(--accent);cursor:pointer;transition:120ms}
button:hover{background:rgba(0,201,167,.25)}
button.ghost{border-color:var(--border);background:var(--bg-tertiary);color:var(--text-secondary)}
button:disabled{opacity:.4;cursor:not-allowed}
input{font-family:var(--mono);font-size:13px;padding:8px 10px;width:88px;
      background:var(--bg-primary);border:1px solid var(--border);border-radius:var(--radius-md);
      color:var(--text-primary)}
label{font-size:12px;color:var(--text-secondary);display:flex;align-items:center;gap:7px}
.controls{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.hint{color:var(--text-muted);font-size:11px;font-family:var(--mono);margin-top:10px}
#preview{width:100%;border-radius:var(--radius-md);border:1px solid var(--border);
         background:var(--bg-primary);display:block;image-rendering:pixelated}
.evt{font-family:var(--mono);font-size:11px;color:var(--text-secondary);
     padding:3px 0;border-bottom:1px solid rgba(240,246,252,.04)}
.evt em{color:var(--accent);font-style:normal}
.mt{margin-top:16px}
</style>
</head>
<body>

<header>
  <div class="logo mono">M</div>
  <div>
    <h1>MemCloud</h1>
    <div class="sub" id="hdr-sub">connecting...</div>
  </div>
  <div class="spacer"></div>
  <div class="pill" id="pill-pressure">pressure: --</div>
  <div class="pill" id="pill-peers">peers: --</div>
  <div class="pill" id="pill-tls" title="data plane transport">tls: --</div>
  <div class="pill ok" id="pill-live">live</div>
</header>

<h2>Cluster nodes &mdash; physical RAM</h2>
<div class="grid g3" id="nodes"></div>

<div class="grid g2 mt">
  <div class="card accent">
    <h2>Image cache</h2>
    <div class="metrics">
      <div class="metric"><div class="big local" id="m-local">0</div><small>local hits</small></div>
      <div class="metric"><div class="big peer" id="m-peer">0</div><small>peer hits</small></div>
      <div class="metric"><div class="big disk" id="m-disk">0</div><small>disk reads</small></div>
    </div>
    <div class="mt">
      <div class="kv"><span>avg local RAM</span><b id="l-local">--</b></div>
      <div class="kv"><span>avg peer RAM</span><b id="l-peer">--</b></div>
      <div class="kv"><span>avg disk</span><b id="l-disk">--</b></div>
      <div class="kv"><span>RAM hit rate</span><b id="l-hit">--</b></div>
      <div class="kv"><span>bytes in local RAM</span><b id="l-lb">--</b></div>
      <div class="kv"><span>bytes in peer RAM</span><b id="l-rb">--</b></div>
      <div class="kv"><span>checksum failures</span><b id="l-corrupt">0</b></div>
    </div>
  </div>

  <div class="card">
    <h2>Controls</h2>
    <div class="controls">
      <label>frames <input id="in-frames" type="number" value="400" min="1"></label>
      <button id="btn-seed">Seed cache</button>
      <button class="ghost" id="btn-reset">Reset</button>
    </div>
    <div class="controls mt">
      <label>reads <input id="in-reads" type="number" value="120" min="1"></label>
      <button id="btn-read">Run reads</button>
      <button class="ghost" id="btn-spill">Spill 25% to peers</button>
    </div>
    <div class="hint" id="hint">Seed, then run reads. Overflow goes to peer RAM automatically.</div>
    <div class="mt">
      <h2>Live events</h2>
      <div class="scroll" id="events" style="max-height:150px"></div>
    </div>
  </div>
</div>

<div class="grid g2 mt">
  <div class="card">
    <h2>Recent reads</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>frame</th><th>source</th><th>where</th><th>latency</th><th>ok</th></tr></thead>
        <tbody id="reads"></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <h2>Frame preview &mdash; bytes fetched through MemCloud</h2>
    <img id="preview" alt="frame preview"/>
    <div class="hint" id="prev-note">Runs a real read; if the frame lives on a peer, these pixels crossed the network.</div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
const fmt = b => { if(b==null) return '--'; const u=['B','KB','MB','GB','TB']; let i=0,n=b;
  while(n>=1024&&i<u.length-1){n/=1024;i++;} return i===0?n+' B':n.toFixed(1)+' '+u[i]; };
const ms = v => v==null ? '--' : v.toFixed(2)+' ms';

function nodeCard(n, self){
  const pct = n.ram_percent||0;
  // MemCloud bytes are already counted inside system-used, so draw them as a
  // slice OF the used bar rather than on top of it.
  const mcPct = n.ram_total ? Math.min(pct, 100*(n.memcloud_bytes||0)/n.ram_total) : 0;
  const usedPct = Math.max(0, pct - mcPct);
  return `<div class="card node">
    <div class="node-top">
      <span class="dot ${n.status==='online'?'on':''}"></span>
      <span class="node-name">${n.name}</span>
      ${self?'<span class="tag self">this node</span>':''}
      ${n.static?'<span class="tag">manual</span>':''}
      <div class="spacer"></div>
      <span class="tag">${n.rtt_ms?n.rtt_ms.toFixed(1)+' ms':'local'}</span>
    </div>
    <div class="bar">
      <span class="seg-used" style="width:${usedPct}%"></span>
      <span class="seg-mc" style="width:${mcPct}%"></span>
    </div>
    <div class="legend">
      <span><i class="seg-used" style="background:var(--text-muted)"></i>system ${pct.toFixed(1)}%</span>
      <span><i class="seg-mc" style="background:var(--accent)"></i>memcloud ${fmt(n.memcloud_bytes||0)}</span>
    </div>
    <div class="kv"><span>total RAM</span><b>${fmt(n.ram_total)}</b></div>
    <div class="kv"><span>available</span><b>${fmt(n.ram_available)}</b></div>
    <div class="kv"><span>hosting for peers</span><b>${fmt(n.hosted_bytes||0)} (${n.hosted_blocks||0})</b></div>
    <div class="kv"><span>process RSS</span><b>${fmt(n.process_rss||0)}</b></div>
  </div>`;
}

async function refresh(){
  try{
    const r = await fetch('/api/state'); const s = await r.json();
    $('hdr-sub').textContent = `${s.self.name} · data :${s.self.data_port} · api :${s.self.api_port} · cluster "${s.self.cluster}"`;
    const online = s.nodes.filter(n=>n.status==='online' && !n.self).length;
    $('pill-peers').textContent = `peers: ${online}`;
    const sec = s.security||{};
    const tp = $('pill-tls');
    tp.textContent = sec.tls ? 'mTLS' : 'PLAINTEXT';
    tp.className = 'pill ' + (sec.tls ? 'ok' : 'warn');
    tp.title = sec.tls ? ('mutual TLS · cluster cert ' + (sec.fingerprint||'').slice(0,23) + '…')
                       : 'data plane is unencrypted (--insecure)';
    const p = $('pill-pressure');
    p.textContent = 'pressure: ' + (s.pressure.active ? 'YES' : 'no');
    p.className = 'pill ' + (s.pressure.active ? 'warn' : 'ok');
    $('nodes').innerHTML = s.nodes.map(n=>nodeCard(n, n.self)).join('');

    const c = s.cache.counters, L = s.cache.latency;
    $('m-local').textContent = c.local_hits;
    $('m-peer').textContent  = c.peer_hits;
    $('m-disk').textContent  = c.disk_reads;
    $('l-local').textContent = ms(L.LOCAL.avg_ms);
    $('l-peer').textContent  = ms(L.PEER.avg_ms);
    $('l-disk').textContent  = ms(L.DISK.avg_ms);
    $('l-hit').textContent   = s.cache.hit_rate_ram + ' %';
    $('l-lb').textContent    = fmt(s.cache.memcloud.local_bytes) + ' / ' + fmt(s.cache.memcloud.local_budget);
    $('l-rb').textContent    = fmt(s.cache.memcloud.remote_bytes);
    $('l-corrupt').textContent = c.corrupt;

    $('reads').innerHTML = s.cache.recent.map(e=>`<tr>
        <td>${e.frame}</td>
        <td class="src ${e.source}">${e.source}</td>
        <td>${e.detail||''}</td>
        <td>${e.ms!=null?e.ms.toFixed(2)+' ms':''}</td>
        <td>${e.verified===undefined?'':(e.verified?'&#10003;':'&#10007;')}</td>
      </tr>`).join('');

    if(s.cache.seeding){
      $('hint').textContent = `seeding ${s.cache.seed_progress.done}/${s.cache.seed_progress.total} frames...`;
    }
  }catch(e){ $('pill-live').className='pill warn'; $('pill-live').textContent='reconnecting'; }
}

function ev(msg){
  const d=document.createElement('div'); d.className='evt'; d.innerHTML=msg;
  const box=$('events'); box.insertBefore(d, box.firstChild);
  while(box.children.length>40) box.removeChild(box.lastChild);
}

async function post(url, note){
  ev(note);
  const r = await fetch(url,{method:'POST'});
  const j = await r.json();
  ev('<em>&rarr;</em> ' + JSON.stringify(j).slice(0,220));
  refresh();
  return j;
}

$('btn-seed').onclick = () => post('/api/demo/seed?frames='+$('in-frames').value, 'seeding frames...');
$('btn-read').onclick = async () => {
  await post('/api/demo/reads?count='+$('in-reads').value, 'running reads...');
  $('preview').src = '/api/frame/random?t=' + Date.now();
};
$('btn-spill').onclick = () => post('/api/demo/spill?fraction=0.25', 'spilling LRU blocks to peer RAM...');
$('btn-reset').onclick = () => post('/api/demo/reset', 'resetting cache...');

const es = new EventSource('/api/events');
es.onmessage = m => { try{ const d=JSON.parse(m.data);
  ev('<em>'+d.event+'</em> '+JSON.stringify(d.data).slice(0,180)); }catch(e){} };

refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>
"""
