// 탭 전환
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('panel-' + btn.dataset.tab).classList.add('active');
  });
});

// 서버 정보
async function fetchServerInfo() {
  try {
    const [ping, stats] = await Promise.all([
      fetch(`${API_BASE}/load-test/ping`).then(r => r.json()),
      fetch(`${API_BASE}/queue-stats`).then(r => r.json()),
    ]);
    document.getElementById('statusDot').className = 'status-dot';
    document.getElementById('statusText').textContent = `엔진: ${ping.engine}`;
    const mem = stats.memory;
    document.getElementById('serverInfo').innerHTML = `힙: ${mem.heapUsed}/${mem.heapTotal} MB<br>RSS: ${mem.rss} MB<br>활성: ${stats.activeRequests}<br>처리됨: ${stats.totalProcessed}<br>가동: ${Math.floor(stats.uptime)}초`;
  } catch {
    document.getElementById('statusDot').className = 'status-dot offline';
    document.getElementById('statusText').textContent = '오프라인';
  }
}

// DB 드라이버 감지
async function detectDbDriver() {
  try {
    const ping = await fetch(`${API_BASE}/load-test/ping`).then(r => r.json());
    const driver = ping.dbDriver || 'sqlite';
    const label = driver === 'postgresql' ? 'PostgreSQL' : 'SQLite WAL';
    const readLabel = driver === 'postgresql' ? 'PostgreSQL' : 'SQLite 쿼리';
    document.getElementById('dbWriteDesc').textContent = label;
    document.getElementById('dbReadDesc').textContent = readLabel;
  } catch {
    document.getElementById('dbWriteDesc').textContent = 'SQLite WAL';
    document.getElementById('dbReadDesc').textContent = 'SQLite 쿼리';
  }
}

// 초기화
initPerfCharts();
initCompareCharts();
fetchServerInfo();
detectDbDriver();
setInterval(fetchServerInfo, 5000);
