/**
 * 탭을 전환한다.
 *
 * 카프카 탭만 2초마다 서버를 조회하므로, 다른 탭으로 옮기면 조회를 멈춘다.
 * 안 멈추면 실험 중인 서버에 계속 요청이 가서 측정에 영향을 준다.
 */
function switchTab(tabName) {
  const btn = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
  const panel = document.getElementById('panel-' + tabName);
  // 버튼과 패널의 짝이 안 맞으면 화면이 빈 채로 남는다. 조용히 넘기지 않는다.
  if (!btn || !panel) {
    throw new Error(`그런 탭이 없습니다: ${tabName}`);
  }

  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  panel.classList.add('active');

  if (tabName === 'kafka') kfStartPolling();
  else kfStopPolling();
}

function bindTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });
}

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
bindTabs();
initPerfCharts();
initCompareCharts();
initKafkaTab();
fetchServerInfo();
detectDbDriver();
setInterval(fetchServerInfo, 5000);
