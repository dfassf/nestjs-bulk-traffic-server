let selectedType = 'cpu';
let latencyChart, histogramChart, memoryChart, queueChart;
let liveLatencies = [];
let resourcePollInterval = null;
const MAX_RESOURCE_POINTS = 60;

function initPerfCharts() {
  latencyChart = new Chart(document.getElementById('latencyChart'), {
    type: 'line',
    data: { labels: [], datasets: [{ data: [], borderColor: '#6c5ce7', backgroundColor: 'rgba(108,92,231,0.1)', borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.3 }] },
    options: { ...chartDefaults, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: 'ms', color: '#8b8fa3' } } }, animation: false },
  });
  histogramChart = new Chart(document.getElementById('histogramChart'), {
    type: 'bar',
    data: { labels: [], datasets: [{ data: [], backgroundColor: 'rgba(108,92,231,0.6)', borderRadius: 4 }] },
    options: { ...chartDefaults, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: '건수', color: '#8b8fa3' }, beginAtZero: true } } },
  });
  memoryChart = new Chart(document.getElementById('memoryChart'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: 'Heap Used', data: [], borderColor: '#e17055', backgroundColor: 'rgba(225,112,85,0.1)', borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.3 },
      { label: 'RSS', data: [], borderColor: '#74b9ff', backgroundColor: 'rgba(116,185,255,0.1)', borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
    ] },
    options: { ...chartDefaults, plugins: { legend: { display: true, labels: { color: '#8b8fa3', font: { size: 10 } } } }, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, beginAtZero: true, title: { display: true, text: 'MB', color: '#8b8fa3' } } }, animation: false },
  });
  queueChart = new Chart(document.getElementById('queueChart'), {
    type: 'line',
    data: { labels: [], datasets: [
      { label: '큐 길이', data: [], borderColor: '#fdcb6e', backgroundColor: 'rgba(253,203,110,0.1)', borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.3 },
      { label: '활성 요청', data: [], borderColor: '#00b894', backgroundColor: 'rgba(0,184,148,0.1)', borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
    ] },
    options: { ...chartDefaults, plugins: { legend: { display: true, labels: { color: '#8b8fa3', font: { size: 10 } } } }, scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, beginAtZero: true } }, animation: false },
  });
}

function startResourcePolling() {
  if (resourcePollInterval) return;
  memoryChart.data.labels = []; memoryChart.data.datasets[0].data = []; memoryChart.data.datasets[1].data = [];
  queueChart.data.labels = []; queueChart.data.datasets[0].data = []; queueChart.data.datasets[1].data = [];
  resourcePollInterval = setInterval(async () => {
    try {
      const stats = await fetch(`${API_BASE}/queue-stats`).then(r => r.json());
      const ts = new Date().toLocaleTimeString('ko', { hour12: false, second: '2-digit' });
      memoryChart.data.labels.push(ts);
      memoryChart.data.datasets[0].data.push(stats.memory.heapUsed);
      memoryChart.data.datasets[1].data.push(stats.memory.rss);
      if (memoryChart.data.labels.length > MAX_RESOURCE_POINTS) { memoryChart.data.labels.shift(); memoryChart.data.datasets[0].data.shift(); memoryChart.data.datasets[1].data.shift(); }
      memoryChart.update();
      queueChart.data.labels.push(ts);
      queueChart.data.datasets[0].data.push(stats.totalQueueLength || 0);
      queueChart.data.datasets[1].data.push(stats.activeRequests || 0);
      if (queueChart.data.labels.length > MAX_RESOURCE_POINTS) { queueChart.data.labels.shift(); queueChart.data.datasets[0].data.shift(); queueChart.data.datasets[1].data.shift(); }
      queueChart.update();
      document.getElementById('statRejected').textContent = stats.totalRejected || 0;
      document.getElementById('statTimeout').textContent = stats.totalTimeout || 0;
      document.getElementById('statProcessed').textContent = stats.totalProcessed || 0;
    } catch {}
  }, 1000);
}

function stopResourcePolling() {
  if (resourcePollInterval) { clearInterval(resourcePollInterval); resourcePollInterval = null; }
}

document.querySelectorAll('.test-type-btn[data-type]').forEach(btn => {
  btn.addEventListener('click', () => {
    if (isRunning) return;
    document.querySelectorAll('.test-type-btn[data-type]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedType = btn.dataset.type;
    document.getElementById('iterationsRow').style.display = selectedType === 'cpu' ? 'flex' : 'none';
    document.getElementById('delayRow').style.display = selectedType === 'io' ? 'flex' : 'none';
  });
});

async function startTest() {
  if (isRunning) return;
  isRunning = true;
  liveLatencies = [];
  const btn = document.getElementById('runBtn');
  btn.textContent = '실행 중...'; btn.classList.add('running'); btn.disabled = true;
  document.getElementById('progressSection').style.display = 'block';
  updateProgressBar('progressFill', 'progressLabel', 'progressPercent', 0, 1);
  latencyChart.data.labels = []; latencyChart.data.datasets[0].data = []; latencyChart.update();
  histogramChart.data.labels = []; histogramChart.data.datasets[0].data = []; histogramChart.update();

  startResourcePolling();

  const count = parseInt(document.getElementById('paramCount').value) || 50;
  const body = { type: selectedType, count, iterations: parseInt(document.getElementById('paramIterations').value) || 1000, delayMs: parseInt(document.getElementById('paramDelay').value) || 100 };
  logTo('logArea', `${selectedType.toUpperCase()} 테스트 시작: ${count}건`, 'log-info');

  try {
    const res = await fetch(`${API_BASE}/load-test/run-stream`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    await readSseStream(res, msg => {
      if (msg.event === 'start') logTo('logArea', `테스트 시작됨: ${msg.type} x ${msg.count}`, 'log-info');
      if (msg.event === 'progress') {
        updateProgressBar('progressFill', 'progressLabel', 'progressPercent', msg.index + 1, msg.total);
        liveLatencies.push(msg.ms);
        latencyChart.data.labels.push(msg.index);
        latencyChart.data.datasets[0].data.push(msg.ms);
        if (latencyChart.data.labels.length > 500) { latencyChart.data.labels.shift(); latencyChart.data.datasets[0].data.shift(); }
        latencyChart.update();
        if (!msg.ok) logTo('logArea', `#${msg.index} 실패 (${msg.ms}ms)`, 'log-fail');
        else if (msg.index % Math.max(1, Math.floor(msg.total / 20)) === 0)
          logTo('logArea', `#${msg.index} 성공 ${msg.ms}ms  [${msg.fulfilled}성공/${msg.rejected}실패]`, 'log-ok');
      }
      if (msg.event === 'done') {
        document.getElementById('statAvg').innerHTML = formatStatValue(msg.avgMs, 'ms');
        document.getElementById('statP95').innerHTML = formatStatValue(msg.p95, 'ms');
        const rps = msg.totalMs > 0 ? Math.round((msg.fulfilled / msg.totalMs) * 1000 * 10) / 10 : 0;
        document.getElementById('statRps').innerHTML = `${rps}<span class="unit">req/s</span>`;
        const sr = msg.total > 0 ? Math.round((msg.fulfilled / msg.total) * 1000) / 10 : 0;
        document.getElementById('statSuccess').innerHTML = `${sr}<span class="unit">%</span>`;
        buildHistogram(histogramChart, liveLatencies);
        document.getElementById('historyEmpty').style.display = 'none';
        const bc = msg.type.startsWith('db') ? 'badge-db' : msg.type === 'mixed' ? 'badge-mixed' : msg.type === 'io' ? 'badge-io' : 'badge-cpu';
        const row = document.createElement('tr');
        row.innerHTML = `<td><span class="badge ${bc}">${msg.type.toUpperCase()}</span></td><td>${msg.total}</td><td>${formatMs(msg.avgMs)}</td><td>${formatMs(msg.p50)}</td><td>${formatMs(msg.p95)}</td><td>${formatMs(msg.p99)}</td><td>${sr}%</td><td>${msg.totalMs}ms</td><td>${rps}</td>`;
        const hb = document.getElementById('historyBody');
        hb.insertBefore(row, hb.firstChild);
        logTo('logArea', '', '');
        logTo('logArea', `완료: ${msg.fulfilled}/${msg.total} 성공 (${msg.totalMs}ms)`, 'log-info');
        logTo('logArea', `  평균=${formatMs(msg.avgMs)}  p50=${formatMs(msg.p50)}  p95=${formatMs(msg.p95)}  p99=${formatMs(msg.p99)}`, 'log-warn');
      }
    });
  } catch (err) { logTo('logArea', `오류: ${err.message}`, 'log-fail'); }
  stopResourcePolling();
  isRunning = false; btn.textContent = '테스트 실행'; btn.classList.remove('running'); btn.disabled = false;
}
