let cmpLineChart, cmpBarChart;
let selectedCmpType = 'cpu';

function selectCmpType(type) {
  if (isRunning) return;
  selectedCmpType = type;
  document.querySelectorAll('.test-type-btn[data-cmp-type]').forEach(b => b.classList.remove('active'));
  document.querySelector(`.test-type-btn[data-cmp-type="${type}"]`).classList.add('active');
  document.getElementById('cmpMaxRow').style.display = type === 'cpu' ? 'flex' : 'none';
  document.getElementById('cmpDelayRow').style.display = type === 'io' ? 'flex' : 'none';

  const howIt = document.getElementById('cmpHowItWorks');
  if (type === 'cpu') {
    document.getElementById('cmpNodeTitle').textContent = 'Node (Worker Thread)';
    document.getElementById('cmpGoTitle').textContent = 'Go (Goroutine)';
    howIt.innerHTML = '매 라운드마다 동일한<br><strong>findPrimes(max)</strong> 작업을<br>양쪽에서 실행합니다:<br><br><span style="color:var(--accent-light);">Node</span> = Worker Thread<br><span style="color:var(--green);">Go</span> = gRPC + Goroutine<br><br>라운드별 순차 실행,<br>실제 소요 시간을 측정합니다.';
  } else {
    document.getElementById('cmpNodeTitle').textContent = 'Node (Event Loop)';
    document.getElementById('cmpGoTitle').textContent = 'Go (gRPC + Goroutine)';
    howIt.innerHTML = '매 라운드마다 동일한<br><strong>asyncIO(delay)</strong> 작업을<br>양쪽에서 실행합니다:<br><br><span style="color:var(--accent-light);">Node</span> = setTimeout (이벤트 루프)<br><span style="color:var(--green);">Go</span> = gRPC + time.After<br><br>Node는 로컬 이벤트 루프,<br>Go는 gRPC 네트워크 경유.<br>I/O 오버헤드 차이를 측정합니다.';
  }
}

function initCompareCharts() {
  cmpLineChart = new Chart(document.getElementById('cmpLineChart'), {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'Node', data: [], borderColor: '#6c5ce7', backgroundColor: 'rgba(108,92,231,0.1)', borderWidth: 2, pointRadius: 3, pointBackgroundColor: '#6c5ce7', fill: false, tension: 0.3 },
        { label: 'Go', data: [], borderColor: '#00b894', backgroundColor: 'rgba(0,184,148,0.1)', borderWidth: 2, pointRadius: 3, pointBackgroundColor: '#00b894', fill: false, tension: 0.3 },
      ],
    },
    options: {
      ...chartDefaults,
      plugins: { legend: { display: true, labels: { color: '#8b8fa3', font: { size: 11 } } } },
      scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: 'ms', color: '#8b8fa3' } } },
      animation: false,
    },
  });

  cmpBarChart = new Chart(document.getElementById('cmpBarChart'), {
    type: 'bar',
    data: {
      labels: ['평균', 'P50', 'P95', 'P99', '최소', '최대'],
      datasets: [
        { label: 'Node', data: [0,0,0,0,0,0], backgroundColor: 'rgba(108,92,231,0.6)', borderRadius: 4 },
        { label: 'Go', data: [0,0,0,0,0,0], backgroundColor: 'rgba(0,184,148,0.6)', borderRadius: 4 },
      ],
    },
    options: {
      ...chartDefaults,
      plugins: { legend: { display: true, labels: { color: '#8b8fa3', font: { size: 11 } } } },
      scales: { ...chartDefaults.scales, y: { ...chartDefaults.scales.y, title: { display: true, text: 'ms (↓ 낮을수록 빠름)', color: '#8b8fa3' }, beginAtZero: true } },
    },
  });
}

async function startCompare() {
  if (isRunning) return;
  isRunning = true;
  const btn = document.getElementById('cmpRunBtn');
  btn.textContent = '실행 중...'; btn.classList.add('running'); btn.disabled = true;
  document.getElementById('cmpProgressSection').style.display = 'block';
  updateProgressBar('cmpProgressFill', 'cmpProgressLabel', 'cmpProgressPercent', 0, 1);

  cmpLineChart.data.labels = [];
  cmpLineChart.data.datasets[0].data = [];
  cmpLineChart.data.datasets[1].data = [];
  cmpLineChart.update();

  const count = parseInt(document.getElementById('cmpCount').value) || 20;
  const max = parseInt(document.getElementById('cmpMax').value) || 500000;
  const delayMs = parseInt(document.getElementById('cmpDelay').value) || 100;

  const typeLabel = selectedCmpType === 'io' ? 'I/O' : 'CPU';
  logTo('cmpLogArea', `Node vs Go ${typeLabel} 비교 시작: ${count}라운드`, 'log-info');

  try {
    const res = await fetch(`${API_BASE}/load-test/compare-stream`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count, max, testType: selectedCmpType, delayMs }),
    });

    await readSseStream(res, msg => {
      if (msg.event === 'error') {
        logTo('cmpLogArea', msg.message, 'log-fail');
      }

      if (msg.event === 'progress') {
        updateProgressBar('cmpProgressFill', 'cmpProgressLabel', 'cmpProgressPercent', msg.index + 1, msg.total);
        cmpLineChart.data.labels.push(msg.index);
        cmpLineChart.data.datasets[0].data.push(msg.nodeMs);
        cmpLineChart.data.datasets[1].data.push(msg.goMs);
        cmpLineChart.update();

        document.getElementById('cmpNodeWins').textContent = msg.nodeWins;
        document.getElementById('cmpGoWins').textContent = msg.goWins;

        const winCls = msg.winner === 'node' ? 'log-ok' : msg.winner === 'go' ? 'log-warn' : 'log-fail';
        const winIcon = msg.winner === 'node' ? 'NODE' : msg.winner === 'go' ? 'GO  ' : 'ERR ';
        if (msg.index % Math.max(1, Math.floor(msg.total / 30)) === 0 || msg.winner === 'error') {
          logTo('cmpLogArea', `#${msg.index} [${winIcon}]  Node=${formatMs(msg.nodeMs)}  Go=${formatMs(msg.goMs)}  (${msg.nodeWins}:${msg.goWins})`, winCls);
        }
      }

      if (msg.event === 'done') {
        const n = msg.node, g = msg.go;
        document.getElementById('cmpNodeAvg').textContent = formatMs(n.avgMs);
        document.getElementById('cmpNodeP50').textContent = formatMs(n.p50);
        document.getElementById('cmpNodeP95').textContent = formatMs(n.p95);
        document.getElementById('cmpNodeP99').textContent = formatMs(n.p99);
        document.getElementById('cmpGoAvg').textContent = formatMs(g.avgMs);
        document.getElementById('cmpGoP50').textContent = formatMs(g.p50);
        document.getElementById('cmpGoP95').textContent = formatMs(g.p95);
        document.getElementById('cmpGoP99').textContent = formatMs(g.p99);

        cmpBarChart.data.datasets[0].data = [n.avgMs, n.p50, n.p95, n.p99, n.min, n.max];
        cmpBarChart.data.datasets[1].data = [g.avgMs, g.p50, g.p95, g.p99, g.min, g.max];
        cmpBarChart.update();

        const badge = document.getElementById('cmpOverallBadge');
        if (msg.nodeWins > msg.goWins) {
          badge.textContent = 'Node 승리'; badge.className = 'win-badge win-node';
        } else if (msg.goWins > msg.nodeWins) {
          badge.textContent = 'Go 승리'; badge.className = 'win-badge win-go';
        } else {
          badge.textContent = '무승부'; badge.className = 'win-badge win-tie';
        }

        document.getElementById('cmpHistoryEmpty').style.display = 'none';
        const winner = msg.nodeWins > msg.goWins ? 'Node' : msg.goWins > msg.nodeWins ? 'Go' : '무승부';
        const wc = winner === 'Node' ? 'badge-node' : winner === 'Go' ? 'badge-go' : '';
        const row = document.createElement('tr');
        row.innerHTML = `<td>${msg.task}</td><td>${msg.total}</td><td>${msg.nodeWins}</td><td>${msg.goWins}</td><td>${formatMs(n.avgMs)}</td><td>${formatMs(g.avgMs)}</td><td><span class="badge ${wc}">${winner}</span></td>`;
        const hb = document.getElementById('cmpHistoryBody');
        hb.insertBefore(row, hb.firstChild);

        logTo('cmpLogArea', '', '');
        logTo('cmpLogArea', `완료 (${msg.totalMs}ms) — Node ${msg.nodeWins} : ${msg.goWins} Go (오류 ${msg.errors}건)`, 'log-info');
        logTo('cmpLogArea', `  Node: 평균=${formatMs(n.avgMs)} p50=${formatMs(n.p50)} p95=${formatMs(n.p95)} p99=${formatMs(n.p99)}`, 'log-warn');
        logTo('cmpLogArea', `  Go:   평균=${formatMs(g.avgMs)} p50=${formatMs(g.p50)} p95=${formatMs(g.p95)} p99=${formatMs(g.p99)}`, 'log-warn');
      }
    });
  } catch (err) { logTo('cmpLogArea', `오류: ${err.message}`, 'log-fail'); }

  isRunning = false; btn.textContent = '비교 실행'; btn.classList.remove('running'); btn.disabled = false;
}
