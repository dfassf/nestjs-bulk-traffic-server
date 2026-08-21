const API_BASE = '';
let isRunning = false;

function logTo(areaId, msg, cls = '') {
  const area = document.getElementById(areaId);
  const ts = new Date().toLocaleTimeString('ko', { hour12: false, fractionalSecondDigits: 1 });
  const line = document.createElement('div');
  line.className = 'log-line ' + cls;
  line.textContent = `[${ts}] ${msg}`;
  area.appendChild(line);
  area.scrollTop = area.scrollHeight;
}

// 지연 지표는 성공 표본이 없으면 null 로 온다. 0ms 로 보이면 가장 좋은 성능처럼
// 읽히기 때문에 서버가 0 으로 메우지 않고, 화면에서 측정 불가로 표시한다.
function formatMs(value) {
  return value === null || value === undefined ? '측정 불가' : `${value}ms`;
}

// 통계 카드처럼 숫자와 단위를 나눠 그리는 자리용.
function formatStatValue(value, unit) {
  return value === null || value === undefined
    ? '<span class="unit">측정 불가</span>'
    : `${value}<span class="unit">${unit}</span>`;
}

function updateProgressBar(fillId, labelId, pctId, current, total) {
  const pct = Math.round((current / total) * 100);
  document.getElementById(fillId).style.width = pct + '%';
  document.getElementById(labelId).textContent = `${current} / ${total}`;
  document.getElementById(pctId).textContent = pct + '%';
}

async function readSseStream(res, onMessage) {
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const raw = line.slice(6).trim();
      if (!raw) continue;
      try { onMessage(JSON.parse(raw)); } catch {}
    }
  }
}

const chartDefaults = {
  responsive: true, maintainAspectRatio: false,
  plugins: { legend: { display: false } },
  scales: {
    x: { grid: { color: '#2a2d3a' }, ticks: { color: '#8b8fa3', font: { size: 10 } } },
    y: { grid: { color: '#2a2d3a' }, ticks: { color: '#8b8fa3', font: { size: 10 } } },
  },
};

function buildHistogram(chart, durations, color) {
  if (durations.length === 0) return;
  const sorted = [...durations].sort((a, b) => a - b);
  const mn = sorted[0], mx = sorted[sorted.length - 1];
  const bc = Math.min(20, Math.max(5, Math.ceil(Math.sqrt(sorted.length))));
  const step = Math.max(0.1, (mx - mn) / bc);
  const labels = [], counts = [];
  for (let i = 0; i < bc; i++) {
    const lo = mn + i * step;
    labels.push(lo.toFixed(1));
    counts.push(sorted.filter(v => v >= lo && (i === bc - 1 ? v <= lo + step : v < lo + step)).length);
  }
  chart.data.labels = labels;
  chart.data.datasets[0].data = counts;
  if (color) chart.data.datasets[0].backgroundColor = color;
  chart.update();
}
