// 카프카 실험 탭
// 주문을 발행하고, 컨슈머를 띄우고 죽이고, 밀린 건수를 본다.
// 실험 목록은 docs/kafka-lab-plan.md 참고.

let kfLagChart = null;
let kfPartitionChart = null;
let kfPollTimer = null;
const kfLagHistory = [];
const KF_MAX_HISTORY = 60;

function initKafkaCharts() {
  const lagCtx = document.getElementById('kfLagChart');
  const partCtx = document.getElementById('kfPartitionChart');
  if (!lagCtx || !partCtx || typeof Chart === 'undefined') return;

  kfLagChart = new Chart(lagCtx, {
    type: 'line',
    data: { labels: [], datasets: [{ label: '밀린 건수', data: [], borderColor: '#e5534b', tension: 0.2, pointRadius: 0 }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });

  kfPartitionChart = new Chart(partCtx, {
    type: 'bar',
    data: { labels: [], datasets: [{ label: '밀린 건수', data: [], backgroundColor: '#d29922' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });
}

function kfLog(message, cls = '') {
  logTo('kfLogArea', message, cls);
}

async function kfRequest(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    // 서버가 왜 거절했는지 그대로 보여준다. 조용히 삼키면 무엇이 잘못됐는지 모른다.
    throw new Error(body.message || `요청 실패 (${res.status})`);
  }
  return body;
}

async function kfPublishOrders() {
  const btn = document.getElementById('kfPublishBtn');
  const count = Number(document.getElementById('kfOrderCount').value);
  const delayMs = Number(document.getElementById('kfOrderDelay').value);

  btn.disabled = true;
  btn.textContent = '발행 중...';
  try {
    const result = await kfRequest('/orders/bulk', {
      method: 'POST',
      body: JSON.stringify({ count, delayMs }),
    });
    const spread = Object.entries(result.partitionCounts)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([p, c]) => `p${p}:${c}`)
      .join(' ');
    kfLog(`주문 ${result.created}건 발행 (${result.elapsedMs}ms) ${spread}`, 'log-ok');
    if (result.failed > 0) {
      kfLog(`발행 실패 ${result.failed}건: ${result.errors.join(' / ')}`, 'log-fail');
    }
  } catch (err) {
    kfLog(`발행 실패: ${err.message}`, 'log-fail');
  } finally {
    btn.disabled = false;
    btn.textContent = '주문 발행';
    kfRefresh();
  }
}

async function kfSpawnConsumer() {
  const body = {
    groupId: document.getElementById('kfGroupId').value.trim(),
    instances: Number(document.getElementById('kfInstances').value),
    processingDelayMs: Number(document.getElementById('kfDelay').value),
    commitMode: document.getElementById('kfCommitMode').value,
    commitDelayMs: Number(document.getElementById('kfCommitDelay').value),
    crashAfter: Number(document.getElementById('kfCrashAfter').value),
    fromBeginning: document.getElementById('kfFromBeginning').checked,
  };

  try {
    const result = await kfRequest('/lab/consumers', {
      method: 'POST',
      body: JSON.stringify(body),
    });
    kfLog(`컨슈머 시작 pid=${result.pid} 그룹=${result.groupId} 인스턴스=${result.instances}`, 'log-ok');
  } catch (err) {
    kfLog(`컨슈머 시작 실패: ${err.message}`, 'log-fail');
  }
  kfRefresh();
}

// SIGKILL 은 커밋할 틈 없이 죽는다. 재시작하면 중복 처리가 관측된다.
// SIGTERM 은 정상 종료라 커밋하고 빠지므로 중복이 안 생긴다.
async function kfStopConsumer(pid, signal) {
  try {
    await kfRequest(`/lab/consumers?pid=${pid}&signal=${signal}`, { method: 'DELETE' });
    const label = signal === 'SIGKILL' ? '강제 종료' : '정상 종료';
    kfLog(`pid=${pid} ${label}`, signal === 'SIGKILL' ? 'log-warn' : 'log-info');
  } catch (err) {
    kfLog(`종료 실패: ${err.message}`, 'log-fail');
  }
  kfRefresh();
}

async function kfStopAll() {
  try {
    const result = await kfRequest('/lab/consumers?signal=SIGTERM', { method: 'DELETE' });
    kfLog(`컨슈머 ${result.stopped}개 정상 종료`, 'log-info');
  } catch (err) {
    kfLog(`종료 실패: ${err.message}`, 'log-fail');
  }
  kfRefresh();
}

async function kfResetOffsets() {
  const topic = document.getElementById('kfResetTopic').value;
  const target = document.getElementById('kfResetTarget').value;
  const groupId = document.getElementById('kfGroupId').value.trim();

  try {
    await kfRequest('/lab/offsets/reset', {
      method: 'POST',
      body: JSON.stringify({ groupId, topic, target }),
    });
    kfLog(`오프셋 되감기 완료 그룹=${groupId} 토픽=${topic} → ${target}`, 'log-ok');
  } catch (err) {
    kfLog(`되감기 실패: ${err.message}`, 'log-fail');
  }
  kfRefresh();
}

async function kfClearOrders() {
  try {
    await kfRequest('/orders', { method: 'DELETE' });
    kfLog('주문·이벤트 기록을 비웠습니다', 'log-info');
    kfLagHistory.length = 0;
    if (kfLagChart) {
      kfLagChart.data.labels = [];
      kfLagChart.data.datasets[0].data = [];
      kfLagChart.update();
    }
  } catch (err) {
    kfLog(`초기화 실패: ${err.message}`, 'log-fail');
  }
  kfRefresh();
}

function kfRenderProcesses(processes) {
  const body = document.getElementById('kfProcessBody');
  const empty = document.getElementById('kfProcessEmpty');
  if (!body) return;

  body.innerHTML = '';
  empty.style.display = processes.length === 0 ? 'block' : 'none';

  for (const proc of processes) {
    const opts = proc.options;
    const detail = [
      opts.commitMode === 'after-process' ? '처리후커밋' : '처리전커밋',
      opts.processingDelayMs > 0 ? `지연${opts.processingDelayMs}ms` : null,
      opts.commitDelayMs > 0 ? `커밋지연${opts.commitDelayMs}ms` : null,
      opts.crashAfter > 0 ? `${opts.crashAfter}건후종료` : null,
      opts.fromBeginning ? '처음부터' : null,
    ].filter(Boolean).join(' ');

    let status;
    if (proc.alive) {
      status = '<span class="badge badge-go">실행 중</span>';
    } else if (proc.exitSignal === 'SIGKILL') {
      // 강제 종료는 커밋을 못 했다는 뜻이라 구분해서 보여준다.
      status = '<span class="badge badge-cpu">강제 종료</span>';
    } else {
      status = `<span class="badge">종료 (${proc.exitCode ?? proc.exitSignal})</span>`;
    }

    const actions = proc.alive
      ? `<button class="run-btn" style="padding:4px 8px;font-size:11px;" onclick="kfStopConsumer(${proc.pid},'SIGKILL')">강제 종료</button>
         <button class="run-btn" style="padding:4px 8px;font-size:11px;background:var(--bg-soft);" onclick="kfStopConsumer(${proc.pid},'SIGTERM')">정상 종료</button>`
      : '-';

    const row = document.createElement('tr');
    row.innerHTML = `<td>${proc.pid}</td><td>${proc.groupId}</td><td>${proc.instances}</td><td style="font-size:11px;">${detail}</td><td>${status}</td><td>${actions}</td>`;
    body.appendChild(row);
  }
}

function kfRenderLag(group) {
  const totalLag = group.topics.reduce((sum, t) => sum + t.totalLag, 0);
  document.getElementById('kfStatLag').textContent = totalLag;

  kfLagHistory.push(totalLag);
  if (kfLagHistory.length > KF_MAX_HISTORY) kfLagHistory.shift();

  if (kfLagChart) {
    kfLagChart.data.labels = kfLagHistory.map((_, i) => i);
    kfLagChart.data.datasets[0].data = [...kfLagHistory];
    kfLagChart.update('none');
  }

  // 주문 생성 토픽의 파티션별 분포를 본다. 어느 파티션이 밀리는지 보인다.
  const ordersTopic = group.topics.find((t) => t.topic === 'orders.created');
  if (kfPartitionChart && ordersTopic) {
    kfPartitionChart.data.labels = ordersTopic.partitions.map((p) => `p${p.partition}`);
    kfPartitionChart.data.datasets[0].data = ordersTopic.partitions.map((p) => p.lag);
    kfPartitionChart.update('none');
  }
}

async function kfRefresh() {
  try {
    const stats = await kfRequest('/orders/stats');
    document.getElementById('kfStatOrders').textContent = stats.orderCount;
    document.getElementById('kfStatEvents').textContent = stats.eventCount;
    document.getElementById('kfStatDuplicates').textContent = stats.duplicateExtra;
  } catch {
    // 서버가 잠깐 안 뜬 경우다. 다음 주기에 다시 시도한다.
  }

  try {
    const { processes } = await kfRequest('/lab/consumers');
    kfRenderProcesses(processes);
  } catch {
    /* 위와 같음 */
  }

  try {
    const groupId = document.getElementById('kfGroupId').value.trim() || 'order-processor';
    const group = await kfRequest(`/lab/lag?groupId=${encodeURIComponent(groupId)}`);
    kfRenderLag(group);
  } catch {
    // 그룹이 아직 없으면 조회가 실패한다. 컨슈머를 한 번도 안 띄운 상태다.
  }
}

function kfStartPolling() {
  if (kfPollTimer) return;
  kfRefresh();
  kfPollTimer = setInterval(kfRefresh, 2000);
}

function kfStopPolling() {
  if (!kfPollTimer) return;
  clearInterval(kfPollTimer);
  kfPollTimer = null;
}

function initKafkaTab() {
  initKafkaCharts();
  document.getElementById('kfPublishBtn')?.addEventListener('click', kfPublishOrders);
  document.getElementById('kfSpawnBtn')?.addEventListener('click', kfSpawnConsumer);
  document.getElementById('kfStopAllBtn')?.addEventListener('click', kfStopAll);
  document.getElementById('kfResetBtn')?.addEventListener('click', kfResetOffsets);
  document.getElementById('kfClearOrdersBtn')?.addEventListener('click', kfClearOrders);
}
