/* =========================================================
   SCALPAI FRONTEND — WebSocket-driven
   Connects to FastAPI backend via relative URLs (proxied)
   ========================================================= */

const API_BASE = (() => {
  if (typeof window !== 'undefined' && window.API_BASE_URL) return window.API_BASE_URL;
  const proto = window.location.protocol;
  const host = window.location.host;
  return proto + '//' + host;
})();

const WS_BASE = API_BASE.replace(/^http/, 'ws');
const CHANNELS = ['prices', 'book', 'signals', 'trades', 'logs', 'risk'];
const sockets = {};

// ---------- Chart ----------
const CANDLE_WINDOW = 30; // seconds per candle
const priceHistory = []; // array of {t, o, h, l, c}
let currentCandle = null;
let chart = null;

function initChart() {
  const ctx = document.getElementById('chart');
  if (!ctx || chart) return;
  chart = new Chart(ctx, {
    type: 'candlestick',
    data: {
      datasets: [{
        label: 'BTCUSD',
        data: priceHistory,
        color: {
          up: '#00ff88',
          down: '#ff4444',
          unchanged: '#00ff88',
        },
        borderWidth: 2,
        barPercentage: 0.8,
        categoryPercentage: 0.9,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'minute',
            displayFormats: { minute: 'HH:mm' },
            tooltipFormat: 'HH:mm:ss',
          },
          title: {
            display: true,
            text: 'Time',
            color: '#888',
          },
          ticks: { color: '#888', font: { size: 10 } },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
        y: {
          title: {
            display: true,
            text: 'Price',
            color: '#888',
          },
          grid: { color: 'rgba(255,255,255,0.05)' },
          ticks: { color: '#888', font: { size: 10 } },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'nearest',
          intersect: false,
          callbacks: {
            label: function(context) {
              const d = context.raw;
              return [
                `O: ${d.o.toFixed(2)}`,
                `H: ${d.h.toFixed(2)}`,
                `L: ${d.l.toFixed(2)}`,
                `C: ${d.c.toFixed(2)}`,
              ];
            },
          },
        },
      },
    },
  });
}

function updateChart(price, timeStr) {
  const now = new Date();
  const bucket = Math.floor(now.getTime() / (CANDLE_WINDOW * 1000));

  if (!currentCandle || currentCandle.bucket !== bucket) {
    if (currentCandle) {
      priceHistory.push(currentCandle);
      if (priceHistory.length > 60) priceHistory.shift();
    }
    currentCandle = {
      bucket,
      x: now,
      o: price,
      h: price,
      l: price,
      c: price,
    };
  } else {
    currentCandle.h = Math.max(currentCandle.h, price);
    currentCandle.l = Math.min(currentCandle.l, price);
    currentCandle.c = price;
  }

  if (!chart) {
    initChart();
    if (!chart) return;
  }
  chart.update('none');
}

// ---------- WebSocket setup ----------
function connectWS(channel) {
  const ws = new WebSocket(`${WS_BASE}/ws/${channel}`);
  sockets[channel] = ws;

  ws.onopen = () => {
    console.log(`WS connected: ${channel}`);
  };

  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleMessage(channel, data);
  };

  ws.onclose = () => {
    console.log(`WS disconnected: ${channel}`);
    setTimeout(() => connectWS(channel), 2000);
  };

  ws.onerror = (err) => {
    console.error(`WS error ${channel}:`, err);
  };
}

function initWS() {
  CHANNELS.forEach(connectWS);
}

// ---------- Message handlers ----------
function handleMessage(channel, data) {
  switch (channel) {
    case 'prices':
      handlePrices(data);
      break;
    case 'book':
      handleBook(data);
      break;
    case 'signals':
      handleSignals(data);
      break;
    case 'trades':
      handleTrades(data);
      break;
    case 'logs':
      handleLogs(data);
      break;
    case 'risk':
      handleRisk(data);
      break;
  }
}

// ---------- Prices ----------
function handlePrices(data) {
  const el = document.getElementById('topPrice');
  if (el) {
    el.textContent = data.price.toFixed(2);
    el.className = 'price ' + data.change;
  }
  updateChart(data.price, data.time);
}

// ---------- Order Book ----------
function handleBook(data) {
  const el = document.getElementById('orderbook');
  if (!el) return;

  let html = '';
  // Asks (top 5, descending)
  for (let i = 0; i < data.asks.length; i++) {
    const a = data.asks[i];
    html += `<div class="ob-row ask">
      <span>${a.s}</span><span class="p">${a.p}</span>
      <span style="text-align:right">${a.d}</span>
      <div class="depth" style="width:${Math.min(100, a.d * 2)}%"></div>
    </div>`;
  }
  html += `<div class="ob-mid">${data.mid.toFixed(2)}
    <span style="color:var(--muted);font-size:10px">SPREAD ${data.spread}</span></div>`;
  // Bids (top 5, ascending)
  for (let i = 0; i < data.bids.length; i++) {
    const b = data.bids[i];
    html += `<div class="ob-row bid">
      <span>${b.s}</span><span class="p">${b.p}</span>
      <span style="text-align:right">${b.d}</span>
      <div class="depth" style="width:${Math.min(100, b.d * 2)}%"></div>
    </div>`;
  }
  el.innerHTML = html;
}

// ---------- Signals ----------
function handleSignals(data) {
  // Update signal overlay
  const overlay = document.getElementById('signalOverlay');
  if (overlay && data.signal) {
    overlay.querySelector('b').textContent = data.signal;
  }

  // Update individual signal scores
  if (data.rsi !== undefined) {
    document.getElementById('sigRsi').textContent = data.rsi.value;
    document.getElementById('sigRsiNote').textContent = data.rsi.note || '';
  }
  if (data.adx !== undefined) {
    document.getElementById('sigAdx').textContent = data.adx.value;
    document.getElementById('sigAdxNote').textContent = data.adx.note || '';
  }
  if (data.vwap !== undefined) {
    document.getElementById('sigVwap').textContent = data.vwap.value;
    document.getElementById('sigVwapNote').textContent = data.vwap.note || '';
  }
  if (data.ml !== undefined) {
    document.getElementById('sigMl').textContent = data.ml.value;
    document.getElementById('sigMlNote').textContent = data.ml.note || '';
  }
}

// ---------- Trades ----------
function handleTrades(data) {
  const tbody = document.getElementById('tradeLog');
  if (!tbody) return;

  // Remove placeholder row if present
  if (tbody.querySelector('td[colspan]')) {
    tbody.innerHTML = '';
  }

  const row = document.createElement('tr');
  const sideClass = data.side === 'LONG' ? 'buy' : 'sell';
  row.innerHTML = `
    <td>${data.time}</td>
    <td>${data.result || 'OPEN'}</td>
    <td>${data.symbol}</td>
    <td><span class="tag ${sideClass}">${data.side}</span></td>
    <td>${data.price}</td>
    <td>${data.size}</td>
    <td>${data.pnl !== null ? data.pnl.toFixed(2) : '—'}</td>
    <td style="color:var(--muted)">—</td>
    <td>${data.result || '—'}</td>`;
  tbody.insertAdjacentHTML('afterbegin', row.outerHTML);

  while (tbody.children.length > 30) {
    tbody.lastChild.remove();
  }
}

// ---------- Logs ----------
function handleLogs(data) {
  const tbody = document.getElementById('tradeLog');
  if (!tbody) return;

  // Remove placeholder row if present
  if (tbody.querySelector('td[colspan]')) {
    tbody.innerHTML = '';
  }

  const now = new Date().toLocaleTimeString('en-GB');
  const row = document.createElement('tr');
  row.innerHTML = `
    <td>${now}</td>
    <td>${data.msg}</td>
    <td>—</td>
    <td>—</td>
    <td>—</td>
    <td>—</td>
    <td>—</td>
    <td style="color:var(--muted)">System</td>
    <td>${data.type || 'info'}</td>`;
  tbody.insertAdjacentHTML('afterbegin', row.outerHTML);

  while (tbody.children.length > 30) {
    tbody.lastChild.remove();
  }
}

// ---------- Risk ----------
function handleRisk(data) {
  document.getElementById('rBalance').textContent = '$' + data.balance.toFixed(2);
  document.getElementById('rEquity').textContent = '$' + data.equity.toFixed(2);
  document.getElementById('rDayPnl').textContent = (data.day_pnl >= 0 ? '+' : '') + '$' + data.day_pnl.toFixed(2);
  document.getElementById('rDayPnl').className = data.day_pnl >= 0 ? 'pos' : 'neg';
  document.getElementById('rWinRate').textContent = data.win_rate + '%';
  document.getElementById('rMaxDD').textContent = (100 - (data.equity / 5000) * 100).toFixed(2) + '%';
  document.getElementById('openPos').textContent = data.open_positions + ' / 2';

  // Sidebar risk
  document.getElementById('lossUsed').textContent = data.daily_loss_used.toFixed(2) + '% / 2.0%';
  document.getElementById('consecLoss').textContent = data.consecutive_losses + ' / 3';
  const lossBar = document.getElementById('lossBar');
  if (lossBar) {
    lossBar.style.width = Math.min(100, (data.daily_loss_used / 2.0) * 100) + '%';
  }

  // Top bar
  document.getElementById('equity').textContent = '$' + data.equity.toFixed(2);
  document.getElementById('dayPnl').textContent = (data.day_pnl >= 0 ? '+' : '') + '$' + data.day_pnl.toFixed(2);
  document.getElementById('dayPnl').className = data.day_pnl >= 0 ? 'pos' : 'neg';
}

// ---------- Engine buttons (REST API) ----------
const btnEngine = document.getElementById('btnEngine');
const btnKill = document.getElementById('btnKill');
const engineState = document.getElementById('engineState');

async function startEngine() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/engine/start`, {
      method: 'POST',
      headers: { 'X-API-Key': API_KEY },
    });
    if (res.ok) {
      engineState.textContent = 'RUNNING';
      engineState.className = 'pos';
      btnEngine.disabled = true;
      btnKill.disabled = false;
    }
  } catch (e) {
    console.error('Start engine failed:', e);
  }
}

async function killEngine() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/engine/kill`, {
      method: 'POST',
      headers: { 'X-API-Key': API_KEY },
    });
    if (res.ok) {
      engineState.textContent = 'KILLED';
      engineState.className = 'neg';
      btnEngine.disabled = false;
      btnKill.disabled = true;
    }
  } catch (e) {
    console.error('Kill engine failed:', e);
  }
}

btnEngine.addEventListener('click', startEngine);
btnKill.addEventListener('click', killEngine);
btnKill.disabled = true;

// ---------- Confidence slider (local UI only) ----------
const confSlider = document.getElementById('confSlider');
const confLabel = document.getElementById('confLabel');
const confVal = document.getElementById('confVal');
confSlider.addEventListener('input', (e) => {
  confLabel.textContent = e.target.value + '%';
  confVal.textContent = e.target.value + '%';
});

// ---------- Initialize ----------
initChart();
initWS();
