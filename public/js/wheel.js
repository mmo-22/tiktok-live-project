// ── Wheel Logic ───────────────────────────────────────────────────────────────
const WHEEL_COLORS = [
  '#0891b2','#0e7490','#06b6d4','#22d3ee',
  '#7c3aed','#6d28d9','#8b5cf6','#a78bfa',
  '#dc2626','#b91c1c','#ef4444','#f87171',
  '#d97706','#b45309','#f59e0b','#fbbf24',
  '#059669','#047857','#10b981','#34d399',
];

let entries = [];
let angle = 0;
let spinning = false;
const canvas = document.getElementById('wheelCanvas');
const ctx = canvas?.getContext('2d');

// ── Draw Wheel ────────────────────────────────────────────────────────────────
function drawWheel(rotationAngle = 0) {
  if (!ctx) return;
  const W = canvas.width, H = canvas.height;
  const cx = W / 2, cy = H / 2, r = Math.min(cx, cy) - 4;

  ctx.clearRect(0, 0, W, H);

  if (entries.length === 0) {
    ctx.fillStyle = '#1e1e2e';
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = '#64748b';
    ctx.font = 'bold 14px Tajawal, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('لا يوجد مشاركين', cx, cy);
    return;
  }

  const slice = (Math.PI * 2) / entries.length;

  entries.forEach((entry, i) => {
    const start = rotationAngle + i * slice;
    const end = start + slice;
    const color = WHEEL_COLORS[i % WHEEL_COLORS.length];

    // Slice
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, start, end);
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.fill();
    ctx.strokeStyle = '#06060b';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Label
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate(start + slice / 2);
    ctx.textAlign = 'right';
    ctx.fillStyle = '#fff';
    ctx.font = `bold ${entries.length > 20 ? 9 : entries.length > 10 ? 11 : 13}px Tajawal, sans-serif`;
    ctx.fillText(entry.nickname.substring(0, 12), r - 10, 4);
    ctx.restore();
  });

  // Center circle
  ctx.beginPath();
  ctx.arc(cx, cy, 18, 0, Math.PI * 2);
  ctx.fillStyle = '#06060b';
  ctx.fill();
  ctx.strokeStyle = '#22d3ee';
  ctx.lineWidth = 2;
  ctx.stroke();
}

// ── Spin Animation ────────────────────────────────────────────────────────────
function animateSpin(targetAngle, duration, winnerIndex) {
  const start = performance.now();
  const startAngle = angle;

  function step(now) {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    // Ease out cubic
    const ease = 1 - Math.pow(1 - progress, 3);
    angle = startAngle + (targetAngle - startAngle) * ease;
    drawWheel(angle);

    if (progress < 1) {
      requestAnimationFrame(step);
    } else {
      angle = targetAngle;
      drawWheel(angle);
      spinning = false;
    }
  }
  requestAnimationFrame(step);
}

// ── Socket Events ─────────────────────────────────────────────────────────────
// (socket is defined in admin.js which loads first, but wheel.js loads first in HTML)
// We defer socket usage to after DOM is ready
window.addEventListener('DOMContentLoaded', () => {
  // socket is available globally from admin.js via the socket.io script
  // We hook into the global socket set by admin.js after it loads
  // Use a small delay to ensure admin.js socket is ready
  setTimeout(hookWheelSocket, 100);
});

function hookWheelSocket() {
  if (typeof socket === 'undefined') { setTimeout(hookWheelSocket, 100); return; }

  socket.on('wheel:entries', (e) => {
    entries = e;
    document.getElementById('entriesCount').textContent = `${entries.length} مشارك`;
    drawWheel(angle);
    renderEntriesList();
  });

  socket.on('wheel:spinning', ({ winner }) => {
    spinning = true;
    document.getElementById('spinBtn').disabled = true;

    if (entries.length === 0) return;
    const winnerIndex = entries.findIndex(e => e.uniqueId === winner.uniqueId);
    const slice = (Math.PI * 2) / entries.length;

    // Target: pointer (top = -π/2) points to winner slice center
    const winnerMidAngle = winnerIndex * slice + slice / 2;
    const extraSpins = Math.PI * 2 * (5 + Math.random() * 3); // 5-8 full spins
    const targetAngle = angle + extraSpins + (Math.PI * 1.5 - winnerMidAngle - (angle % (Math.PI * 2)));

    animateSpin(targetAngle, 5000, winnerIndex);
  });

  socket.on('wheel:result', ({ winner }) => {
    document.getElementById('spinBtn').disabled = false;

    const wEl = document.getElementById('wheelWinner');
    wEl.style.display = 'flex';
    document.getElementById('winnerName').textContent = winner.nickname;
    if (winner.profilePicture) {
      document.getElementById('winnerPic').src = winner.profilePicture;
    }
    showToast(`🏆 الفائز: ${winner.nickname}`);
  });

  socket.on('wheel:reset', () => {
    document.getElementById('wheelWinner').style.display = 'none';
    document.getElementById('spinBtn').disabled = false;
    angle = 0;
    drawWheel(angle);
  });

  // Initial draw
  drawWheel();
}

// ── Controls ──────────────────────────────────────────────────────────────────
function spinWheel() {
  if (spinning) return;
  socket.emit('wheel:spin');
}

function resetWheel() {
  if (confirm('هل تريد إعادة تعيين العجلة وحذف جميع المشاركين؟')) {
    socket.emit('wheel:reset');
  }
}

// ── Entries List ──────────────────────────────────────────────────────────────
function renderEntriesList() {
  const list = document.getElementById('entriesList');
  if (!list) return;
  list.innerHTML = '';
  entries.forEach(e => {
    const div = document.createElement('div');
    div.className = 'entry-item';
    div.innerHTML = `
      ${e.profilePicture ? `<img src="${e.profilePicture}" alt="" onerror="this.style.display='none'"/>` : ''}
      <span class="entry-name">${escapeHtml(e.nickname)}</span>
      <button class="entry-remove" onclick="removeEntry('${e.uniqueId}')" title="حذف">✕</button>
    `;
    list.appendChild(div);
  });
}

function removeEntry(uniqueId) {
  socket.emit('wheel:remove', { uniqueId });
}
