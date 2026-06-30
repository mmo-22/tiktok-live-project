const socket = io();

let currentUsername = null;
let stats = { viewers: 0, likes: 0, diamonds: 0, shares: 0, followers: 0, comments: 0, gifts: 0 };

// ── Connect / Disconnect ───────────────────────────────────────────────────────
function connectTikTok() {
  const u = document.getElementById('usernameInput').value.replace('@', '').trim();
  if (!u) { showToast('أدخل اسم المستخدم أولاً'); return; }
  currentUsername = u;
  // Reset stats for new connection
  stats = { viewers: 0, likes: 0, diamonds: 0, shares: 0, followers: 0, comments: 0, gifts: 0 };
  updateStats();
  // Clear chat
  document.getElementById('chatFeed').innerHTML = '<div class="empty-state">جاري الاتصال...</div>';
  socket.emit('tiktok:connect', { username: u });
  setStatus('connecting', 'جاري الاتصال...');
}

function disconnectTikTok() {
  if (!currentUsername) return;
  socket.emit('tiktok:disconnect', { username: currentUsername });
}

// ── Status ────────────────────────────────────────────────────────────────────
function setStatus(state, label) {
  const badge = document.getElementById('statusBadge');
  badge.className = 'status-badge ' + state;
  badge.textContent = label;
  document.getElementById('connectBtn').style.display    = state === 'connected' ? 'none' : '';
  document.getElementById('disconnectBtn').style.display = state === 'connected' ? '' : 'none';
  document.getElementById('roomInfo').style.display      = state === 'connected' ? 'flex' : 'none';
}

function showLinks(u) {
  const base = location.origin;
  document.getElementById('chatOverlayUrl').textContent  = `${base}/overlay/chat?username=${u}`;
  document.getElementById('wheelOverlayUrl').textContent = `${base}/overlay/wheel?username=${u}`;
  document.getElementById('wheelPageLink').href          = `/wheel?username=${u}`;
  document.getElementById('linksCard').style.display     = 'block';
}

function copyLink(id) {
  const url = document.getElementById(id).textContent;
  navigator.clipboard.writeText(url).then(() => showToast('✅ تم النسخ!'));
}

// ── Socket Events ─────────────────────────────────────────────────────────────
socket.on('tiktok:status', ({ status, username, message }) => {
  if (status === 'connected') {
    setStatus('connected', `✅ متصل بـ @${username}`);
    showToast(`✅ متصل بـ @${username}`);
    showLinks(username);
  }
  else if (status === 'disconnected') { setStatus('disconnected', 'غير متصل'); currentUsername = null; }
  else if (status === 'connecting')   { setStatus('connecting', 'جاري الاتصال...'); }
  else if (status === 'error')        { setStatus('error', `❌ ${message}`); showToast(`❌ ${message}`); currentUsername = null; }
});

socket.on('tiktok:chat', (msg) => {
  stats.comments++;
  updateStats();
  addChatMessage({ nickname: msg.user, profilePicture: msg.avatar, comment: msg.comment });
});

socket.on('tiktok:gift', (gift) => {
  stats.gifts++;
  updateStats();
  addChatMessage({
    nickname: gift.user,
    profilePicture: gift.avatar,
    comment: `🎁 ${gift.giftName} × ${gift.repeatCount}  💎${(gift.diamondCount * gift.repeatCount).toLocaleString()}`,
    isGift: true
  });
});

socket.on('tiktok:follow', ({ user }) => {
  addChatMessage({ nickname: user, comment: '➕ تابع الحساب', isSystem: true });
});

socket.on('tiktok:share', ({ user }) => {
  addChatMessage({ nickname: user, comment: '🔗 شارك البث', isSystem: true });
});

socket.on('tiktok:member', ({ user }) => {
  addChatMessage({ nickname: user, comment: '👋 انضم إلى البث', isSystem: true });
});

socket.on('tiktok:stats', (s) => {
  if (s.viewers   !== undefined) stats.viewers   = s.viewers;
  if (s.likes     !== undefined) stats.likes     = s.likes;
  if (s.diamonds  !== undefined) stats.diamonds  = s.diamonds;
  if (s.shares    !== undefined) stats.shares    = s.shares;
  if (s.followers !== undefined) stats.followers = s.followers;
  updateStats();
});

// ── Stats Display ──────────────────────────────────────────────────────────────
function updateStats() {
  document.getElementById('viewerCount').textContent  = `👁 ${(stats.viewers||0).toLocaleString()}`;
  document.getElementById('likeCount').textContent    = `❤️ ${(stats.likes||0).toLocaleString()}`;
  document.getElementById('commentCount').textContent = `💬 ${(stats.comments||0).toLocaleString()}`;
  document.getElementById('giftCount').textContent    = `🎁 ${(stats.gifts||0).toLocaleString()}`;
  document.getElementById('diamondCount').textContent = `💎 ${(stats.diamonds||0).toLocaleString()}`;
  document.getElementById('shareCount').textContent    = `🔗 ${(stats.shares||0).toLocaleString()}`;
  document.getElementById('followerCount').textContent = `➕ ${(stats.followers||0).toLocaleString()}`;
}

// ── Chat Feed ─────────────────────────────────────────────────────────────────
function addChatMessage(msg) {
  const feed = document.getElementById('chatFeed');
  const empty = feed.querySelector('.empty-state');
  if (empty) empty.remove();
  while (feed.children.length >= 120) feed.removeChild(feed.firstChild);

  const div = document.createElement('div');
  div.className = 'chat-msg' + (msg.isGift ? ' gift-msg' : '') + (msg.isSystem ? ' system-msg' : '');
  const letter = (msg.nickname || '?').charAt(0).toUpperCase();
  const avatar = msg.profilePicture
    ? `<img class="chat-avatar" src="${msg.profilePicture}" alt="" onerror="this.style.display='none'" />`
    : `<div class="chat-avatar-placeholder">${letter}</div>`;

  div.innerHTML = `${avatar}<div class="chat-body"><div class="chat-user">${esc(msg.nickname||'')}</div><div class="chat-text">${esc(msg.comment||'')}</div></div>`;
  feed.appendChild(div);
  feed.scrollTop = feed.scrollHeight;
}

function clearChat() {
  document.getElementById('chatFeed').innerHTML = '<div class="empty-state">تم مسح التعليقات</div>';
  stats.comments = 0; stats.gifts = 0;
  updateStats();
}

// ── Utils ──────────────────────────────────────────────────────────────────────
function esc(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function showToast(m) {
  const t = document.getElementById('toast');
  t.textContent = m; t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}
document.getElementById('usernameInput').addEventListener('keydown', e => { if (e.key === 'Enter') connectTikTok(); });
