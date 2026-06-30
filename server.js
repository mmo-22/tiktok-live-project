require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const PORT = process.env.PORT || 3000;

app.use(express.json());

// API: get active connected usernames
app.get('/api/active', (_, res) => {
  res.json({ usernames: [...connections.keys()] });
});
// No cache for HTML files
app.use((req, res, next) => {
  if (req.path.endsWith('.html') || req.path === '/' || req.path === '/obs' || req.path === '/wheel' || req.path.startsWith('/overlay/')) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
  }
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

// ── Routes ─────────────────────────────────────────────────────────────────────
app.get('/',              (_, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));
app.get('/obs',           (_, res) => res.sendFile(path.join(__dirname, 'public', 'obs-generator.html')));
app.get('/overlay/chat',  (_, res) => res.sendFile(path.join(__dirname, 'public', 'overlays', 'chat.html')));
app.get('/overlay/wheel', (_, res) => res.sendFile(path.join(__dirname, 'public', 'overlays', 'wheel.html')));
app.get('/wheel',          (_, res) => res.sendFile(path.join(__dirname, 'public', 'wheel.html')));

// ── API: Save tik.tools key at runtime ────────────────────────────────────────
app.post('/api/set-key', (req, res) => {
  const { key } = req.body;
  if (!key || typeof key !== 'string' || key.trim().length < 10) {
    return res.json({ ok: false, error: 'مفتاح غير صالح' });
  }
  process.env.TIKTOOL_API_KEY = key.trim();
  res.json({ ok: true });
});

// ── Wheel REST API ─────────────────────────────────────────────────────────────
app.get('/api/wheel/:username', (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const w = getWheel(key);
  res.json({ keyword: w.keyword, accepting: w.accepting, entries: Array.from(w.entries.values()), count: w.entries.size, winner: w.winner });
});

app.post('/api/wheel/config', (req, res) => {
  const { username, keyword } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getWheel(key).keyword = keyword || 'اشتراك';
  res.json({ ok: true, keyword: getWheel(key).keyword });
});

app.post('/api/wheel/open', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getWheel(key).accepting = true;
  io.to(roomOf(key)).emit('wheel:status', { accepting: true, keyword: getWheel(key).keyword });
  res.json({ ok: true });
});

app.post('/api/wheel/close', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getWheel(key).accepting = false;
  io.to(roomOf(key)).emit('wheel:status', { accepting: false });
  res.json({ ok: true });
});

app.post('/api/wheel/spin', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const w = getWheel(key);
  const entries = Array.from(w.entries.values());
  if (entries.length < 2) return res.json({ ok: false, message: 'يحتاج مشتركين أكثر' });
  w.accepting = false;
  const winner = entries[Math.floor(Math.random() * entries.length)];
  w.winner = winner;
  io.to(roomOf(key)).emit('wheel:spin', { winner, entries });
  io.to(roomOf(key)).emit('wheel:status', { accepting: false });
  res.json({ ok: true, winner });
});

app.post('/api/wheel/clear', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const w = getWheel(key);
  w.entries.clear(); w.winner = null;
  io.to(roomOf(key)).emit('wheel:update', { entries: [], count: 0 });
  res.json({ ok: true });
});

app.post('/api/wheel/remove', (req, res) => {
  const { username, uniqueId } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const w = getWheel(key);
  w.entries.delete(uniqueId);
  io.to(roomOf(key)).emit('wheel:update', { entries: Array.from(w.entries.values()), count: w.entries.size });
  res.json({ ok: true });
});

// ── State ──────────────────────────────────────────────────────────────────────
const connections = new Map(); // username -> { ws, stats, retryTimer }
// Wheel state per username (Map-based entries like private project)
const wheelState = new Map();
function getWheel(u) {
  if (!wheelState.has(u)) wheelState.set(u, { keyword: 'اشتراك', entries: new Map(), accepting: false, winner: null });
  return wheelState.get(u);
}

function roomOf(u) { return `room:${u}`; }

// ── Fetch current room stats via REST ──────────────────────────────────────────
async function fetchRoomStats(username, apiKey) {
  try {
    const res = await fetch(`https://api.tik.tools/user/${encodeURIComponent(username)}/live?apiKey=${apiKey}`);
    const data = await res.json();
    if (data?.data) {
      const room = data.data;
      return {
        viewers: room.viewerCount || room.viewer_count || room.liveRoomStats?.userCount || 0,
        likes:   room.likeCount  || room.like_count  || room.liveRoomStats?.likeCount || 0,
        shares:  room.shareCount || room.share_count  || 0,
      };
    }
  } catch (e) {
    console.log('[tik.tools] REST stats fetch error:', e.message);
  }
  return null;
}

// ── tik.tools WebSocket Connection ────────────────────────────────────────────
async function connectTikTools(username, onEvent, onStatus, onReconnect) {
  const apiKey = process.env.TIKTOOL_API_KEY;
  if (!apiKey) { onStatus('error', 'TIKTOOL_API_KEY غير موجود — أضفه من صفحة الإعدادات'); return null; }

  // Step 1: Get JWT token
  let wsUrl;
  try {
    const res = await fetch(`https://api.tik.tools/authentication/jwt?apiKey=${apiKey}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ allowed_creators: [username], expire_after: 7200, max_websockets: 1 }),
    });
    const data = await res.json();
    if (data?.data?.token) {
      wsUrl = `wss://api.tik.tools?uniqueId=${username}&jwtKey=${data.data.token}`;
      console.log(`[tik.tools] JWT obtained @${username}`);
    } else {
      console.log(`[tik.tools] JWT failed, fallback @${username}:`, JSON.stringify(data));
      wsUrl = `wss://api.tik.tools?uniqueId=${username}&apiKey=${apiKey}`;
    }
  } catch (e) {
    console.log(`[tik.tools] JWT error, fallback @${username}:`, e.message);
    wsUrl = `wss://api.tik.tools?uniqueId=${username}&apiKey=${apiKey}`;
  }

  const ws = new WebSocket(wsUrl);

  ws.on('open', () => {
    console.log(`[tik.tools] Connected @${username}`);
    onStatus('connected');
  });

  ws.on('message', (raw) => {
    const str = raw.toString();
    // Ping/pong — must handle before JSON parse
    if (str.includes('"ping"')) {
      ws.send(JSON.stringify({ event: 'pong' }));
      return;
    }
    try {
      const msg = JSON.parse(str);
      onEvent(msg);
    } catch (_) {}
  });

  ws.on('close', (code) => {
    console.log(`[tik.tools] Disconnected @${username} code=${code}`);
    // Auto-reconnect on non-fatal codes (1005, 1006)
    if ([1005, 1006].includes(code)) {
      console.log(`[tik.tools] Auto-reconnecting @${username} in 5s...`);
      onStatus('connecting', 'إعادة الاتصال...');
      setTimeout(() => {
        onReconnect && onReconnect();
      }, 5000);
    } else {
      onStatus('disconnected', `code ${code}`);
    }
  });

  ws.on('error', (err) => {
    console.error(`[tik.tools] Error @${username}:`, err.message);
    onStatus('error', err.message);
  });

  return ws;
}

// ── Parse tik.tools events ────────────────────────────────────────────────────
function parseTikToolsEvent(msg, username, socket) {
  const conn = connections.get(username);
  if (!conn) return;
  const { stats } = conn;
  const ev = msg.event;
  const data = msg.data || {};

  // Helper — user info nested under data.user in tik.tools
  const u = (d) => d.user || d;

  if (ev === 'chat') {
    const user = u(data);
    const payload = {
      user: user.nickname || user.uniqueId,
      avatar: user.profilePicture?.url?.[0] || user.profilePictureUrl,
      comment: data.comment,
      isModerator: user.isModerator || false,
      uniqueId: user.uniqueId,
    };
    socket.emit('tiktok:chat', payload);
    io.to(roomOf(username)).emit('chat', payload);

    // Wheel: server-side registration (if accepting + keyword match)
    const wh = getWheel(username);
    if (wh.accepting && wh.keyword && (data.comment || '').includes(wh.keyword)) {
      const uid = user.uniqueId;
      if (!wh.entries.has(uid)) {
        wh.entries.set(uid, { uniqueId: uid, nickname: payload.user, profilePicture: payload.avatar });
        io.to(roomOf(username)).emit('wheel:update', { entries: Array.from(wh.entries.values()), count: wh.entries.size });
      }
    }
  }

  else if (ev === 'gift') {
    if (data.giftType === 1 && !data.repeatEnd) return;
    const user = u(data);
    const diamonds = (data.diamondCount || 0) * (data.repeatCount || 1);
    stats.diamonds += diamonds;
    const payload = {
      user: user.nickname || user.uniqueId,
      avatar: user.profilePicture?.url?.[0] || user.profilePictureUrl,
      giftName: data.giftName,
      giftImageUrl: data.giftPictureUrl,
      repeatCount: data.repeatCount || 1,
      diamondCount: data.diamondCount || 0,
      uniqueId: user.uniqueId,
    };
    socket.emit('tiktok:gift', payload);
    io.to(roomOf(username)).emit('gift', payload);
    emitStats(username, socket);
  }

  else if (ev === 'like') {
    // Use totalLikeCount (absolute) if available, otherwise increment
    if (data.totalLikeCount) stats.likes = data.totalLikeCount;
    else stats.likes += (data.likeCount || 1);
    emitStats(username, socket);
  }

  else if (ev === 'roomUser' || ev === 'roomUserSeq' || ev === 'viewerCount' || ev === 'viewer' || ev === 'liveInfo') {
    const vc = data.viewerCount || data.totalViewers || data.viewer_count || data.count || null;
    if (vc !== null) stats.viewers = vc;
    emitStats(username, socket);
  }

  else if (ev === 'roomInfo') {
    // Initial room data — extract likes, viewers, etc.
    if (data.likeCount) stats.likes = data.likeCount;
    if (data.viewerCount) stats.viewers = data.viewerCount;
    if (data.totalViewers) stats.viewers = data.totalViewers;
    if (data.shareCount) stats.shares = data.shareCount;
    if (data.followCount) stats.followers = data.followCount;
    emitStats(username, socket);
  }

  else if (ev === 'follow' || data.type === 'follow') {
    const user = u(data);
    stats.followers++;
    const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
    socket.emit('tiktok:follow', payload);
    io.to(roomOf(username)).emit('follow', payload);
    emitStats(username, socket);
  }

  else if (ev === 'share' || data.type === 'share') {
    const user = u(data);
    stats.shares++;
    const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
    socket.emit('tiktok:share', payload);
    io.to(roomOf(username)).emit('share', payload);
    emitStats(username, socket);
  }

  else if (ev === 'social' || data.type === 'social') {
    const user = u(data);
    const displayType = data.displayType || data.display_type || '';
    const action = data.action || data.event_sub_type || 0;
    const label = data.label || '';

    // Log full social event for debugging (remove later)
    console.log('[tik.tools] SOCIAL:', JSON.stringify({ displayType, action, label, nickname: user.nickname, uniqueId: user.uniqueId }));

    // Differentiate: share vs follow
    const isShare = displayType.toString().includes('share')
      || label.toLowerCase().includes('share')
      || action === 3 || action === '3'
      || displayType === 'pm_mt_msg_viewer_share'
      || displayType === 'pm_mt_guidance_share_2';

    if (isShare) {
      stats.shares++;
      const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
      socket.emit('tiktok:share', payload);
      io.to(roomOf(username)).emit('share', payload);
    } else {
      stats.followers++;
      const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
      socket.emit('tiktok:follow', payload);
      io.to(roomOf(username)).emit('follow', payload);
    }
    emitStats(username, socket);
  }

  else if (ev === 'member' || data.type === 'member') {
    const user = u(data);
    const payload = {
      user: user.nickname || user.uniqueId,
      avatar: user.profilePicture?.url?.[0] || user.profilePictureUrl,
      actionId: 1,
      uniqueId: user.uniqueId,
    };
    socket.emit('tiktok:member', payload);
    io.to(roomOf(username)).emit('member', payload);
  }

  // Debug: log social/follow/share for troubleshooting
  // (remove these lines once confirmed working)

  // Log unknown events (must be LAST in the chain)
  else if (!['websocket_upgrade','connected','disconnected','pong','streamEnd','emote','envelope','subscribe','linkMicBattle','linkMicArmies','giftDynamicRestriction','shareRevenueNotice','linkMicLayout','linkMicMethod','fanTicket','giftPanelUpdate','controlMessage','msgDetect','toast','liveIntro','perception','systemMessage','linkMicPermission','linkMic','linkLayer','link','commentTray','barrage','aiSummary','room','streamStatus','questionNew','imDelete','oecLive','rankUpdate','rankText','hourlyRank','topFans','caption','subNotify','pollMessage','goalkeeperUpdate','unauthorized'].includes(ev)) {
    console.log('[tik.tools] Unknown event:', ev, JSON.stringify(data).substring(0, 200));
  }
}

function emitStats(username, socket) {
  const s = connections.get(username)?.stats;
  if (!s) return;
  socket.emit('tiktok:stats', s);
  io.to(roomOf(username)).emit('stats', s);
}

// ── Socket.IO ──────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {

  socket.on('tiktok:connect', async ({ username }) => {
    if (!username) return;
    const u = username.replace('@', '').trim().toLowerCase();

    // Disconnect ALL existing connections (not just same username)
    for (const [key, conn] of connections) {
      try { conn.ws?.close(); } catch (_) {}
      connections.delete(key);
      io.emit('tiktok:status', { status: 'disconnected', username: key });
    }

    socket.emit('tiktok:status', { status: 'connecting', username: u });

    const stats = { viewers: 0, likes: 0, diamonds: 0, shares: 0, followers: 0 };
    const conn = { ws: null, stats };
    connections.set(u, conn);

    // Fetch current stats via REST API first
    const apiKey = process.env.TIKTOOL_API_KEY;
    if (apiKey) {
      const roomStats = await fetchRoomStats(u, apiKey);
      if (roomStats) {
        if (roomStats.viewers) stats.viewers = roomStats.viewers;
        if (roomStats.likes) stats.likes = roomStats.likes;
        if (roomStats.shares) stats.shares = roomStats.shares;
        console.log(`[tik.tools] Initial stats @${u}: ${stats.viewers} viewers, ${stats.likes} likes`);
      }
    }

    async function doConnect() {
      const ws = await connectTikTools(u,
        (msg) => parseTikToolsEvent(msg, u, socket),
        (status, message) => {
          socket.emit('tiktok:status', { status, username: u, message });
          io.emit('tiktok:status', { status, username: u, message });
          if (status === 'connected') {
          io.to(roomOf(u)).emit('overlay:status', { connected: true });
          // Send initial stats immediately
          emitStats(u, socket);
        }
          if (status === 'disconnected' || status === 'error') {
            io.to(roomOf(u)).emit('overlay:status', { connected: false });
            connections.delete(u);
          }
        },
        () => doConnect() // reconnect callback
      );
      if (ws) {
        conn.ws = ws;
        connections.set(u, conn);
      }
    }
    await doConnect();

  });

  socket.on('tiktok:disconnect', ({ username }) => {
    const u = username?.replace('@', '').trim().toLowerCase();
    if (connections.has(u)) {
      try { connections.get(u).ws?.close(); } catch (_) {}
      connections.delete(u);
    }
    socket.emit('tiktok:status', { status: 'disconnected', username: u });
  });

  // Overlay joins room
  socket.on('join',         ({ username }) => { const u = (username||'').replace('@','').trim().toLowerCase(); if (u) socket.join(roomOf(u)); });
  socket.on('overlay:join', ({ username }) => { const u = (username||'').replace('@','').trim().toLowerCase(); if (u) socket.join(roomOf(u)); });

  // Wheel is now handled via REST API (see /api/wheel/*)
});

server.listen(PORT, () => console.log(`✅ Server → http://localhost:${PORT}`));
