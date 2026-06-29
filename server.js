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

// ── State ──────────────────────────────────────────────────────────────────────
const connections = new Map(); // username -> { ws, stats, retryTimer }
// Wheel state per username
const wheelState = new Map(); // username -> { keyword, entries, winner }
function getWheel(u) {
  if (!wheelState.has(u)) wheelState.set(u, { keyword: 'اشتراك', entries: [], winner: null });
  return wheelState.get(u);
}

function roomOf(u) { return `room:${u}`; }

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
    // Wheel registration is handled client-side (wheel page checks keyword + registration state)
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
    stats.likes = data.totalLikeCount || stats.likes + (data.likeCount || 1);
    emitStats(username, socket);
  }

  else if (ev === 'roomUser' || ev === 'roomUserSeq' || ev === 'viewerCount' || ev === 'viewer' || ev === 'liveInfo') {
    const vc = data.viewerCount || data.totalViewers || data.viewer_count || data.count || null;
    if (vc !== null) stats.viewers = vc;
    emitStats(username, socket);
  }

  else if (ev === 'follow') {
    const user = u(data);
    stats.followers++;
    const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
    socket.emit('tiktok:follow', payload);
    io.to(roomOf(username)).emit('follow', payload);
    emitStats(username, socket);
  }

  else if (ev === 'share') {
    const user = u(data);
    stats.shares++;
    const payload = { user: user.nickname || user.uniqueId, uniqueId: user.uniqueId };
    socket.emit('tiktok:share', payload);
    io.to(roomOf(username)).emit('share', payload);
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

  // Log unknown events (must be LAST in the chain)
  else if (!['websocket_upgrade','connected','disconnected','pong','streamEnd','roomInfo','emote','envelope','subscribe','linkMicBattle','linkMicArmies','giftDynamicRestriction','shareRevenueNotice','linkMicLayout','fanTicket','giftPanelUpdate','linkMicMethod','controlMessage','msgDetect','toast','liveIntro','perception','systemMessage','linkMicPermission'].includes(ev)) {
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

    // Disconnect existing
    if (connections.has(u)) {
      try { connections.get(u).ws?.close(); } catch (_) {}
      connections.delete(u);
    }

    socket.emit('tiktok:status', { status: 'connecting', username: u });

    const stats = { viewers: 0, likes: 0, diamonds: 0, shares: 0, followers: 0 };
    const conn = { ws: null, stats };
    connections.set(u, conn);

    async function doConnect() {
      const ws = await connectTikTools(u,
        (msg) => parseTikToolsEvent(msg, u, socket),
        (status, message) => {
          socket.emit('tiktok:status', { status, username: u, message });
          io.emit('tiktok:status', { status, username: u, message });
          if (status === 'connected') io.to(roomOf(u)).emit('overlay:status', { connected: true });
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

  // Wheel — per-username state
  socket.on('wheel:join', ({ username }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    if (u) socket.join(`wheel:${u}`);
  });

  socket.on('wheel:getState', ({ username }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    const w = getWheel(u);
    socket.emit('wheel:state', { keyword: w.keyword, entries: w.entries, winner: w.winner });
  });

  socket.on('wheel:setKeyword', ({ username, keyword }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    getWheel(u).keyword = keyword;
  });

  socket.on('wheel:updateEntries', ({ username, entries }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    getWheel(u).entries = entries || [];
    io.to(`wheel:${u}`).emit('wheel:updateEntries', { username: u, entries: getWheel(u).entries });
  });

  socket.on('wheel:removeEntry', ({ username, uniqueId }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    const w = getWheel(u);
    w.entries = w.entries.filter(e => e.uniqueId !== uniqueId);
    io.to(`wheel:${u}`).emit('wheel:updateEntries', { username: u, entries: w.entries });
  });

  socket.on('wheel:spin', ({ username, winner, entries }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    const w = getWheel(u);
    if (entries) w.entries = entries;
    w.winner = winner;
    io.to(`wheel:${u}`).emit('wheel:spin', { username: u, winner, entries: w.entries });
  });

  socket.on('wheel:result', ({ username, winner }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    getWheel(u).winner = winner;
    io.to(`wheel:${u}`).emit('wheel:result', { username: u, winner });
  });

  socket.on('wheel:reset', ({ username }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    getWheel(u).winner = null;
    io.to(`wheel:${u}`).emit('wheel:reset', { username: u });
  });

  socket.on('wheel:clear', ({ username }) => {
    const u = (username||'').replace('@','').trim().toLowerCase();
    const w = getWheel(u);
    w.entries = []; w.winner = null;
    io.to(`wheel:${u}`).emit('wheel:updateEntries', { username: u, entries: [] });
    io.to(`wheel:${u}`).emit('wheel:clear', { username: u });
  });
});

server.listen(PORT, () => console.log(`✅ Server → http://localhost:${PORT}`));
