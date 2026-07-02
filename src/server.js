const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const { TikTokLive } = require('tiktok-live-api');

// ── 🔑 مزود التوقيع: tik.tools ──
// tik.tools يقدم signing على Free tier (Sandbox: 15 WS, 2,500 req/يوم)
// احصل على مفتاح مجاني من: https://tik.tools/login
// ضع في Railway env vars: TIKTOOL_API_KEY=<مفتاحك>
const TIKTOK_API_KEY = process.env.TIKTOOL_API_KEY
  || process.env.SIGN_API_KEY
  || process.env.TIKTOOLS_API_KEY
  || '';

if (!TIKTOK_API_KEY) {
  console.error('⚠️  [tik.tools] مفتاح API مفقود — احصل على واحد مجاني من https://tik.tools/login');
  console.error('   ثم أضفه في Railway env vars بالاسم: TIKTOOL_API_KEY');
} else {
  console.log('[tik.tools] Provider: tik.tools | Key:', TIKTOK_API_KEY.slice(0, 12) + '...');
}
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*', credentials: false },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000,
  // تحسينات لـ TikTok LIVE Studio و OBS Browser Source (WebView محدودة)
  allowEIO3: true,           // توافق مع عملاء قدماء
  maxHttpBufferSize: 1e6,    // 1MB يكفي للأحداث العادية
  perMessageDeflate: false,  // تعطيل الضغط — أسرع للأحداث الصغيرة المتكررة
  httpCompression: false,    // نفس السبب
});

// ── Body parsing مع التقاط raw كاحتياطي ──
app.use(express.json({
  limit: '2mb',
  verify: (req, _res, buf) => { req.rawBody = buf.toString('utf8'); },
}));
// fallback إضافي: لو الـ Content-Type غلط (مثلاً text/plain من Cloudflare)
app.use(express.text({ type: '*/*', limit: '2mb' }));
// post-process: لو req.body من text middleware string، حاول JSON parse
app.use((req, _res, next) => {
  if (typeof req.body === 'string' && req.body.trim().startsWith('{')) {
    try { req.body = JSON.parse(req.body); } catch(_) {}
  }
  next();
});

// ── Simple cookie parser (no external deps) ──────────────
app.use((req, res, next) => {
  req.cookies = {};
  const h = req.headers.cookie;
  if (h) h.split(';').forEach(c => {
    const i = c.indexOf('=');
    if (i > 0) req.cookies[c.slice(0, i).trim()] = decodeURIComponent(c.slice(i + 1).trim());
  });
  next();
});

// ══════════════════════════════════════════════════════════
// ── Version ───────────────────────────────────────────────
// ══════════════════════════════════════════════════════════
const VERSION = '2.9.3';
app.get('/api/version', (req, res) => res.json({ version: VERSION }));

// ══════════════════════════════════════════════════════════
// ── Email Setup (Resend) ─────────────────────────────────
// ══════════════════════════════════════════════════════════
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const SUPPORT_EMAIL = process.env.SUPPORT_EMAIL || 'Mxo2009@gmail.com';
const FROM_EMAIL = process.env.FROM_EMAIL || 'BthLab <noreply@bthlab.live>';

async function sendEmail(to, subject, html) {
  if (!RESEND_API_KEY) { console.log('[Email] No API key — skipping'); return; }
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: FROM_EMAIL, to, subject, html }),
    });
    const data = await res.json();
    if (data.id) console.log(`[Email] Sent to ${to}: ${subject}`);
    else console.error('[Email] Error:', JSON.stringify(data));
  } catch(e) { console.error('[Email] Failed:', e.message); }
}

// ══════════════════════════════════════════════════════════
// ── Config ───────────────────────────────────────────────
// ══════════════════════════════════════════════════════════
const OWNER_PASSWORD = process.env.OWNER_PASSWORD || 'admin2024';
const STREAM_API_KEY = process.env.STREAM_API_KEY || 'NjRmNDYzNTgtMjhjYi00MmNmLTlkNWUtN2FmZjQxMDhlM2QzOjliYTE1ZmJmLWI1ZGEtNGYxZS04MmYxLWY0ZmMxZGQ1ZmQ1NA==';
const STREAM_PRODUCT_ID = process.env.STREAM_PRODUCT_ID || '228d203d-e530-40c4-8665-5724c7174d4e';
const STREAM_API_BASE = 'https://stream-app-service.streampay.sa/api/v2';

// Use RAILWAY_VOLUME_MOUNT_PATH if available (persistent), otherwise fallback to local
const PERSIST_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, '../data');
const DATA_FILE = path.join(PERSIST_DIR, 'subscribers.json');

if (!fs.existsSync(PERSIST_DIR)) fs.mkdirSync(PERSIST_DIR, { recursive: true });

function loadSubscribers() {
  try { if (fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch(e) {}
  return {};
}
function saveSubscribers(data) { fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), 'utf8'); }

let subscribers = loadSubscribers();

function generateKey() { return 'TLR-' + crypto.randomBytes(4).toString('hex').toUpperCase(); }

// StreamPay API helper
async function streamAPI(method, endpoint, body) {
  const res = await fetch(`${STREAM_API_BASE}${endpoint}`, {
    method,
    headers: { 'x-api-key': STREAM_API_KEY, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

// ── Subscription API (StreamPay) ─────────────────────────

// Create payment link — user clicks "subscribe" → gets redirect URL
app.post('/api/create-payment', async (req, res) => {
  const { name, email, tiktokUsername } = req.body;
  if (!name || !name.trim()) return res.json({ ok: false, error: 'الاسم مطلوب' });
  if (!email || !email.trim()) return res.json({ ok: false, error: 'الإيميل مطلوب' });
  if (!tiktokUsername || !tiktokUsername.trim()) return res.json({ ok: false, error: 'يوزرنيم التيك توك مطلوب' });
  try {
    // Create consumer in StreamPay
    const consumer = await streamAPI('POST', '/consumers', {
      name: name.trim(),
      email: email.trim(),
      communication_methods: ['EMAIL'],
    });
    const consumerId = consumer.id;
    if (!consumerId) {
      // Consumer might already exist — try without creating
      console.log('[StreamPay] Consumer creation response:', JSON.stringify(consumer));
    }
    // Create payment link
    const origin = req.headers.origin || req.headers.referer?.replace(/\/[^\/]*$/, '') || 'https://bthlab.live';
    const paymentLink = await streamAPI('POST', '/payment_links', {
      name: 'اشتراك BthLab الشهري',
      description: 'اشتراك شهري في مختبر البث — 80 ريال',
      items: [{ product_id: STREAM_PRODUCT_ID, quantity: 1 }],
      contact_information_type: 'EMAIL',
      currency: 'SAR',
      max_number_of_payments: 1,
      organization_consumer_id: consumerId || undefined,
      success_redirect_url: `${origin}/payment-callback.html?tiktokUsername=${encodeURIComponent(tiktokUsername)}&email=${encodeURIComponent(email)}&name=${encodeURIComponent(name)}`,
      failure_redirect_url: `${origin}/payment-callback.html?status=failed`,
      custom_metadata: { tiktokUsername: tiktokUsername.trim(), email: email.trim(), name: name.trim() },
    });
    if (paymentLink.url) {
      console.log(`[StreamPay] Payment link created for ${email}: ${paymentLink.url}`);
      res.json({ ok: true, url: paymentLink.url });
    } else {
      console.error('[StreamPay] Error:', JSON.stringify(paymentLink));
      res.json({ ok: false, error: 'خطأ في إنشاء رابط الدفع' });
    }
  } catch(e) { console.error('[StreamPay] Error:', e); res.json({ ok: false, error: 'خطأ في الاتصال بنظام الدفع' }); }
});

// Verify payment after redirect — called by payment-callback.html
app.post('/api/subscribe', async (req, res) => {
  const { invoiceId, paymentId, email, name, tiktokUsername } = req.body;
  if (!invoiceId && !paymentId) return res.json({ ok: false, error: 'معرف الدفع مطلوب' });
  if (!tiktokUsername || !tiktokUsername.trim()) return res.json({ ok: false, error: 'يوزرنيم التيك توك مطلوب' });
  try {
    // Check if already subscribed with this payment
    const existing = Object.entries(subscribers).find(([k, v]) => v.paymentId === (invoiceId || paymentId));
    if (existing) return res.json({ ok: true, key: existing[0] });
    // Verify payment with StreamPay
    let verified = false;
    if (invoiceId) {
      const invoice = await streamAPI('GET', `/invoices/${invoiceId}`);
      verified = invoice.status === 'paid' || invoice.status === 'PAID';
      if (!verified) console.log('[StreamPay] Invoice status:', invoice.status);
    }
    if (!verified && paymentId) {
      const payment = await streamAPI('GET', `/payments/${paymentId}`);
      verified = payment.status === 'paid' || payment.status === 'PAID' || payment.status === 'succeeded';
      if (!verified) console.log('[StreamPay] Payment status:', payment.status);
    }
    if (!verified) return res.json({ ok: false, error: 'الدفع لم يكتمل' });
    const key = generateKey();
    const now = new Date();
    const expires = new Date(now); expires.setDate(expires.getDate() + 30);
    const cleanUsername = tiktokUsername.toLowerCase().replace('@', '').trim();
    subscribers[key] = { email: email||'', name: name||'', tiktokUsername: cleanUsername, paymentId: invoiceId || paymentId, createdAt: now.toISOString(), expiresAt: expires.toISOString(), active: true };
    saveSubscribers(subscribers);
    console.log(`[Subscribe] New: ${key} (${email}) @${cleanUsername} expires ${expires.toISOString()}`);
    // Send welcome email
    sendEmail(email, '[BthLab] مرحباً بك في مختبر البث!',
      `<div dir="rtl" style="font-family:Tahoma,sans-serif;max-width:500px;margin:0 auto;padding:20px;text-align:center">
        <h2 style="color:#0891b2">🎉 مرحباً بك في BthLab!</h2>
        <p style="font-size:14px">مرحباً ${name || ''},</p>
        <p style="font-size:14px">تم تفعيل اشتراكك بنجاح</p>
        <div style="margin:20px 0;padding:16px;background:#f5f5f5;border-radius:12px;font-family:monospace;font-size:20px;font-weight:bold;letter-spacing:2px">${key}</div>
        <p style="font-size:12px;color:#888">ادخل هذا المفتاح في صفحة تسجيل الدخول</p>
        <p style="font-size:12px;color:#888">ينتهي اشتراكك: ${expires.toLocaleDateString('ar-SA')}</p>
        <p style="font-size:10px;color:#aaa;margin-top:20px">⚠️ لا تشارك هذا المفتاح مع أحد</p>
      </div>`
    );
    res.json({ ok: true, key, expiresAt: expires.toISOString() });
  } catch(e) { console.error('[Subscribe] Error:', e); res.json({ ok: false, error: 'خطأ في التحقق' }); }
});

app.get('/api/validate-key', (req, res) => {
  const ip = req.ip || req.connection.remoteAddress;
  if (!checkRateLimit(ip)) return res.status(429).json({ valid: false, error: 'محاولات كثيرة. حاول بعد 15 دقيقة' });
  const key = req.query.key;
  const deviceId = req.query.deviceId || '';
  if (!key) return res.json({ valid: false });
  const sub = subscribers[key];
  if (!sub || !sub.active) return res.json({ valid: false });
  if (new Date(sub.expiresAt) < new Date()) { sub.active = false; saveSubscribers(subscribers); return res.json({ valid: false, expired: true }); }

  // Device binding
  if (!deviceId) return res.json({ valid: false, error: 'معرّف الجهاز مطلوب' });
  if (!sub.deviceId) {
    // First login — bind device
    sub.deviceId = deviceId;
    saveSubscribers(subscribers);
    console.log(`[Auth] Device bound for ${key}: ${deviceId.substring(0, 8)}...`);
  } else if (sub.deviceId !== deviceId) {
    // Different device — reject
    console.log(`[Auth] Device mismatch for ${key}: expected ${sub.deviceId.substring(0, 8)} got ${deviceId.substring(0, 8)}`);
    return res.json({ valid: false, error: 'هذا المفتاح مربوط بجهاز ثاني. تواصل مع الدعم لنقله.' });
  }

  res.json({ valid: true, expiresAt: sub.expiresAt, name: sub.name });
});

app.post('/api/validate-owner', (req, res) => {
  res.json({ valid: req.body.password === OWNER_PASSWORD });
});

// Recover key by email
app.post('/api/recover-key', (req, res) => {
  const ip = req.ip || req.connection.remoteAddress;
  if (!checkRateLimit(ip)) return res.status(429).json({ ok: false, error: 'محاولات كثيرة. حاول بعد 15 دقيقة' });
  const email = (req.body.email || '').trim().toLowerCase();
  if (!email) return res.json({ ok: false, error: 'ادخل إيميلك' });
  const found = Object.entries(subscribers).find(([k, v]) => (v.email || '').toLowerCase() === email && v.active);
  if (!found) return res.json({ ok: false, error: 'ما لقينا اشتراك نشط بهذا الإيميل' });
  const [key, sub] = found;
  sendEmail(email, '[BthLab] مفتاح اشتراكك',
    `<div dir="rtl" style="font-family:Tahoma,sans-serif;max-width:500px;margin:0 auto;padding:20px;text-align:center">
      <h2 style="color:#0891b2">🔑 مفتاح اشتراكك</h2>
      <p style="font-size:14px">مرحباً ${sub.name || ''},</p>
      <div style="margin:20px 0;padding:16px;background:#f5f5f5;border-radius:12px;font-family:monospace;font-size:20px;font-weight:bold;letter-spacing:2px">${key}</div>
      <p style="font-size:12px;color:#888">ادخل هذا المفتاح في صفحة تسجيل الدخول</p>
      <p style="font-size:12px;color:#888">ينتهي اشتراكك: ${new Date(sub.expiresAt).toLocaleDateString('ar-SA')}</p>
      <p style="font-size:10px;color:#aaa;margin-top:20px">⚠️ لا تشارك هذا المفتاح مع أحد</p>
    </div>`
  );
  console.log(`[Recover] Key sent to ${email}`);
  res.json({ ok: true });
});

// ── 🟢 تتبع المشتركين المتصلين (للوحة المالك) ───────────
// key → Set من socket IDs (مشترك قد يفتح أكثر من تاب)
const onlineSubs = new Map();
function isOnline(key) { const s = onlineSubs.get(key); return !!(s && s.size > 0); }

app.get('/api/subscribers', (req, res) => {
  if (req.query.pw !== OWNER_PASSWORD) return res.status(403).json({ error: 'غير مصرح' });
  const list = Object.entries(subscribers).map(([key, v]) => ({
    key, ...v,
    isExpired: new Date(v.expiresAt) < new Date(),
    online: isOnline(key),
    tabs: (onlineSubs.get(key)?.size) || 0,
  }));
  res.json(list);
});

app.post('/api/subscribers/toggle', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  if (subscribers[req.body.key]) { subscribers[req.body.key].active = !subscribers[req.body.key].active; saveSubscribers(subscribers); res.json({ ok: true, active: subscribers[req.body.key].active }); }
  else res.json({ ok: false });
});

app.post('/api/subscribers/delete', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  if (subscribers[req.body.key]) { delete subscribers[req.body.key]; saveSubscribers(subscribers); res.json({ ok: true }); }
  else res.json({ ok: false });
});

app.post('/api/subscribers/extend', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  if (subscribers[req.body.key]) {
    const d = new Date(subscribers[req.body.key].expiresAt);
    d.setDate(d.getDate() + (parseInt(req.body.days) || 30));
    subscribers[req.body.key].expiresAt = d.toISOString();
    subscribers[req.body.key].active = true;
    saveSubscribers(subscribers);
    res.json({ ok: true, expiresAt: d.toISOString() });
  } else res.json({ ok: false });
});

// Owner: unbind device from subscriber
app.post('/api/subscribers/unbind-device', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  const sub = subscribers[req.body.key];
  if (!sub) return res.json({ ok: false });
  delete sub.deviceId;
  saveSubscribers(subscribers);
  console.log(`[Auth] Device unbound for ${req.body.key}`);
  res.json({ ok: true });
});

// Owner: change subscriber's tiktok username
app.post('/api/subscribers/change-username', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  const sub = subscribers[req.body.key];
  if (!sub) return res.json({ ok: false });
  const newUsername = (req.body.username || '').toLowerCase().replace('@', '').trim();
  if (!newUsername) return res.json({ ok: false, error: 'يوزرنيم مطلوب' });
  // Disconnect old room if exists
  if (sub.tiktokUsername && rooms[sub.tiktokUsername]) {
    const oldRoom = rooms[sub.tiktokUsername];
    if (oldRoom.retryTimer) clearTimeout(oldRoom.retryTimer);
    if (oldRoom.tiktok) { try { oldRoom.tiktok.disconnect(); } catch(_) {} }
    delete rooms[sub.tiktokUsername];
    io.to(`room:${sub.tiktokUsername}`).emit('room:status', { username: sub.tiktokUsername, status: 'removed' });
  }
  sub.tiktokUsername = newUsername;
  saveSubscribers(subscribers);
  res.json({ ok: true, username: newUsername });
});

// Contact support — sends to admin email
const supportMessages = [];
app.post('/api/contact', (req, res) => {
  const { name, email, subject, message, key } = req.body;
  if (!message || !message.trim()) return res.json({ ok: false, error: 'اكتب رسالتك' });
  const msg = {
    id: Date.now(),
    name: name || 'مجهول',
    email: email || '',
    subject: subject || 'بدون عنوان',
    message: message.trim(),
    key: key || '',
    tiktokUsername: key && subscribers[key] ? subscribers[key].tiktokUsername : '',
    createdAt: new Date().toISOString(),
    read: false,
  };
  supportMessages.push(msg);
  // Save to file
  const msgFile = path.join(PERSIST_DIR, 'messages.json');
  try { fs.writeFileSync(msgFile, JSON.stringify(supportMessages, null, 2), 'utf8'); } catch(e) {}
  console.log(`[Support] New message from ${msg.name} (${msg.email}): ${msg.subject}`);
  // Send email notification
  sendEmail(SUPPORT_EMAIL, `[BthLab Support] ${msg.subject}`,
    `<div dir="rtl" style="font-family:Tahoma,sans-serif;max-width:500px;margin:0 auto;padding:20px">
      <h2 style="color:#0891b2">📩 رسالة جديدة من الدعم</h2>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <tr><td style="padding:8px;font-weight:bold;color:#888">الاسم:</td><td style="padding:8px">${msg.name}</td></tr>
        <tr><td style="padding:8px;font-weight:bold;color:#888">الإيميل:</td><td style="padding:8px">${msg.email || '—'}</td></tr>
        <tr><td style="padding:8px;font-weight:bold;color:#888">المفتاح:</td><td style="padding:8px;font-family:monospace">${msg.key || '—'}</td></tr>
        <tr><td style="padding:8px;font-weight:bold;color:#888">التيك توك:</td><td style="padding:8px">@${msg.tiktokUsername || '—'}</td></tr>
        <tr><td style="padding:8px;font-weight:bold;color:#888">الموضوع:</td><td style="padding:8px;color:#0891b2">${msg.subject}</td></tr>
      </table>
      <div style="background:#f5f5f5;border-radius:10px;padding:16px;margin-top:12px;font-size:14px;line-height:1.8">${msg.message}</div>
    </div>`
  );
  res.json({ ok: true });
});

// Owner: get support messages
app.get('/api/messages', (req, res) => {
  if (req.query.pw !== OWNER_PASSWORD) return res.status(403).json({});
  const msgFile = path.join(PERSIST_DIR, 'messages.json');
  try { if (fs.existsSync(msgFile)) return res.json(JSON.parse(fs.readFileSync(msgFile, 'utf8'))); } catch(e) {}
  res.json([]);
});

app.post('/api/subscribers/add', (req, res) => {
  if (req.body.pw !== OWNER_PASSWORD) return res.status(403).json({});
  // مفتاح مخصص اختياري — يسمح بإعادة استخدام مفتاح محذوف سابقاً
  let key = (req.body.customKey || '').trim();
  if (key) {
    if (subscribers[key]) {
      return res.status(409).json({ ok: false, error: 'هذا المفتاح مستخدم لمشترك آخر' });
    }
    if (!/^[A-Za-z0-9_-]{6,64}$/.test(key)) {
      return res.status(400).json({ ok: false, error: 'مفتاح غير صالح — يسمح بـ A-Z a-z 0-9 _ - فقط (6-64 حرف)' });
    }
  } else {
    key = generateKey();
  }
  const now = new Date(); const expires = new Date(now); expires.setDate(expires.getDate() + (parseInt(req.body.days) || 30));
  subscribers[key] = { email: req.body.email||'', name: req.body.name||'', paymentId: 'manual', createdAt: now.toISOString(), expiresAt: expires.toISOString(), active: true };
  saveSubscribers(subscribers);
  res.json({ ok: true, key, expiresAt: expires.toISOString(), customized: !!req.body.customKey });
});

// ── Resolve key → username (for overlay auto-connect) ───
app.get('/api/resolve-key', (req, res) => {
  const key = req.query.key || '';
  const sub = subscribers[key];
  if (!sub || !sub.active || new Date(sub.expiresAt) < new Date()) {
    return res.json({ ok: false, error: 'مفتاح غير صالح أو منتهي' });
  }
  // Find connected room for this subscriber
  const connectedRooms = Object.keys(rooms).filter(u => rooms[u].status === 'connected' || rooms[u].status === 'connecting' || rooms[u].status === 'retrying');
  // Also store tiktokUsername on subscriber if set
  const tiktokUsername = sub.tiktokUsername || '';
  res.json({ ok: true, username: tiktokUsername, connectedRooms, name: sub.name });
});

// ── Save TikTok username to subscriber ──────────────────
app.post('/api/save-username', (req, res) => {
  const { key, username } = req.body;
  if (!key || !username) return res.json({ ok: false });
  const sub = subscribers[key];
  if (!sub || !sub.active || new Date(sub.expiresAt) < new Date()) {
    return res.json({ ok: false, error: 'مفتاح غير صالح' });
  }
  sub.tiktokUsername = username.toLowerCase().replace('@', '').trim();
  saveSubscribers(subscribers);
  res.json({ ok: true, username: sub.tiktokUsername });
});

// ── My Links (generate all overlay links for subscriber) ─
app.get('/api/my-links', (req, res) => {
  const key = req.query.key || '';
  const sub = subscribers[key];
  if (!sub || !sub.active || new Date(sub.expiresAt) < new Date()) {
    return res.json({ ok: false, error: 'مفتاح غير صالح' });
  }
  const tiktokUsername = sub.tiktokUsername || '';
  const baseUrl = `${req.protocol}://${req.get('host')}`;
  const links = {
    wheelOverlay: `${baseUrl}/wheel-overlay.html?key=${encodeURIComponent(key)}`,
    chatOverlay: `${baseUrl}/overlay.html?key=${encodeURIComponent(key)}`,
    wheel: `${baseUrl}/wheel.html?key=${encodeURIComponent(key)}`,
    admin: `${baseUrl}/admin.html?mode=subscriber&key=${encodeURIComponent(key)}`,
    sounds: `${baseUrl}/sounds.html?key=${encodeURIComponent(key)}`,
  };
  res.json({ ok: true, tiktokUsername, links, name: sub.name, expiresAt: sub.expiresAt });
});

// ── Auth middleware (protect pages) ──────────────────────
// ── Auth: cookies primary, query string fallback ─────────
// لما المستخدم يصل برابط فيه ?key= أو ?pw=، نحفظهم في cookies HttpOnly
// عشان الطلبات اللاحقة ما تحتاجهم في الـ URL — يبقى الرابط نظيف بدون كشف
function setAuthCookie(res, name, value) {
  // 30 يوم، HttpOnly (محمي من JS)، SameSite=Lax (يشتغل مع التنقل العادي)
  const secure = process.env.NODE_ENV === 'production' ? '; Secure' : '';
  res.setHeader('Set-Cookie', (res.getHeader('Set-Cookie') || [])
    .concat(`${name}=${encodeURIComponent(value)}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax${secure}`));
}

function requireAuth(req, res, next) {
  const qKey = req.query.key || '';
  const qPw = req.query.pw || '';
  const cKey = req.cookies.bthlab_key || '';
  const cPw = req.cookies.bthlab_pw || '';
  const key = qKey || cKey;
  const pw = qPw || cPw;

  // Owner bypass
  if (pw === OWNER_PASSWORD) {
    if (qPw && !cPw) setAuthCookie(res, 'bthlab_pw', pw);
    return next();
  }
  // Subscriber check
  const sub = subscribers[key];
  if (!sub || !sub.active || new Date(sub.expiresAt) < new Date()) return res.redirect('/login.html');
  if (qKey && !cKey) setAuthCookie(res, 'bthlab_key', key);
  next();
}

// مسار تسجيل الخروج — يمسح cookies
app.post('/api/logout', (req, res) => {
  res.setHeader('Set-Cookie', [
    'bthlab_key=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax',
    'bthlab_pw=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax',
  ]);
  res.json({ ok: true });
});

// ── 🔒 حماية API الفعاليات: يتحقق أن المتصل يملك الـ username المطلوب ───
// المالك يقدر يصل لأي حساب، والمشترك يقدر يصل فقط للحساب المسجّل عنده
function requireGameAccess(req, res, next) {
  const key = req.cookies.bthlab_key || req.query.key || '';
  const pw = req.cookies.bthlab_pw || req.query.pw || '';
  const reqUsername = (req.body?.username || req.query?.username || req.params?.username || '').toLowerCase().replace('@','').trim();
  if (!reqUsername) return res.status(400).json({ ok: false, error: 'username مطلوب' });
  // المالك يمر دائماً
  if (pw === OWNER_PASSWORD) return next();
  // المشترك يمر فقط إذا الـ username يطابق المسجّل عنده
  const sub = subscribers[key];
  if (!sub || !sub.active || new Date(sub.expiresAt) < new Date()) {
    return res.status(401).json({ ok: false, error: 'غير مصرح — سجّل الدخول' });
  }
  if (sub.tiktokUsername !== reqUsername) {
    return res.status(403).json({ ok: false, error: 'غير مصرح بالوصول لهذا الحساب' });
  }
  next();
}

// Redirect root to landing page
// index.html served automatically by express.static

// Protected pages
app.get('/wheel.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/wheel.html')));
app.get('/admin.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/admin.html')));
app.get('/password.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/password.html')));
app.get('/word-war.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/word-war.html')));
app.get('/guess.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/guess.html')));
app.get('/knockout.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/knockout.html')));
app.get('/quiz.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/quiz.html')));
app.get('/poll.html', requireAuth, (req, res) => res.sendFile(path.join(__dirname, '../public/poll.html')));
app.get('/subscriptions.html', (req, res) => {
  const pw = req.query.pw || req.cookies.bthlab_pw || '';
  if (pw !== OWNER_PASSWORD) return res.redirect('/login.html');
  if (req.query.pw && !req.cookies.bthlab_pw) setAuthCookie(res, 'bthlab_pw', pw);
  res.sendFile(path.join(__dirname, '../public/subscriptions.html'));
});

// Static files (login, subscribe, callback, overlays)
app.use(express.static(path.join(__dirname, '../public')));

// ══════════════════════════════════════════════════════════
// ── TikTok Connection ────────────────────────────────────
// ══════════════════════════════════════════════════════════
const rooms = {};

// ── TikTok v1/v2 data normalization ──────────────────────
// tiktok-live-api v2.x (نفس بنية tiktok-live-connector v2) نقل حقول المستخدم داخل data.user
// والصورة صارت قائمة روابط في avatarThumb/profilePicture
function pickUrl(p) {
  if (!p) return null;
  if (typeof p === 'string') return p;
  const list = p.urlList || p.urls || p.url;
  if (Array.isArray(list) && list.length) return list[0];
  if (typeof list === 'string') return list;
  return null;
}
function extractUser(data) {
  const u = (data && data.user) || {};
  const userId = String(data.userId || u.userId || u.id || data.uniqueId || u.uniqueId || '') || null;
  const uniqueId = data.uniqueId || u.uniqueId || '';
  const nickname = data.nickname || u.nickname || uniqueId || 'مشاهد';
  const avatar = data.profilePictureUrl
    || pickUrl(u.profilePicture) || pickUrl(u.avatarThumb)
    || pickUrl(data.profilePicture) || pickUrl(data.avatarThumb)
    || null;
  const isModerator = (data.isModerator !== undefined) ? data.isModerator : (u.isModerator || false);
  const isSubscriber = (data.isSubscriber !== undefined) ? data.isSubscriber : (u.isSubscriber || false);
  const followRole = (data.followRole !== undefined) ? data.followRole : ((u.followInfo && u.followInfo.followStatus) !== undefined ? u.followInfo.followStatus : u.followRole);
  return { userId, uniqueId, nickname, avatar, isModerator, isSubscriber, followRole };
}
const MAX_STORED = 100;

function normalizeAr(s) {
  if (!s) return '';
  return s.trim()
    .replace(/[أإآا]/g, 'ا')
    .replace(/[ةه]/g, 'ه')
    .replace(/[يى]/g, 'ي')
    .replace(/\u0640/g, '')
    .replace(/[\u064B-\u065F]/g, '')
    .toLowerCase();
}

function broadcast(key, event, data) { io.to(`room:${key}`).emit(event, data); }

function storeMsg(key, msg) {
  const room = rooms[key]; if (!room) return;
  room.messages.push(msg);
  if (room.messages.length > MAX_STORED) room.messages.shift();
}

async function connectRoom(username, sessionid = null) {
  const key = username.toLowerCase().replace('@', '').trim();
  if (!rooms[key]) {
    rooms[key] = { tiktok: null, stats: { viewers:0, likes:0, diamonds:0, shares:0, followers:0 }, followerSet: new Set(), messages: [], status: 'idle', retryTimer: null, sessionid: sessionid || null, gifts: {} };
  } else if (sessionid) { rooms[key].sessionid = sessionid; }
  const room = rooms[key];
  if (room.status === 'connected') return;
  if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
  if (room.tiktok) { try { room.tiktok.disconnect(); } catch(_) {} room.tiktok = null; }

  room.status = 'connecting';
  io.to(`room:${key}`).emit('room:status', { username: key, status: 'connecting' });
  console.log(`[TikTok] Connecting to @${key}...`);

  const opts = { apiKey: TIKTOK_API_KEY, processInitialData: false, enableExtendedGiftInfo: true };
  if (sessionid) opts.sessionId = sessionid;
  if (room.sessionid) opts.sessionId = room.sessionid;
  const tiktok = new TikTokLive(key, opts);
  room.tiktok = tiktok;

  try {
    // مكتبة tik.tools ترجع undefined من connect() - الحالة تجي عبر حدث 'connected'
    // فنحط listener أولاً ثم نطلب الاتصال
    let connectedState = null;
    tiktok.on('connected', (state) => {
      connectedState = state || {};
      room.status = 'connected';
      room.retryCount = 0;
      // viewerCount قد يكون في state.viewerCount أو state.roomInfo.user_count
      const viewers = state?.viewerCount
        || state?.roomInfo?.user_count
        || state?.roomInfo?.viewerCount
        || 0;
      room.stats.viewers = viewers;
      console.log(`[TikTok] ✅ Connected @${key} (viewers: ${viewers})`);
      io.to(`room:${key}`).emit('room:status', { username: key, status: 'connected', viewers });
      broadcast(key, 'stats', room.stats);
    });

    // محاولة الاتصال — قد ترجع undefined لكن الحدث connected يطلق بنجاح
    const ret = await tiktok.connect();
    // لو المكتبة رجعت state بدلاً من الحدث، استخدمه
    if (ret && !connectedState) {
      connectedState = ret;
      room.status = 'connected';
      room.retryCount = 0;
      const viewers = ret.viewerCount || ret.roomInfo?.user_count || 0;
      room.stats.viewers = viewers;
      console.log(`[TikTok] ✅ Connected @${key} (viewers: ${viewers}, via return)`);
      io.to(`room:${key}`).emit('room:status', { username: key, status: 'connected', viewers });
      broadcast(key, 'stats', room.stats);
    }
    // لو لا حدث connected ولا return — اعتبره ناجح بدون viewers
    if (!connectedState) {
      room.status = 'connected';
      room.retryCount = 0;
      console.log(`[TikTok] ✅ Connected @${key} (silent)`);
      io.to(`room:${key}`).emit('room:status', { username: key, status: 'connected', viewers: 0 });
      broadcast(key, 'stats', room.stats);
    }
  } catch(err) {
    const msg = err.message || String(err) || 'unknown';
    let userMsg = '';
    // تحليل دقيق لسبب الفشل
    if (/not.*live|stream.*end|not.*online|userOffline|room.*not.*found/i.test(msg)) {
      userMsg = '⚠️ الحساب مش مباشر حالياً. ابدأ البث في تيك توك ثم اضغط "إعادة الاتصال"';
    } else if (/user.*not.*found|account.*not.*found|404/i.test(msg)) {
      userMsg = '❌ اليوزرنيم غير موجود في تيك توك — تأكد من الإملاء';
    } else if (/sign.*request|signing|signature|401|403/i.test(msg)) {
      userMsg = '🔑 مشكلة في مفتاح tik.tools — تواصل مع الدعم';
    } else if (/rate.*limit|429|too.*many/i.test(msg)) {
      userMsg = '⏱️ تجاوز معدل الطلبات — انتظر دقيقة وأعد المحاولة';
    } else if (/ban|forbidden|blocked/i.test(msg)) {
      userMsg = '🚫 تيك توك حظر هذا الـ IP مؤقتاً — تواصل مع الدعم';
    } else {
      userMsg = `❌ فشل الاتصال: ${msg.slice(0, 100)}`;
    }
    console.log(`[TikTok] ❌ Failed @${key}: ${msg}`);
    console.log(`[TikTok]    سبب مبسط: ${userMsg}`);
    room.status = 'error';
    io.to(`room:${key}`).emit('room:status', { username: key, status: 'error', message: userMsg, technical: msg });
    scheduleRetry(key, 0, msg);
    return;
  }

  tiktok.on('chat', (data) => {
    const usr = extractUser(data);
    const msg = { type:'chat', id: data.msgId || Date.now(), user: usr.nickname, avatar: usr.avatar, comment: data.comment, isModerator: usr.isModerator, isSubscriber: usr.isSubscriber, followRole: usr.followRole, ts: Date.now() };
    storeMsg(key, msg);
    broadcast(key, 'chat', msg);
    // Wheel keyword check
    const wheel = getWheel(key);
    if (wheel.accepting && wheel.keyword && data.comment && data.comment.trim().includes(wheel.keyword) && usr.userId && !wheel.entries.has(usr.userId)) {
      const entry = { userId: usr.userId, name: usr.nickname, avatar: usr.avatar };
      wheel.entries.set(usr.userId, entry);
      broadcast(key, 'wheel:update', { entries: Array.from(wheel.entries.values()), count: wheel.entries.size, newEntry: entry });
    }

    // Password game check
    const pw = getPassword(key);
    if (pw.active && pw.word && data.comment) {
      const guess = data.comment.trim();
      if (guess === pw.word && !pw.winner) {
        pw.winner = { userId: usr.userId, name: usr.nickname, avatar: usr.avatar };
        pw.active = false;
        pw.revealed = new Array(pw.word.length).fill(true);
        broadcast(key, 'password:winner', { winner: pw.winner, word: pw.word });
        broadcast(key, 'password:update', { length: pw.word.length, revealed: pw.revealed, letters: pw.word.split(''), active: false, winner: pw.winner, hints: pw.hints });
      }
    }

    // Check Word War game (حرب الكلمات)
    const wwGame = getWordWarGame(key);
    if (data.comment && (wwGame.active || wwGame.registrationOpen)) {
      const word = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = usr.userId;

      if (uid && !wwGame.registrationLocked && (word === wwGame.redKeyword || word === wwGame.blueKeyword) && !wwGame.redTeam.has(uid) && !wwGame.blueTeam.has(uid)) {
        const team = word === wwGame.redKeyword ? 'red' : 'blue';
        const teamMap = team === 'red' ? wwGame.redTeam : wwGame.blueTeam;
        teamMap.set(uid, { name: usr.nickname, avatar: usr.avatar, words: [] });
        broadcast(key, 'word-war:join', { team, player: usr.nickname, avatar: usr.avatar, redCount: wwGame.redTeam.size, blueCount: wwGame.blueTeam.size });
      }
      else if (wwGame.active) {
        let team = null;
        if (wwGame.redTeam.has(uid)) team = 'red';
        else if (wwGame.blueTeam.has(uid)) team = 'blue';
        if (team) {
          const isValid = wwGame.validWords.length === 0 || wwGame.validWords.includes(word);
          if (isValid && word.length >= 2) {
            const teamWords = team === 'red' ? wwGame.redWords : wwGame.blueWords;
            const oppositeWords = team === 'red' ? wwGame.blueWords : wwGame.redWords;
            const teamMap = team === 'red' ? wwGame.redTeam : wwGame.blueTeam;
            if (!teamWords.has(word) && !oppositeWords.has(word)) {
              teamWords.add(word);
              const player = teamMap.get(uid);
              if (player) player.words.push(word);
              if (team === 'red') wwGame.redScore++; else wwGame.blueScore++;
              broadcast(key, 'word-war:word', { team, word: data.comment.trim(), player: usr.nickname, avatar: usr.avatar, redScore: wwGame.redScore, blueScore: wwGame.blueScore });
            }
          }
        }
      }
    }

    // Check Guess Game (خمن الكلمة)
    const guessGame = getGuessGame(key);

    // Check Knockout (بطولة الخروج)
    const ko = getKnockout(key);
    if (data.comment && ko.phase !== 'idle') {
      const comment = data.comment.trim().toLowerCase().replace(/\s+/g,'');
      const uid = usr.userId;
      // Registration phase
      if (uid && ko.phase === 'register' && comment === ko.keyword && !ko.players.has(uid) && ko.players.size < ko.maxPlayers) {
        ko.players.set(uid, { name: usr.nickname, avatar: usr.avatar, alive: true });
        broadcast(key, 'knockout:joined', { count: ko.players.size, max: ko.maxPlayers, player: usr.nickname, avatar: usr.avatar });
      }
      // Question phase — answer by number
      if (ko.phase === 'question' && ko.currentQuestion) {
        const p = ko.players.get(uid);
        if (p && p.alive && !ko.currentQuestion.answers.has(uid)) {
          const num = parseInt(data.comment.trim());
          if (num >= 1 && num <= ko.currentQuestion.options.length) {
            ko.currentQuestion.answers.set(uid, num - 1);
            broadcast(key, 'knockout:answered', { count: ko.currentQuestion.answers.size, total: Array.from(ko.players.values()).filter(pp => pp.alive).length });
          }
        }
      }
    }

    // Check Guess Game (خمن الكلمة) — continued
    if (guessGame.active && !guessGame.transitioning && data.comment) {
      const commentClean = normalizeAr(data.comment);
      const wordClean = normalizeAr(guessGame.word);
      const alreadyWon = guessGame.winners.some(w => w.userId === usr.userId || w.name === usr.nickname);
      if (guessGame.winners.length < 5 && commentClean && wordClean && commentClean === wordClean && !alreadyWon) {
        const uid = usr.userId || usr.nickname;
        const existing = guessGame.playerStats.get(uid) || { name: usr.nickname, avatar: usr.avatar, totalWords: 0 };
        existing.totalWords += 1;
        existing.name = usr.nickname;
        existing.avatar = usr.avatar;
        guessGame.playerStats.set(uid, existing);
        const winner = { userId: uid, name: existing.name, avatar: existing.avatar, rank: guessGame.winners.length + 1, word: guessGame.word, totalWords: existing.totalWords };
        guessGame.winners.push(winner);
        const allPlayers = Array.from(guessGame.playerStats.entries()).map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalWords: s.totalWords })).sort((a,b) => b.totalWords - a.totalWords);
        broadcast(key, 'guess:won', { winner, word: guessGame.word, winners: guessGame.winners, allPlayers });
        if (guessGame.winners.length === 5 && !guessGame.transitioning) {
          guessGame.active = false;
          guessGame.transitioning = true;
          broadcast(key, 'guess:reveal', { word: guessGame.word });
        }
      }
    }

    // Quiz check — answer by number (1, 2, 3, 4)
    const quiz = getQuiz(key);
    if (quiz.active && data.comment && usr.userId) {
      const num = parseInt(data.comment.trim());
      if (num >= 1 && num <= quiz.choices.length && !quiz.answers.has(usr.userId)) {
        quiz.answers.set(usr.userId, num - 1);
        if (num - 1 === quiz.correctIndex && !quiz.winner) {
          quiz.winner = { userId: usr.userId, name: usr.nickname, avatar: usr.avatar };
        }
        broadcast(key, 'quiz:answer', { totalAnswers: quiz.answers.size });
      }
    }

    // Poll check — vote by number (1, 2, 3, ...)
    const poll = getPoll(key);
    if (poll.active && data.comment && usr.userId) {
      const num = parseInt(data.comment.trim());
      if (num >= 1 && num <= poll.options.length && !poll.votes.has(usr.userId)) {
        poll.votes.set(usr.userId, num - 1);
        const results = {};
        poll.options.forEach((_, i) => results[i] = 0);
        poll.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
        broadcast(key, 'poll:vote', { results, totalVotes: poll.votes.size });
      }
    }
  });

  tiktok.on('like', (data) => { const usr = extractUser(data); if (data.totalLikeCount) room.stats.likes = data.totalLikeCount; broadcast(key, 'like', { user: usr.nickname, totalLikeCount: data.totalLikeCount }); broadcast(key, 'stats', room.stats); });

  tiktok.on('gift', (data) => {
    const usr = extractUser(data);
    const gd = data.giftDetails || {};
    const g = data.gift || {};
    const gType = (data.giftType !== undefined) ? data.giftType : (gd.giftType !== undefined ? gd.giftType : g.gift_type);
    const gRepeatEnd = (data.repeatEnd !== undefined) ? data.repeatEnd : g.repeat_end;
    if (gType === 1 && !gRepeatEnd) return;
    const gId = data.giftId || g.id || gd.giftId;
    const gRepeat = data.repeatCount || g.repeat_count || 1;
    const gName = data.giftName || gd.giftName || g.name || 'Gift';
    const gDiamonds = data.diamondCount || gd.diamondCount || g.diamond_count || 0;
    const giftKey = `${usr.userId}-${gId}-${gRepeat}`;
    const now = Date.now();
    room.recentGifts = room.recentGifts || {};
    if (room.recentGifts[giftKey] && now - room.recentGifts[giftKey] < 2000) return;
    room.recentGifts[giftKey] = now;
    if (Object.keys(room.recentGifts).length > 100) { for (const k in room.recentGifts) { if (now - room.recentGifts[k] > 10000) delete room.recentGifts[k]; } }
    room.stats.diamonds += gDiamonds * gRepeat;
    const msg = { type:'gift', user: usr.nickname, avatar: usr.avatar, giftName: gName, giftId: gId, repeatCount: gRepeat, diamondCount: gDiamonds, ts: Date.now() };
    storeMsg(key, msg); broadcast(key, 'gift', msg); broadcast(key, 'stats', room.stats);
  });

  tiktok.on('member', (data) => { const usr = extractUser(data); const msg = { type:'member', user: usr.nickname, avatar: usr.avatar, actionId: data.actionId, ts: Date.now() }; if (data.actionId === 1) storeMsg(key, msg); broadcast(key, 'member', msg); });
  tiktok.on('follow', (data) => { const usr = extractUser(data); const uid = usr.userId; if (uid && !room.followerSet.has(uid)) { room.followerSet.add(uid); room.stats.followers = room.followerSet.size; broadcast(key, 'stats', room.stats); } broadcast(key, 'follow', { type:'follow', user: usr.nickname, avatar: usr.avatar, ts: Date.now() }); });
  tiktok.on('share', (data) => { const usr = extractUser(data); room.stats.shares = (room.stats.shares || 0) + 1; broadcast(key, 'share', { type:'share', user: usr.nickname, ts: Date.now() }); broadcast(key, 'stats', room.stats); });
  tiktok.on('roomUser', (data) => { room.stats.viewers = data.viewerCount || room.stats.viewers; broadcast(key, 'viewers', { count: data.viewerCount }); broadcast(key, 'stats', room.stats); });
  tiktok.on('streamEnd', () => {
    // البث انتهى رسمياً من المستخدم — توقف نظيف، بدون محاولات
    room.status = 'ended';
    if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
    console.log(`[TikTok] @${key} انتهى البث رسمياً — توقف نظيف`);
    io.to(`room:${key}`).emit('room:status', { username: key, status: 'offline', message: '🛑 انتهى البث — اضغط "اتصال" يدوياً عند بدء بث جديد' });
  });
  tiktok.on('disconnected', () => {
    if (room.status === 'connected') {
      // انقطاع غير متعمد — توقف نظيف، المستخدم يضغط يدوياً
      room.status = 'offline';
      if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
      console.log(`[TikTok] @${key} انقطع الاتصال — توقف نظيف`);
      io.to(`room:${key}`).emit('room:status', { username: key, status: 'offline', message: '⚠️ انقطع الاتصال — اضغط "اتصال" يدوياً للمحاولة مرة ثانية' });
    }
  });
  tiktok.on('error', (err) => {
    const msg = String(err?.message || err || '');
    console.log(`[TikTok] خطأ @${key}: ${msg}`);
    scheduleRetry(key, 0, msg);
  });
}

// ── 🛡️ حماية ضد الباند: فحص حالة البث + Rate Limiting ──
// قبل أي محاولة اتصال، نتحقق من tik.tools أن المستخدم live فعلاً
// هذا يمنع المحاولات المتكررة التي سببت الباند سابقاً
async function checkIfLive(username) {
  if (!TIKTOK_API_KEY) return { isLive: false, error: 'مفتاح API مفقود' };
  try {
    const url = `https://api.tik.tools/live/check?uniqueId=${encodeURIComponent(username)}&apiKey=${encodeURIComponent(TIKTOK_API_KEY)}`;
    const res = await fetch(url, { method: 'GET' });
    if (!res.ok) {
      // 404 = user not found, 429 = rate limit
      if (res.status === 429) return { isLive: false, error: 'تجاوز معدل الطلبات — انتظر دقيقة' };
      return { isLive: false, error: `فشل التحقق (${res.status})` };
    }
    const data = await res.json();
    return { isLive: !!(data.isLive || data.data?.isLive), data };
  } catch (err) {
    console.log('[liveCheck] خطأ:', err.message);
    return { isLive: false, error: 'فشل الاتصال بـ tik.tools' };
  }
}

// Rate limiting لكل user + للسيرفر كله (يمنع الضغط المتكرر السريع)
const lastConnectAttempt = new Map(); // username → timestamp
const PER_USER_COOLDOWN = 10 * 1000;    // 10 ثوانٍ بين المحاولات لنفس المستخدم
let lastGlobalAttempt = 0;
const GLOBAL_COOLDOWN = 1500;            // 1.5 ثانية بين أي محاولات (كل السيرفر)

function checkRateLimit(username) {
  const now = Date.now();
  const userLast = lastConnectAttempt.get(username) || 0;
  if (now - userLast < PER_USER_COOLDOWN) {
    const wait = Math.ceil((PER_USER_COOLDOWN - (now - userLast)) / 1000);
    return { ok: false, error: `انتظر ${wait} ثانية قبل المحاولة مرة ثانية` };
  }
  if (now - lastGlobalAttempt < GLOBAL_COOLDOWN) {
    return { ok: false, error: 'السيرفر مشغول — جرّب بعد ثوان' };
  }
  lastConnectAttempt.set(username, now);
  lastGlobalAttempt = now;
  // تنظيف الـ Map من entries قديمة (أكثر من ساعة)
  if (lastConnectAttempt.size > 100) {
    for (const [k, v] of lastConnectAttempt) {
      if (now - v > 3600000) lastConnectAttempt.delete(k);
    }
  }
  return { ok: true };
}

// 🚫 إلغاء إعادة المحاولة التلقائية كلياً
// الإصدارات السابقة كانت تعيد المحاولة 5-30 مرة → سبب الباند
// الآن: محاولة واحدة فقط، المستخدم يضغط "اتصال" يدوياً عند الفشل
function scheduleRetry(key, delay, reason = '') {
  const room = rooms[key]; if (!room) return;
  console.log(`[TikTok] ⛔ توقف الاتصال @${key} (${reason}) — المستخدم يحتاج يضغط "اتصال" يدوياً`);
  room.status = 'offline';
  room.retryCount = 0;
  if (room.retryTimer) { clearTimeout(room.retryTimer); room.retryTimer = null; }
  // رسالة واضحة للمستخدم — لا محاولات تلقائية
  let userMsg = 'توقف الاتصال';
  const notLive = /not.*live|offline|stream.*end|room.*not.*found|UserOffline|not.*online/i.test(String(reason));
  if (notLive) userMsg = '⚠️ الحساب مش مباشر حالياً — اضغط "اتصال" بعد بدء البث';
  else if (/sign|signature|429|rate/i.test(String(reason))) userMsg = 'تجاوز معدل الطلبات — انتظر دقيقة وأعد المحاولة';
  else userMsg = `توقف الاتصال (${reason || 'خطأ'}) — اضغط "اتصال" يدوياً للمحاولة مرة ثانية`;
  io.to(`room:${key}`).emit('room:status', { username: key, status: 'offline', message: userMsg });
}

// ── Wheel Store ──────────────────────────────────────────
const wheels = {};
function getWheel(key) {
  if (!wheels[key]) wheels[key] = { keyword: 'اشتراك', entries: new Map(), accepting: false, removedIds: new Set() };
  return wheels[key];
}

app.post('/api/wheel/config', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); if (req.body.keyword) w.keyword = req.body.keyword; if (req.body.title) w.title = req.body.title; if (req.body.colors) w.colors = req.body.colors; if (req.body.textColor) w.textColor = req.body.textColor; if (req.body.pointerColor) w.pointerColor = req.body.pointerColor; if (req.body.spinDuration) w.spinDuration = req.body.spinDuration; io.to(`room:${key}`).emit('wheel:config', { keyword: w.keyword, title: w.title, colors: w.colors, textColor: w.textColor, pointerColor: w.pointerColor, spinDuration: w.spinDuration }); res.json({ ok: true }); });
app.post('/api/wheel/clear', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); w.entries.clear(); w.removedIds.clear(); io.to(`room:${key}`).emit('wheel:update', { entries: [], count: 0, fullSync: true }); res.json({ ok: true }); });
app.post('/api/wheel/add', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key || !req.body.name) return res.json({ ok: false }); const w = getWheel(key); const userId = 'manual_' + Date.now(); const entry = { userId, name: req.body.name.trim(), avatar: null }; w.entries.set(userId, entry); io.to(`room:${key}`).emit('wheel:update', { entries: Array.from(w.entries.values()), count: w.entries.size, newEntry: entry }); res.json({ ok: true, entry }); });
app.post('/api/wheel/start-registration', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); w.accepting = true; const dur = parseInt(req.body.duration) || 0; const endTime = dur > 0 ? Date.now() + dur * 1000 : 0; w.regEndTime = endTime; if (w.regTimer) clearTimeout(w.regTimer); if (dur > 0) { w.regTimer = setTimeout(() => { w.accepting = false; w.regEndTime = 0; io.to(`room:${key}`).emit('wheel:registration', { accepting: false, endTime: 0 }); }, dur * 1000); } io.to(`room:${key}`).emit('wheel:registration', { accepting: true, endTime, keyword: w.keyword || 'اشتراك', count: w.entries.size }); res.json({ ok: true }); });
app.post('/api/wheel/stop-registration', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); w.accepting = false; w.regEndTime = 0; if (w.regTimer) { clearTimeout(w.regTimer); w.regTimer = null; } io.to(`room:${key}`).emit('wheel:registration', { accepting: false, endTime: 0 }); res.json({ ok: true }); });
app.post('/api/wheel/remove-winner', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); w.entries.delete(req.body.userId); io.to(`room:${key}`).emit('wheel:update', { entries: Array.from(w.entries.values()), count: w.entries.size, fullSync: true }); res.json({ ok: true }); });
app.post('/api/wheel/remove', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); w.entries.delete(req.body.userId); io.to(`room:${key}`).emit('wheel:update', { entries: Array.from(w.entries.values()), count: w.entries.size, fullSync: true }); res.json({ ok: true }); });
app.post('/api/wheel/spin', requireGameAccess, (req, res) => { const key = req.body.username?.toLowerCase().replace('@','').trim(); if (!key) return res.json({ ok: false }); const w = getWheel(key); if (w.entries.size < 2) return res.json({ ok: false, message: 'يحتاج مشتركين أكثر' }); const entries = Array.from(w.entries.values()); const winnerIndex = Math.floor(Math.random() * entries.length); const winner = entries[winnerIndex]; const durationMs = (req.body.duration || 5) * 1000; io.to(`room:${key}`).emit('wheel:spin', { winner, winnerIndex, duration: durationMs, speed: req.body.speed || 'normal', entries }); res.json({ ok: true, winner }); });
app.get('/api/wheel/:username', requireGameAccess, (req, res) => { const key = req.params.username.toLowerCase().replace('@','').trim(); const w = getWheel(key); res.json({ keyword: w.keyword, entries: Array.from(w.entries.values()), count: w.entries.size, accepting: w.accepting, regEndTime: w.regEndTime || 0 }); });

// ══════════════════════════════════════════════════════════
// ── Password Game (كلمة السر) ────────────────────────────
// ══════════════════════════════════════════════════════════
const passwords = {};
function getPassword(key) {
  if (!passwords[key]) passwords[key] = { word: '', revealed: [], active: false, winner: null, hints: [] };
  return passwords[key];
}

app.post('/api/password/start', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const pw = getPassword(key);
  pw.word = (req.body.word || '').trim();
  pw.revealed = new Array(pw.word.length).fill(false);
  pw.active = true;
  pw.winner = null;
  pw.hints = [];
  // Reveal first and last letter
  if (pw.word.length >= 2) { pw.revealed[0] = true; pw.revealed[pw.word.length - 1] = true; }
  broadcast(key, 'password:update', { length: pw.word.length, revealed: pw.revealed, letters: pw.word.split('').map((c, i) => pw.revealed[i] ? c : '_'), active: true, winner: null, hints: pw.hints });
  res.json({ ok: true });
});

app.post('/api/password/reveal', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const pw = getPassword(key);
  if (!pw.active) return res.json({ ok: false });
  // Reveal a random hidden letter
  const hidden = pw.revealed.map((r, i) => r ? -1 : i).filter(i => i >= 0);
  if (hidden.length === 0) return res.json({ ok: false, message: 'كل الحروف مكشوفة' });
  const idx = hidden[Math.floor(Math.random() * hidden.length)];
  pw.revealed[idx] = true;
  broadcast(key, 'password:update', { length: pw.word.length, revealed: pw.revealed, letters: pw.word.split('').map((c, i) => pw.revealed[i] ? c : '_'), active: true, winner: null, hints: pw.hints });
  res.json({ ok: true });
});

app.post('/api/password/hint', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key || !req.body.hint) return res.json({ ok: false });
  const pw = getPassword(key);
  pw.hints.push(req.body.hint);
  broadcast(key, 'password:update', { length: pw.word.length, revealed: pw.revealed, letters: pw.word.split('').map((c, i) => pw.revealed[i] ? c : '_'), active: pw.active, winner: pw.winner, hints: pw.hints });
  res.json({ ok: true });
});

app.post('/api/password/stop', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const pw = getPassword(key);
  pw.active = false;
  pw.revealed = new Array(pw.word.length).fill(true);
  broadcast(key, 'password:update', { length: pw.word.length, revealed: pw.revealed, letters: pw.word.split(''), active: false, winner: pw.winner, hints: pw.hints });
  res.json({ ok: true });
});

app.get('/api/password/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const pw = getPassword(key);
  res.json({ length: pw.word.length, revealed: pw.revealed, letters: pw.word.split('').map((c, i) => pw.revealed[i] ? c : '_'), active: pw.active, winner: pw.winner, hints: pw.hints });
});

// ══════════════════════════════════════════════════════════
// ── Word War (حرب الكلمات) — from private project ────────
// ══════════════════════════════════════════════════════════
const wordWarGames = {};
function getWordWarGame(key) {
  if (!wordWarGames[key]) wordWarGames[key] = {
    category: '', validWords: [], duration: 60,
    active: false, endTime: 0, endTimer: null,
    redTeam: new Map(), blueTeam: new Map(),
    redWords: new Set(), blueWords: new Set(),
    redScore: 0, blueScore: 0,
    roundHistory: [],
    registrationLocked: false, registrationOpen: false,
    redKeyword: 'أحمر', blueKeyword: 'أزرق',
  };
  return wordWarGames[key];
}

app.post('/api/word-war/start', requireGameAccess, (req, res) => {
  const { username, category, validWords, duration, redKeyword, blueKeyword, resetTeams, redColor, blueColor } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !category) return res.json({ ok: false });
  const game = getWordWarGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.category = category;
  game.validWords = (validWords || []).map(w => w.trim().toLowerCase().replace(/\s+/g,''));
  game.duration = Math.max(15, Math.min(120, parseInt(duration) || 60));
  game.active = true;
  game.endTime = Date.now() + game.duration * 1000;
  game.registrationOpen = false; // 🔧 انتهت مرحلة التسجيل بمجرد بدء الجولة
  if (resetTeams) { game.redTeam.clear(); game.blueTeam.clear(); game.redScore = 0; game.blueScore = 0; }
  game.redWords.clear(); game.blueWords.clear();
  for (const [uid, p] of game.redTeam) p.words = [];
  for (const [uid, p] of game.blueTeam) p.words = [];
  game.redKeyword = (redKeyword || 'أحمر').trim().toLowerCase().replace(/\s+/g,'');
  game.blueKeyword = (blueKeyword || 'أزرق').trim().toLowerCase().replace(/\s+/g,'');
  game.redColor = redColor || '#ef4444';
  game.blueColor = blueColor || '#3b82f6';
  if (resetTeams) game.registrationLocked = false;

  broadcast(key, 'word-war:start', { category: game.category, duration: game.duration, redKeyword: redKeyword || 'أحمر', blueKeyword: blueKeyword || 'أزرق', redCount: game.redTeam.size, blueCount: game.blueTeam.size, redScore: game.redScore, blueScore: game.blueScore, resetTeams: !!resetTeams, redColor: game.redColor, blueColor: game.blueColor });

  game.endTimer = setTimeout(() => {
    if (!game.active) return;
    game.active = false;
    game.registrationOpen = false; // 🔧 انتهت الجولة كلياً
    const result = { category: game.category, redScore: game.redScore, blueScore: game.blueScore, winner: game.redScore > game.blueScore ? 'red' : game.blueScore > game.redScore ? 'blue' : 'tie', redWords: [...game.redWords], blueWords: [...game.blueWords] };
    game.roundHistory.push(result);
    broadcast(key, 'word-war:end', result);
  }, game.duration * 1000);
  res.json({ ok: true });
});

app.post('/api/word-war/open-registration', requireGameAccess, (req, res) => {
  const { username, redKeyword, blueKeyword } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.redKeyword = (redKeyword || 'أحمر').trim().toLowerCase().replace(/\s+/g,'');
  game.blueKeyword = (blueKeyword || 'أزرق').trim().toLowerCase().replace(/\s+/g,'');
  game.registrationLocked = false;
  game.registrationOpen = true;
  broadcast(key, 'word-war:registration', { open: true, redKeyword: redKeyword || 'أحمر', blueKeyword: blueKeyword || 'أزرق', redCount: game.redTeam.size, blueCount: game.blueTeam.size });
  res.json({ ok: true });
});

app.post('/api/word-war/lock', requireGameAccess, (req, res) => {
  const { username, locked } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.registrationLocked = !!locked;
  broadcast(key, 'word-war:lock', { locked: game.registrationLocked });
  res.json({ ok: true });
});

app.post('/api/word-war/remove-player', requireGameAccess, (req, res) => {
  const { username, team, playerName } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  const teamMap = team === 'red' ? game.redTeam : game.blueTeam;
  for (const [uid, p] of teamMap) {
    if (p.name === playerName) { teamMap.delete(uid); broadcast(key, 'word-war:player-removed', { team, playerName, redCount: game.redTeam.size, blueCount: game.blueTeam.size }); break; }
  }
  res.json({ ok: true });
});

app.post('/api/word-war/add-player', requireGameAccess, (req, res) => {
  const { username, team, playerName } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !playerName) return res.json({ ok: false });
  const game = getWordWarGame(key);
  const teamMap = team === 'red' ? game.redTeam : game.blueTeam;
  const uid = 'manual_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
  teamMap.set(uid, { name: playerName.trim(), avatar: null, words: [] });
  broadcast(key, 'word-war:join', { team, player: playerName.trim(), avatar: null, redCount: game.redTeam.size, blueCount: game.blueTeam.size });
  res.json({ ok: true });
});

app.post('/api/word-war/clear-teams', requireGameAccess, (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.redTeam.clear(); game.blueTeam.clear();
  game.redWords.clear(); game.blueWords.clear();
  game.redScore = 0; game.blueScore = 0;
  broadcast(key, 'word-war:teams-cleared');
  res.json({ ok: true });
});

app.post('/api/word-war/stop', requireGameAccess, (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  if (game.endTimer) clearTimeout(game.endTimer);
  game.active = false;
  game.registrationOpen = false; // 🔧 إيقاف يدوي = إنهاء كل مراحل اللعبة
  broadcast(key, 'word-war:stopped');
  res.json({ ok: true });
});

app.post('/api/word-war/clear', requireGameAccess, (req, res) => {
  const { username } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  game.roundHistory = [];
  res.json({ ok: true });
});

app.get('/api/word-war/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getWordWarGame(key);
  const redPlayers = Array.from(game.redTeam.entries()).map(([uid, p]) => ({ userId: uid, ...p })).sort((a,b) => b.words.length - a.words.length);
  const bluePlayers = Array.from(game.blueTeam.entries()).map(([uid, p]) => ({ userId: uid, ...p })).sort((a,b) => b.words.length - a.words.length);
  res.json({ active: game.active, category: game.category, duration: game.duration, endTime: game.endTime, redScore: game.redScore, blueScore: game.blueScore, redWords: [...game.redWords], blueWords: [...game.blueWords], redPlayers, bluePlayers, roundHistory: game.roundHistory.slice(-10) });
});

// ══════════════════════════════════════════════════════════
// ── Knockout (بطولة الخروج) ──────────────────────────────
// ══════════════════════════════════════════════════════════
const knockoutGames = {};
function getKnockout(key) {
  if (!knockoutGames[key]) knockoutGames[key] = {
    phase: 'idle', keyword: 'بطولة', maxPlayers: 50,
    players: new Map(), round: 0,
    currentQuestion: null, answerTime: 15, timer: null,
  };
  return knockoutGames[key];
}

app.post('/api/knockout/register', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const ko = getKnockout(key);
  ko.phase = 'register';
  ko.keyword = (req.body.keyword || 'بطولة').trim().toLowerCase().replace(/\s+/g,'');
  ko.maxPlayers = parseInt(req.body.maxPlayers) || 50;
  ko.players.clear(); ko.round = 0; ko.currentQuestion = null;
  broadcast(key, 'knockout:register', { keyword: req.body.keyword || 'بطولة', maxPlayers: ko.maxPlayers });
  res.json({ ok: true });
});

app.post('/api/knockout/lock', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const ko = getKnockout(key);
  ko.phase = 'locked';
  const players = Array.from(ko.players.values());
  broadcast(key, 'knockout:locked', { count: ko.players.size, players });
  res.json({ ok: true });
});

app.post('/api/knockout/ask', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const ko = getKnockout(key);
  ko.phase = 'question'; ko.round++;
  ko.currentQuestion = {
    question: req.body.question || '',
    options: req.body.options || [],
    correct: parseInt(req.body.correct),
    answerTime: parseInt(req.body.answerTime) || 15,
    answers: new Map(),
  };
  const alive = Array.from(ko.players.values()).filter(p => p.alive).length;
  broadcast(key, 'knockout:question', { question: ko.currentQuestion.question, options: ko.currentQuestion.options, round: ko.round, alivePlayers: alive, answerTime: ko.currentQuestion.answerTime });
  // Auto-reveal after answerTime
  if (ko.timer) clearTimeout(ko.timer);
  ko.timer = setTimeout(() => {
    if (ko.phase === 'question') revealKnockout(key);
  }, (ko.currentQuestion.answerTime + 2) * 1000);
  res.json({ ok: true });
});

function revealKnockout(key) {
  const ko = getKnockout(key);
  const q = ko.currentQuestion; if (!q) return;
  ko.phase = 'reveal';
  const eliminated = [];
  // q.correct يأتي 1-based من صفحة التحكم، وإجابات المشاهدين تُحفظ 0-based (num-1)
  // لذلك نحوّل q.correct إلى 0-based للمقارنة
  const correctIdx = q.correct - 1;
  ko.players.forEach((p, uid) => {
    if (!p.alive) return;
    const ans = q.answers.get(uid);
    if (ans === undefined || ans !== correctIdx) { p.alive = false; eliminated.push({ name: p.name, avatar: p.avatar }); }
  });
  const alive = Array.from(ko.players.values()).filter(p => p.alive);
  const winner = alive.length <= 1 ? (alive[0] || null) : null;
  broadcast(key, 'knockout:reveal', { correct: q.correct, eliminated, aliveCount: alive.length, winner, round: ko.round });
}

app.post('/api/knockout/reveal', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  if (getKnockout(key).timer) clearTimeout(getKnockout(key).timer);
  revealKnockout(key);
  res.json({ ok: true });
});

app.post('/api/knockout/stop', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const ko = getKnockout(key);
  ko.phase = 'idle';
  if (ko.timer) { clearTimeout(ko.timer); ko.timer = null; }
  broadcast(key, 'knockout:stopped', {});
  res.json({ ok: true });
});

app.get('/api/knockout/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const ko = getKnockout(key);
  const players = Array.from(ko.players.values());
  const alive = players.filter(p => p.alive);
  const eliminated = players.filter(p => !p.alive);
  res.json({ phase: ko.phase, round: ko.round, totalPlayers: ko.players.size, aliveCount: alive.length, players, eliminated, alive });
});

// ══════════════════════════════════════════════════════════
// ── Guess Game (خمن الكلمة) ──────────────────────────────
// ══════════════════════════════════════════════════════════
const guessGames = {};
function getGuessGame(key) {
  if (!guessGames[key]) guessGames[key] = {
    word: '', hint: '', active: false, winner: null,
    revealed: [], winners: [], playerStats: new Map(),
    autoMode: false, wordPool: [], usedWords: new Set(),
    scale: 1, transitionDelay: 5000, pendingTransition: null, transitioning: false,
  };
  return guessGames[key];
}

function goToNextWord(key) {
  const game = getGuessGame(key);
  game.pendingTransition = null;
  game.transitioning = false;
  let pool = (game.wordPool && game.wordPool.length) ? game.wordPool.filter(w => w.w !== game.word && !game.usedWords.has(w.w)) : [];
  if (!pool.length && game.wordPool && game.wordPool.length) { game.usedWords.clear(); pool = game.wordPool.filter(w => w.w !== game.word); }
  if (!pool.length) { game.winners = []; game.active = true; broadcast(key, 'guess:started', { length: game.word.length, hint: game.hint, revealed: game.revealed, letters: game.revealed.map(i => ({ i, c: game.word[i] })), winners: [] }); return; }
  const next = pool[Math.floor(Math.random() * pool.length)];
  game.word = next.w; game.hint = next.h || ''; game.active = true; game.winners = []; game.usedWords.add(next.w);
  const indices = [...Array(next.w.length).keys()];
  const revealCount = Math.max(1, Math.floor(next.w.length * 0.3));
  game.revealed = indices.sort(() => Math.random() - 0.5).slice(0, revealCount).sort((a,b) => a-b);
  broadcast(key, 'guess:started', { length: game.word.length, hint: game.hint, revealed: game.revealed, letters: game.revealed.map(i => ({ i, c: game.word[i] })), winners: [] });
}

app.post('/api/guess/start', requireGameAccess, (req, res) => {
  const { username, word, hint, wordPool } = req.body;
  const key = username?.toLowerCase().replace('@','').trim();
  if (!key || !word) return res.json({ ok: false });
  const game = getGuessGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  game.transitioning = false;
  if (!game.active) game.usedWords = new Set();
  game.word = word.trim(); game.hint = hint || ''; game.active = true; game.winner = null; game.winners = [];
  game.usedWords.add(game.word);
  if (wordPool && Array.isArray(wordPool)) game.wordPool = wordPool;
  const indices = [...Array(word.length).keys()];
  const revealCount = Math.max(1, Math.floor(word.length * 0.3));
  game.revealed = indices.sort(() => Math.random() - 0.5).slice(0, revealCount).sort((a,b) => a-b);
  broadcast(key, 'guess:started', { length: game.word.length, hint: game.hint, revealed: game.revealed, letters: game.revealed.map(i => ({ i, c: game.word[i] })), winners: [] });
  res.json({ ok: true });
});

app.post('/api/guess/next', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  if (game.pendingTransition) { clearTimeout(game.pendingTransition); game.pendingTransition = null; }
  goToNextWord(key);
  res.json({ ok: true });
});

app.post('/api/guess/delay', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.transitionDelay = Math.max(0, Math.min(60000, parseInt(req.body.delay) || 5000));
  res.json({ ok: true, delay: game.transitionDelay });
});

app.post('/api/guess/auto', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.autoMode = !!req.body.enabled;
  if (req.body.wordPool && Array.isArray(req.body.wordPool)) game.wordPool = req.body.wordPool;
  res.json({ ok: true });
});

app.post('/api/guess/scale', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.scale = Math.max(0.6, Math.min(2, parseFloat(req.body.scale) || 1));
  broadcast(key, 'guess:scale', { scale: game.scale });
  res.json({ ok: true, scale: game.scale });
});

app.post('/api/guess/clear-stats', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getGuessGame(key).playerStats.clear();
  res.json({ ok: true });
});

app.post('/api/guess/clear-winners', (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  getGuessGame(key).winners = [];
  broadcast(key, 'guess:winners', { winners: [] });
  res.json({ ok: true });
});

app.post('/api/guess/stop', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const game = getGuessGame(key);
  game.active = false; game.autoMode = false; game.usedWords = new Set();
  const top5 = Array.from(game.playerStats.entries()).map(([uid, s]) => ({ userId: uid, name: s.name, avatar: s.avatar, totalWords: s.totalWords })).sort((a,b) => b.totalWords - a.totalWords).slice(0, 5);
  broadcast(key, 'guess:stopped', { word: game.word, top5 });
  res.json({ ok: true });
});

app.get('/api/guess/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const game = getGuessGame(key);
  const allPlayers = Array.from(game.playerStats.entries()).map(([userId, s]) => ({ userId, name: s.name, avatar: s.avatar, totalWords: s.totalWords })).sort((a,b) => b.totalWords - a.totalWords);
  res.json({ word: game.word, hint: game.hint, active: game.active, winner: game.winner, winners: game.winners || [], revealed: game.revealed, letters: game.revealed.map(i => ({ i, c: game.word[i] })), length: game.word.length, allPlayers, scale: game.scale || 1, transitionDelay: game.transitionDelay || 5000 });
});

// ══════════════════════════════════════════════════════════
// ── Quiz (سؤال وجواب) ───────────────────────────────────
// ══════════════════════════════════════════════════════════
const quizzes = {};
function getQuiz(key) {
  if (!quizzes[key]) quizzes[key] = { active: false, question: '', choices: [], correctIndex: -1, answers: new Map(), winner: null, timer: 0 };
  return quizzes[key];
}

app.post('/api/quiz/start', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const q = getQuiz(key);
  q.active = true;
  q.question = req.body.question || '';
  q.choices = req.body.choices || [];
  q.correctIndex = parseInt(req.body.correctIndex) ?? -1;
  q.answers.clear();
  q.winner = null;
  q.timer = parseInt(req.body.timer) || 30;
  const endTime = Date.now() + q.timer * 1000;
  q.endTime = endTime;
  broadcast(key, 'quiz:start', { question: q.question, choices: q.choices, timer: q.timer, endTime, choiceCount: q.choices.length });
  // Auto-stop
  if (q.quizTimer) clearTimeout(q.quizTimer);
  q.quizTimer = setTimeout(() => {
    q.active = false;
    const results = {};
    q.choices.forEach((_, i) => results[i] = 0);
    q.answers.forEach(v => { if (results[v] !== undefined) results[v]++; });
    broadcast(key, 'quiz:end', { correctIndex: q.correctIndex, results, winner: q.winner, totalAnswers: q.answers.size });
  }, q.timer * 1000);
  res.json({ ok: true });
});

app.post('/api/quiz/stop', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const q = getQuiz(key);
  q.active = false;
  if (q.quizTimer) { clearTimeout(q.quizTimer); q.quizTimer = null; }
  const results = {};
  q.choices.forEach((_, i) => results[i] = 0);
  q.answers.forEach(v => { if (results[v] !== undefined) results[v]++; });
  broadcast(key, 'quiz:end', { correctIndex: q.correctIndex, results, winner: q.winner, totalAnswers: q.answers.size });
  res.json({ ok: true });
});

app.get('/api/quiz/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const q = getQuiz(key);
  res.json({ active: q.active, question: q.question, choices: q.choices, timer: q.timer, endTime: q.endTime || 0, totalAnswers: q.answers.size });
});

// ══════════════════════════════════════════════════════════
// ── Poll (التصويت) ───────────────────────────────────────
// ══════════════════════════════════════════════════════════
const polls = {};
function getPoll(key) {
  if (!polls[key]) polls[key] = { active: false, question: '', options: [], votes: new Map(), timer: 0 };
  return polls[key];
}

app.post('/api/poll/start', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const p = getPoll(key);
  p.active = true;
  p.question = req.body.question || '';
  p.options = req.body.options || [];
  p.votes.clear();
  p.timer = parseInt(req.body.timer) || 60;
  const endTime = Date.now() + p.timer * 1000;
  p.endTime = endTime;
  broadcast(key, 'poll:start', { question: p.question, options: p.options, timer: p.timer, endTime });
  // Auto-stop
  if (p.pollTimer) clearTimeout(p.pollTimer);
  p.pollTimer = setTimeout(() => {
    p.active = false;
    const results = {};
    p.options.forEach((_, i) => results[i] = 0);
    p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
    broadcast(key, 'poll:end', { results, totalVotes: p.votes.size });
  }, p.timer * 1000);
  res.json({ ok: true });
});

app.post('/api/poll/stop', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@','').trim();
  if (!key) return res.json({ ok: false });
  const p = getPoll(key);
  p.active = false;
  if (p.pollTimer) { clearTimeout(p.pollTimer); p.pollTimer = null; }
  const results = {};
  p.options.forEach((_, i) => results[i] = 0);
  p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
  broadcast(key, 'poll:end', { results, totalVotes: p.votes.size });
  res.json({ ok: true });
});

app.get('/api/poll/:username', requireGameAccess, (req, res) => {
  const key = req.params.username.toLowerCase().replace('@','').trim();
  const p = getPoll(key);
  const results = {};
  p.options.forEach((_, i) => results[i] = 0);
  p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
  res.json({ active: p.active, question: p.question, options: p.options, results, totalVotes: p.votes.size, timer: p.timer, endTime: p.endTime || 0 });
});

// ── REST API ─────────────────────────────────────────────
app.post('/api/connect', async (req, res) => {
  console.log('[Connect] ━━━ طلب جديد ━━━');
  console.log('[Connect] body:', JSON.stringify(req.body || {}).slice(0, 300));

  // 1) استخراج username (دعم raw body كاحتياطي)
  let body = req.body || {};
  if (!body.username && req.rawBody) {
    try { body = JSON.parse(req.rawBody); } catch(_) {}
  }
  const username = String(body.username || '').trim().replace('@', '').toLowerCase();
  const sessionid = body.sessionid || null;
  const subKey = body.key || '';

  // 2) فحص أساسي فقط: username موجود
  if (!username) {
    console.log('[Connect] ❌ username فاضي');
    return res.json({ ok: false, error: 'اكتب اليوزرنيم' });
  }

  // 3) فحص المفتاح (لو موجود) - بدون منع التغيير، فقط ربط إذا أول مرة
  if (subKey && subscribers[subKey]) {
    const sub = subscribers[subKey];
    if (sub.active && new Date(sub.expiresAt) >= new Date()) {
      if (!sub.tiktokUsername) {
        sub.tiktokUsername = username;
        saveSubscribers(subscribers);
        console.log(`[Connect] ربط ${username} بمفتاح المشترك`);
      } else if (sub.tiktokUsername !== username) {
        console.log(`[Connect] ❌ المشترك مربوط بـ ${sub.tiktokUsername} لا يقدر يتصل بـ ${username}`);
        return res.json({ ok: false, error: `حسابك مربوط بـ @${sub.tiktokUsername} — تواصل مع الدعم لتغييره` });
      }
    }
  }

  // 4) إلغاء أي retry قديم وبدء الاتصال مباشرة
  const existing = rooms[username];
  if (existing && existing.retryTimer) {
    clearTimeout(existing.retryTimer);
    existing.retryTimer = null;
    existing.retryCount = 0;
  }

  console.log(`[Connect] ✅ بدء الاتصال بـ @${username}`);
  connectRoom(username, sessionid);
  res.json({ ok: true, username });
});

app.post('/api/disconnect', requireGameAccess, (req, res) => {
  const key = req.body.username?.toLowerCase().replace('@', '').trim();
  const room = rooms[key]; if (!room) return res.json({ ok: false });
  if (room.retryTimer) clearTimeout(room.retryTimer);
  if (room.tiktok) { try { room.tiktok.disconnect(); } catch(_) {} }
  delete rooms[key];
  io.to(`room:${key}`).emit('room:status', { username: key, status: 'removed' });
  res.json({ ok: true });
});

app.get('/api/rooms', (req, res) => {
  const subKey = req.query.key || '';
  const pw = req.query.pw || '';
  // Owner sees all rooms
  if (pw === OWNER_PASSWORD) {
    return res.json(Object.entries(rooms).map(([username, room]) => ({ username, status: room.status, stats: room.stats, msgCount: room.messages.length })));
  }
  // Subscriber sees only their room
  if (subKey) {
    const sub = subscribers[subKey];
    if (sub && sub.tiktokUsername && rooms[sub.tiktokUsername]) {
      const room = rooms[sub.tiktokUsername];
      return res.json([{ username: sub.tiktokUsername, status: room.status, stats: room.stats, msgCount: room.messages.length }]);
    }
  }
  res.json([]);
});

// ── Socket.IO ────────────────────────────────────────────
// مساعد: استخراج مفتاح/باسورد من كوكي السوكت
function parseSocketCookie(socket) {
  const h = socket.handshake.headers.cookie || '';
  const out = {};
  h.split(';').forEach(c => { const i = c.indexOf('='); if (i > 0) out[c.slice(0, i).trim()] = decodeURIComponent(c.slice(i + 1).trim()); });
  return out;
}

io.on('connection', (socket) => {
  // اقرأ هوية السوكت من الكوكي (مرة واحدة عند الاتصال)
  const ck = parseSocketCookie(socket);
  const subKey = ck.bthlab_key || socket.handshake.auth?.key || '';
  const ownerPw = ck.bthlab_pw || socket.handshake.auth?.pw || '';
  socket._isOwner = ownerPw && ownerPw === OWNER_PASSWORD;
  socket._sub = subKey && subscribers[subKey] && subscribers[subKey].active && new Date(subscribers[subKey].expiresAt) >= new Date() ? subscribers[subKey] : null;
  socket._subKey = socket._sub ? subKey : null;

  // 🟢 تسجيل المشترك المتصل + إعلام لوحة المالك
  if (socket._subKey) {
    if (!onlineSubs.has(socket._subKey)) onlineSubs.set(socket._subKey, new Set());
    onlineSubs.get(socket._subKey).add(socket.id);
    const tabs = onlineSubs.get(socket._subKey).size;
    if (tabs === 1) io.to('owner').emit('subs:online', { key: socket._subKey, online: true, tabs });
    else io.to('owner').emit('subs:online', { key: socket._subKey, online: true, tabs });
  }
  // 🟣 السماح للمالك بالاشتراك في تحديثات الحضور
  if (socket._isOwner) {
    socket.join('owner');
    // ابعث له لقطة فورية بالحالة الحالية
    const snapshot = {};
    onlineSubs.forEach((set, key) => { if (set.size) snapshot[key] = set.size; });
    socket.emit('subs:snapshot', snapshot);
  }

  socket.on('join', ({ username, key: joinKey, pw: joinPw }) => {
    const key = username?.toLowerCase().replace('@', '').trim();
    if (!key) return;
    // ── احتياطي لـ WebView (TikTok LIVE Studio): إذا handshake auth فشل،
    //    اقبل المفتاح من حمولة الـ join نفسها (يأتي من URL params في الأوفرلاي)
    if (!socket._isOwner && joinPw && joinPw === OWNER_PASSWORD) {
      socket._isOwner = true;
    }
    if (!socket._sub && joinKey && subscribers[joinKey]) {
      const candidate = subscribers[joinKey];
      if (candidate.active && new Date(candidate.expiresAt) >= new Date()) {
        socket._sub = candidate;
        socket._subKey = joinKey;
        // سجّله كمتصل (مهم لمؤشر "متصل" في لوحة المالك)
        if (!onlineSubs.has(joinKey)) onlineSubs.set(joinKey, new Set());
        onlineSubs.get(joinKey).add(socket.id);
        io.to('owner').emit('subs:online', { key: joinKey, online: true, tabs: onlineSubs.get(joinKey).size });
      }
    }
    // 🔒 تحقق: المشترك يقدر ينضم فقط لقناة اليوزرنيم المسجّل عنده، المالك يقدر ينضم لأي قناة
    if (!socket._isOwner) {
      if (!socket._sub) {
        console.log(`[Join] ❌ رفض @${key}: لا اشتراك صالح (joinKey: ${joinKey ? joinKey.slice(0,8)+'..' : 'فاضي'}, cookie: ${socket._subKey ? 'موجود' : 'لا'})`);
        return;
      }
      if (socket._sub.tiktokUsername !== key) {
        console.log(`[Join] ❌ رفض @${key}: الاشتراك مربوط بـ @${socket._sub.tiktokUsername}`);
        return;
      }
    }
    console.log(`[Join] ✅ @${key} انضم (${socket._isOwner ? 'مالك' : 'مشترك'})`);
    socket.rooms.forEach(room => { if (room.startsWith('room:') && room !== `room:${key}`) socket.leave(room); });
    socket.join(`room:${key}`);
    const room = rooms[key];
    if (room) {
      socket.emit('stats', room.stats);
      socket.emit('history', room.messages.slice(-30));
      socket.emit('room:status', { username: key, status: room.status });
      const wheel = getWheel(key);
      socket.emit('wheel:update', { entries: Array.from(wheel.entries.values()), count: wheel.entries.size, keyword: wheel.keyword });
    }
  });
  let key = null;
  socket.on('disconnect', () => {
    if (key) socket.leave(`room:${key}`);
    // 🟢 إزالة السوكت من قائمة المشتركين المتصلين
    if (socket._subKey) {
      const set = onlineSubs.get(socket._subKey);
      if (set) {
        set.delete(socket.id);
        const tabs = set.size;
        if (tabs === 0) {
          onlineSubs.delete(socket._subKey);
          io.to('owner').emit('subs:online', { key: socket._subKey, online: false, tabs: 0 });
        } else {
          io.to('owner').emit('subs:online', { key: socket._subKey, online: true, tabs });
        }
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
// ══════════════════════════════════════════════════════════
// ── Subscription Expiry Alerts (check every hour) ───────
// ══════════════════════════════════════════════════════════
const alertedToday = new Set();
setInterval(() => {
  const now = new Date();
  const hour = now.getHours();
  const dateKey = now.toISOString().split('T')[0];
  // Only run at 9 AM Riyadh time (UTC+3 = 6 AM UTC)
  if (hour !== 6) return;

  Object.entries(subscribers).forEach(([key, sub]) => {
    if (!sub.active || !sub.email) return;
    const alertId = `${key}-${dateKey}`;
    if (alertedToday.has(alertId)) return;

    const expires = new Date(sub.expiresAt);
    const daysLeft = Math.ceil((expires - now) / (1000 * 60 * 60 * 24));

    if (daysLeft === 3 || daysLeft === 1 || daysLeft === 0) {
      alertedToday.add(alertId);
      const urgency = daysLeft === 0 ? '⚠️ اشتراكك ينتهي اليوم!' : daysLeft === 1 ? '⏰ باقي يوم واحد على انتهاء اشتراكك' : '📅 باقي 3 أيام على انتهاء اشتراكك';
      sendEmail(sub.email, `[BthLab] ${urgency}`,
        `<div dir="rtl" style="font-family:Tahoma,sans-serif;max-width:500px;margin:0 auto;padding:20px;text-align:center">
          <h2 style="color:#ffd700">${urgency}</h2>
          <p style="font-size:14px;color:#666">مرحباً ${sub.name || ''},</p>
          <p style="font-size:14px">اشتراكك في <strong>BthLab</strong> ينتهي بتاريخ <strong>${expires.toLocaleDateString('ar-SA')}</strong></p>
          <p style="font-size:14px">لتجديد اشتراكك تواصل مع الدعم من صفحة الأدمن</p>
          <div style="margin-top:16px;padding:12px;background:#f5f5f5;border-radius:10px;font-family:monospace">${key}</div>
          <p style="font-size:11px;color:#aaa;margin-top:16px">BthLab — مختبر البث</p>
        </div>`
      );
      if (daysLeft === 1) {
        sendEmail(SUPPORT_EMAIL, `[BthLab] اشتراك ${sub.name || key} ينتهي غداً`,
          `<div dir="rtl" style="font-family:Tahoma,sans-serif;padding:20px">
            <h3>اشتراك ينتهي غداً</h3>
            <p>الاسم: ${sub.name || '—'} | الإيميل: ${sub.email} | المفتاح: ${key} | التيك توك: @${sub.tiktokUsername || '—'}</p>
          </div>`
        );
      }
    }
  });
}, 60 * 60 * 1000); // Check every hour

// ══════════════════════════════════════════════════════════
// ── Security: Rate Limiting for Key Validation ──────────
// ══════════════════════════════════════════════════════════
const loginAttempts = {};
const RATE_LIMIT_MAX = 5; // max attempts per 15 min
const RATE_LIMIT_WINDOW = 15 * 60 * 1000;

function checkRateLimit(ip) {
  const now = Date.now();
  if (!loginAttempts[ip]) loginAttempts[ip] = [];
  loginAttempts[ip] = loginAttempts[ip].filter(t => now - t < RATE_LIMIT_WINDOW);
  if (loginAttempts[ip].length >= RATE_LIMIT_MAX) return false;
  loginAttempts[ip].push(now);
  return true;
}

// Clean up rate limit data every hour
setInterval(() => {
  const now = Date.now();
  Object.keys(loginAttempts).forEach(ip => {
    loginAttempts[ip] = loginAttempts[ip].filter(t => now - t < RATE_LIMIT_WINDOW);
    if (!loginAttempts[ip].length) delete loginAttempts[ip];
  });
}, 60 * 60 * 1000);

// ══════════════════════════════════════════════════════════
// ── LIVE ROOMS (غرف تفاعلية بدون TikTok) ────────────────
// ══════════════════════════════════════════════════════════
const liveRooms = new Map();

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

function createLiveRoom(hostName) {
  let code;
  do { code = generateRoomCode(); } while (liveRooms.has(code));
  const room = {
    code, hostName, hostSocketId: null, createdAt: Date.now(),
    viewers: new Map(), chat: [],
    poll: { active: false, question: '', options: [], votes: new Map(), endTime: 0, timer: null },
    wheel: { entries: new Map(), accepting: false, keyword: 'اشتراك' },
    quiz: { active: false, question: '', choices: [], correctIndex: -1, answers: new Map(), winner: null, endTime: 0, timer: null },
    guess: { active: false, word: '', hint: '', revealed: [], winners: [], wordPool: [], timer: null },
    knockout: { phase: 'idle', keyword: 'بطولة', maxPlayers: 16, players: new Map(), round: 0, currentQuestion: null },
    password: { active: false, word: '', revealed: new Set(), winner: null },
    wordwar: { phase: 'idle', category: '', validWords: [], redKeyword: 'أحمر', blueKeyword: 'أزرق', teams: new Map(), usedWords: new Set(), redScore: 0, blueScore: 0, endTime: 0, timer: null },
  };
  liveRooms.set(code, room);
  return room;
}

function maskPw(pw) { return [...pw.word].map((c, i) => pw.revealed.has(i) ? c : '_'); }
function wwCount(ww, team) { let n = 0; ww.teams.forEach(t => { if (t === team) n++; }); return n; }
function wwEndRound(lr) {
  const ww = lr.wordwar;
  if (ww.timer) { clearTimeout(ww.timer); ww.timer = null; }
  ww.phase = 'ended'; ww.endTime = 0;
  const winner = ww.redScore > ww.blueScore ? 'red' : ww.blueScore > ww.redScore ? 'blue' : 'tie';
  io.to(`liveroom:${lr.code}`).emit('room:wordwar-end', { redScore: ww.redScore, blueScore: ww.blueScore, winner });
}

function getRoomViewerList(room) { return Array.from(room.viewers.values()).map(v => v.name); }

// Auto-cleanup after 3 hours
setInterval(() => { const now = Date.now(); for (const [code, room] of liveRooms) { if (now - room.createdAt > 3 * 60 * 60 * 1000) liveRooms.delete(code); } }, 15 * 60 * 1000);

app.post('/api/live-rooms/create', (req, res) => {
  const hostName = (req.body.hostName || '').trim();
  if (!hostName) return res.json({ ok: false, error: 'اسم المضيف مطلوب' });
  const room = createLiveRoom(hostName);
  console.log(`[LiveRooms] Created ${room.code} by ${hostName}`);
  res.json({ ok: true, code: room.code, hostName });
});

app.get('/api/live-rooms/:code', (req, res) => {
  const room = liveRooms.get(req.params.code.toUpperCase());
  if (!room) return res.json({ ok: false, error: 'الغرفة غير موجودة' });
  res.json({ ok: true, code: room.code, hostName: room.hostName, viewerCount: room.viewers.size, viewers: getRoomViewerList(room) });
});

app.get('/api/live-rooms/:code/wheel', (req, res) => {
  const room = liveRooms.get(req.params.code.toUpperCase());
  if (!room) return res.json({ ok: false });
  res.json({ entries: Array.from(room.wheel.entries.values()), count: room.wheel.entries.size, accepting: room.wheel.accepting, keyword: room.wheel.keyword });
});

// ── Live Rooms Socket Handlers ──
io.on('connection', (socket) => {
  socket.on('room:join', ({ code, name }) => {
    const lroom = liveRooms.get((code || '').toUpperCase());
    if (!lroom) return socket.emit('room:error', { message: 'الغرفة غير موجودة أو انتهت' });
    if (!name || !name.trim()) return socket.emit('room:error', { message: 'الاسم مطلوب' });
    socket.rooms.forEach(r => { if (r.startsWith('liveroom:')) socket.leave(r); });
    const viewerName = name.trim().substring(0, 30);
    lroom.viewers.set(socket.id, { name: viewerName, joinedAt: Date.now() });
    socket.join(`liveroom:${lroom.code}`);
    socket._liveRoomCode = lroom.code;
    socket._liveRoomName = viewerName;
    socket.emit('room:joined', {
      code: lroom.code, hostName: lroom.hostName, viewerCount: lroom.viewers.size,
      chatHistory: lroom.chat.slice(-30),
      poll: lroom.poll.active ? { question: lroom.poll.question, options: lroom.poll.options, endTime: lroom.poll.endTime } : null,
      wheel: { accepting: lroom.wheel.accepting, keyword: lroom.wheel.keyword, count: lroom.wheel.entries.size },
      quiz: lroom.quiz.active ? { question: lroom.quiz.question, choices: lroom.quiz.choices, endTime: lroom.quiz.endTime } : null,
      password: lroom.password.active ? { letters: maskPw(lroom.password) } : null,
      wordwar: lroom.wordwar.phase !== 'idle' ? { phase: lroom.wordwar.phase, category: lroom.wordwar.category, redKeyword: lroom.wordwar.redKeyword, blueKeyword: lroom.wordwar.blueKeyword, redScore: lroom.wordwar.redScore, blueScore: lroom.wordwar.blueScore, redCount: wwCount(lroom.wordwar, 'red'), blueCount: wwCount(lroom.wordwar, 'blue'), endTime: lroom.wordwar.endTime } : null,
    });
    io.to(`liveroom:${lroom.code}`).emit('room:viewers', { count: lroom.viewers.size, viewers: getRoomViewerList(lroom) });
    io.to(`liveroom:${lroom.code}`).emit('room:activity', { type: 'join', name: viewerName });
  });

  socket.on('room:host', ({ code, hostName }) => {
    const lroom = liveRooms.get((code || '').toUpperCase());
    if (!lroom) return socket.emit('room:error', { message: 'الغرفة غير موجودة' });
    lroom.hostSocketId = socket.id;
    socket.join(`liveroom:${lroom.code}`);
    socket._liveRoomCode = lroom.code;
    socket._isHost = true;
    socket.emit('room:host-ok', { code: lroom.code, viewerCount: lroom.viewers.size });
  });

  socket.on('room:chat', ({ code, message }) => {
    const lroom = liveRooms.get((code || '').toUpperCase());
    if (!lroom || !message || !message.trim()) return;
    const name = socket._liveRoomName || (socket._isHost ? lroom.hostName : 'مجهول');
    const msg = { name, message: message.trim().substring(0, 200), ts: Date.now() };
    lroom.chat.push(msg);
    if (lroom.chat.length > 100) lroom.chat.shift();
    io.to(`liveroom:${lroom.code}`).emit('room:chat', msg);
    // Wheel keyword check
    const w = lroom.wheel;
    if (w.accepting && w.keyword && msg.message.includes(w.keyword) && !w.entries.has(socket.id)) {
      w.entries.set(socket.id, { userId: socket.id, name });
      io.to(`liveroom:${lroom.code}`).emit('room:wheel-update', { entries: Array.from(w.entries.values()), count: w.entries.size, newEntry: { name } });
    }
    // Quiz answer
    const q = lroom.quiz;
    if (q.active && !q.answers.has(socket.id)) {
      const num = parseInt(msg.message.trim());
      if (num >= 1 && num <= q.choices.length) {
        q.answers.set(socket.id, num - 1);
        if (num - 1 === q.correctIndex && !q.winner) q.winner = { name };
        io.to(`liveroom:${lroom.code}`).emit('room:quiz-answer', { totalAnswers: q.answers.size });
      }
    }
    // Poll vote
    const p = lroom.poll;
    if (p.active && !p.votes.has(socket.id)) {
      const num = parseInt(msg.message.trim());
      if (num >= 1 && num <= p.options.length) {
        p.votes.set(socket.id, num - 1);
        const results = {}; p.options.forEach((_, i) => results[i] = 0); p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
        io.to(`liveroom:${lroom.code}`).emit('room:poll-vote', { results, totalVotes: p.votes.size });
      }
    }
    // Guess answer
    const g = lroom.guess;
    if (g.active && g.word && (msg.message.trim() === g.word) && !g.winners.find(ww => ww.socketId === socket.id)) {
      g.winners.push({ socketId: socket.id, name, rank: g.winners.length + 1 });
      io.to(`liveroom:${lroom.code}`).emit('room:guess-won', { winner: { name, rank: g.winners.length }, winners: g.winners });
    }
    // Password guess
    const pw = lroom.password;
    if (pw.active && !pw.winner && pw.word && msg.message.trim() === pw.word) {
      pw.winner = { name }; pw.active = false;
      io.to(`liveroom:${lroom.code}`).emit('room:password-won', { winner: { name }, word: pw.word });
    }
    // Word War: team join
    const wwG = lroom.wordwar;
    if (wwG.phase === 'register' && !wwG.teams.has(socket.id)) {
      const t = msg.message.includes(wwG.redKeyword) ? 'red' : msg.message.includes(wwG.blueKeyword) ? 'blue' : null;
      if (t) {
        wwG.teams.set(socket.id, t);
        io.to(`liveroom:${lroom.code}`).emit('room:wordwar-join', { team: t, player: name, redCount: wwCount(wwG, 'red'), blueCount: wwCount(wwG, 'blue') });
      }
    }
    // Word War: word scoring
    if (wwG.phase === 'playing' && wwG.endTime > Date.now()) {
      const team = wwG.teams.get(socket.id);
      if (team) {
        const w = msg.message.trim().toLowerCase().replace(/\s+/g, '');
        if (w.length >= 2 && !wwG.usedWords.has(w) && (wwG.validWords.length === 0 || wwG.validWords.includes(w))) {
          wwG.usedWords.add(w);
          if (team === 'red') wwG.redScore++; else wwG.blueScore++;
          io.to(`liveroom:${lroom.code}`).emit('room:wordwar-word', { team, word: msg.message.trim(), player: name, redScore: wwG.redScore, blueScore: wwG.blueScore });
        }
      }
    }
    // Knockout join
    const ko = lroom.knockout;
    if (ko.phase === 'register' && msg.message.includes(ko.keyword) && !ko.players.has(socket.id) && ko.players.size < ko.maxPlayers) {
      ko.players.set(socket.id, { name, alive: true });
      io.to(`liveroom:${lroom.code}`).emit('room:knockout-joined', { count: ko.players.size, max: ko.maxPlayers });
    }
    // Knockout answer
    if (ko.phase === 'question' && ko.currentQuestion) {
      const pp = ko.players.get(socket.id);
      if (pp && pp.alive && !ko.currentQuestion.answers.has(socket.id)) {
        const num = parseInt(msg.message.trim());
        if (num >= 1 && num <= ko.currentQuestion.options.length) {
          ko.currentQuestion.answers.set(socket.id, num - 1);
          io.to(`liveroom:${lroom.code}`).emit('room:knockout-answered', { count: ko.currentQuestion.answers.size, total: Array.from(ko.players.values()).filter(pp2 => pp2.alive).length });
        }
      }
    }
  });

  // Room host controls
  socket.on('room:poll-start', ({ code, question, options, timer }) => {
    const lroom = liveRooms.get((code || '').toUpperCase());
    if (!lroom || !socket._isHost) return;
    const p = lroom.poll;
    if (p.timer) clearTimeout(p.timer);
    p.active = true; p.question = question || ''; p.options = options || []; p.votes = new Map();
    const t = parseInt(timer) || 60;
    p.endTime = Date.now() + t * 1000;
    io.to(`liveroom:${lroom.code}`).emit('room:poll-start', { question: p.question, options: p.options, timer: t, endTime: p.endTime });
    p.timer = setTimeout(() => { p.active = false; const results = {}; p.options.forEach((_, i) => results[i] = 0); p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; }); io.to(`liveroom:${lroom.code}`).emit('room:poll-end', { results, totalVotes: p.votes.size }); }, t * 1000);
  });
  socket.on('room:poll-stop', ({ code }) => {
    const lroom = liveRooms.get((code || '').toUpperCase()); if (!lroom || !socket._isHost) return;
    const p = lroom.poll; p.active = false; if (p.timer) { clearTimeout(p.timer); p.timer = null; }
    const results = {}; p.options.forEach((_, i) => results[i] = 0); p.votes.forEach(v => { if (results[v] !== undefined) results[v]++; });
    io.to(`liveroom:${lroom.code}`).emit('room:poll-end', { results, totalVotes: p.votes.size });
  });
  socket.on('room:wheel-keyword', ({ code, keyword }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(lr&&socket._isHost) { lr.wheel.keyword=keyword||'اشتراك'; io.to(`liveroom:${lr.code}`).emit('room:wheel-keyword',{keyword:lr.wheel.keyword}); }});
  socket.on('room:wheel-open', ({ code }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(lr&&socket._isHost) { lr.wheel.accepting=true; io.to(`liveroom:${lr.code}`).emit('room:wheel-open',{keyword:lr.wheel.keyword,count:lr.wheel.entries.size}); }});
  socket.on('room:wheel-close', ({ code }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(lr&&socket._isHost) { lr.wheel.accepting=false; io.to(`liveroom:${lr.code}`).emit('room:wheel-close',{}); }});
  socket.on('room:wheel-clear', ({ code }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(lr&&socket._isHost) { lr.wheel.entries.clear(); io.to(`liveroom:${lr.code}`).emit('room:wheel-update',{entries:[],count:0,fullSync:true}); }});
  socket.on('room:wheel-spin', ({ code, duration }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(!lr||!socket._isHost) return; const w=lr.wheel; if(w.entries.size<2) return socket.emit('room:error',{message:'يحتاج مشتركين أكثر'}); const entries=Array.from(w.entries.values()); const wi=Math.floor(Math.random()*entries.length); io.to(`liveroom:${lr.code}`).emit('room:wheel-spin',{winner:entries[wi],winnerIndex:wi,entries,duration:(duration||5)*1000}); });
  socket.on('room:quiz-start', ({ code, question, choices, correctIndex, timer }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(!lr||!socket._isHost) return; const q=lr.quiz; if(q.timer)clearTimeout(q.timer); q.active=true;q.question=question||'';q.choices=choices||[];q.correctIndex=parseInt(correctIndex)??-1;q.answers=new Map();q.winner=null; const t=parseInt(timer)||30;q.endTime=Date.now()+t*1000; io.to(`liveroom:${lr.code}`).emit('room:quiz-start',{question:q.question,choices:q.choices,timer:t,endTime:q.endTime}); q.timer=setTimeout(()=>{q.active=false;const r={};q.choices.forEach((_,i)=>r[i]=0);q.answers.forEach(v=>{if(r[v]!==undefined)r[v]++;});io.to(`liveroom:${lr.code}`).emit('room:quiz-end',{correctIndex:q.correctIndex,results:r,winner:q.winner,totalAnswers:q.answers.size});},t*1000); });
  socket.on('room:quiz-stop', ({ code }) => { const lr = liveRooms.get((code||'').toUpperCase()); if(!lr||!socket._isHost) return; const q=lr.quiz;q.active=false;if(q.timer){clearTimeout(q.timer);q.timer=null;} const r={};q.choices.forEach((_,i)=>r[i]=0);q.answers.forEach(v=>{if(r[v]!==undefined)r[v]++;}); io.to(`liveroom:${lr.code}`).emit('room:quiz-end',{correctIndex:q.correctIndex,results:r,winner:q.winner,totalAnswers:q.answers.size}); });
  socket.on('room:guess-start', ({ code, word, hint }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const g=lr.guess;g.active=true;g.word=word||'';g.hint=hint||'';g.revealed=g.word.length>=2?[0,g.word.length-1]:[];g.winners=[]; io.to(`liveroom:${lr.code}`).emit('room:guess-started',{length:g.word.length,hint:g.hint,letters:g.revealed.map(i=>({i,c:g.word[i]})),winners:[]}); });
  socket.on('room:guess-stop', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return;lr.guess.active=false; io.to(`liveroom:${lr.code}`).emit('room:guess-stopped',{}); });
  socket.on('room:guess-reveal', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; io.to(`liveroom:${lr.code}`).emit('room:guess-reveal',{word:lr.guess.word}); });
  socket.on('room:knockout-register', ({ code, keyword, maxPlayers }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ko=lr.knockout;ko.phase='register';ko.keyword=keyword||'بطولة';ko.maxPlayers=parseInt(maxPlayers)||16;ko.players=new Map();ko.round=0;ko.currentQuestion=null; io.to(`liveroom:${lr.code}`).emit('room:knockout-register',{keyword:ko.keyword,maxPlayers:ko.maxPlayers}); });
  socket.on('room:knockout-lock', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return;lr.knockout.phase='locked'; io.to(`liveroom:${lr.code}`).emit('room:knockout-locked',{count:lr.knockout.players.size,players:Array.from(lr.knockout.players.values())}); });
  socket.on('room:knockout-ask', ({ code, question, options, correct, answerTime }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ko=lr.knockout;ko.phase='question';ko.round++;ko.currentQuestion={question,options,correct:parseInt(correct),answerTime:parseInt(answerTime)||15,answers:new Map()}; const alive=Array.from(ko.players.values()).filter(p=>p.alive).length; io.to(`liveroom:${lr.code}`).emit('room:knockout-question',{question,options:options||[],round:ko.round,alivePlayers:alive,answerTime:ko.currentQuestion.answerTime}); });
  socket.on('room:knockout-reveal', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ko=lr.knockout;const q=ko.currentQuestion;if(!q)return; const elim=[];ko.players.forEach((p,sid)=>{if(!p.alive)return;if(q.answers.get(sid)!==q.correct){p.alive=false;elim.push({name:p.name});}}); const alive=Array.from(ko.players.values()).filter(p=>p.alive); const winner=alive.length<=1?(alive[0]?{name:alive[0].name}:null):null; io.to(`liveroom:${lr.code}`).emit('room:knockout-reveal',{correct:q.correct,eliminated:elim,aliveCount:alive.length,winner}); });
  socket.on('room:knockout-stop', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return;lr.knockout.phase='idle'; io.to(`liveroom:${lr.code}`).emit('room:knockout-stopped',{}); });
  // ── Password (host) ──
  socket.on('room:password-start', ({ code, word }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const w=(word||'').trim(); if(w.length<2)return socket.emit('room:error-soft',{message:'كلمة السر قصيرة'}); const pw=lr.password; pw.word=w; pw.winner=null; pw.active=true; pw.revealed=new Set([0,w.length-1]); io.to(`liveroom:${lr.code}`).emit('room:password-update',{letters:maskPw(pw)}); });
  socket.on('room:password-reveal', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const pw=lr.password; if(!pw.active)return; const hidden=[...pw.word].map((_,i)=>i).filter(i=>!pw.revealed.has(i)); if(!hidden.length)return; pw.revealed.add(hidden[Math.floor(Math.random()*hidden.length)]); io.to(`liveroom:${lr.code}`).emit('room:password-update',{letters:maskPw(pw)}); });
  socket.on('room:password-stop', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const pw=lr.password; pw.active=false; io.to(`liveroom:${lr.code}`).emit('room:password-stopped',{word:pw.word}); });
  // ── Word War (host) ──
  socket.on('room:wordwar-open', ({ code, category, redKeyword, blueKeyword, validWords }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ww=lr.wordwar; if(ww.timer){clearTimeout(ww.timer);ww.timer=null;} ww.phase='register'; ww.category=(category||'').trim(); ww.redKeyword=(redKeyword||'أحمر').trim(); ww.blueKeyword=(blueKeyword||'أزرق').trim(); ww.validWords=(validWords||[]).map(w=>String(w).trim().toLowerCase().replace(/\s+/g,'')).filter(Boolean); io.to(`liveroom:${lr.code}`).emit('room:wordwar-open',{category:ww.category,redKeyword:ww.redKeyword,blueKeyword:ww.blueKeyword,redScore:ww.redScore,blueScore:ww.blueScore,redCount:wwCount(ww,'red'),blueCount:wwCount(ww,'blue')}); });
  socket.on('room:wordwar-start', ({ code, duration }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ww=lr.wordwar; if(ww.teams.size<2)return socket.emit('room:error-soft',{message:'يحتاج لاعبين في الفريقين'}); if(ww.timer){clearTimeout(ww.timer);} ww.phase='playing'; ww.usedWords.clear(); const d=Math.min(Math.max(parseInt(duration)||60,15),300); ww.endTime=Date.now()+d*1000; io.to(`liveroom:${lr.code}`).emit('room:wordwar-start',{category:ww.category,duration:d,endTime:ww.endTime,redScore:ww.redScore,blueScore:ww.blueScore,redCount:wwCount(ww,'red'),blueCount:wwCount(ww,'blue')}); ww.timer=setTimeout(()=>wwEndRound(lr),d*1000); });
  socket.on('room:wordwar-stop', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; if(lr.wordwar.phase==='playing'||lr.wordwar.phase==='register') wwEndRound(lr); });
  socket.on('room:wordwar-reset', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; const ww=lr.wordwar; if(ww.timer){clearTimeout(ww.timer);ww.timer=null;} ww.phase='idle'; ww.redScore=0; ww.blueScore=0; ww.teams.clear(); ww.usedWords.clear(); ww.endTime=0; io.to(`liveroom:${lr.code}`).emit('room:wordwar-reset',{}); });
  socket.on('room:close', ({ code }) => { const lr=liveRooms.get((code||'').toUpperCase());if(!lr||!socket._isHost)return; io.to(`liveroom:${lr.code}`).emit('room:closed',{message:'أغلق المضيف الغرفة'});liveRooms.delete(lr.code); });

  // Cleanup on disconnect
  const origDisconnect = socket._events?.disconnect;
  socket.on('disconnect', () => {
    const code = socket._liveRoomCode;
    if (code) {
      const lroom = liveRooms.get(code);
      if (lroom) {
        lroom.viewers.delete(socket.id);
        io.to(`liveroom:${code}`).emit('room:viewers', { count: lroom.viewers.size, viewers: getRoomViewerList(lroom) });
      }
    }
  });
});

httpServer.listen(PORT, () => console.log(`\n🎯 BthLab running at http://localhost:${PORT}\n`));

setInterval(() => {
  Object.keys(rooms).forEach(key => {
    const room = rooms[key];
    if (room.status === 'disconnected' || room.status === 'error') connectRoom(key);
  });
}, 30000);
