// Garbage Day game server.
// Serves the game page and relays each player's live state to everyone else in the same room.
// No packages needed: plain Node.js 18 or newer. Start it with:  node server.js
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
// the game page can sit right next to this file (easiest to upload) or in a "public" folder
const PAGE_FILE = [path.join(__dirname, 'index.html'), path.join(__dirname, 'public', 'index.html')].find(f => fs.existsSync(f));
if (!PAGE_FILE) { console.error('index.html is missing. Upload it next to server.js.'); process.exit(1); }
const PAGE = fs.readFileSync(PAGE_FILE);
const MAX_ROOMS = 2000;          // rooms alive at once
const MAX_PER_ROOM = 40;         // people in one room
const MAX_STATE = 8192;          // bytes of state one player may hold
const MAX_FRAME = 32 * 1024;     // biggest message accepted

/* ---------------- web pages ---------------- */
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  // the lobby lives at "/", private games at "/r/<code>"; both get the same page
  if (url.pathname === '/' || url.pathname === '/index.html' || /^\/r\/[a-z0-9-]{1,24}\/?$/i.test(url.pathname)) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
    res.end(PAGE);
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('Not found');
});

/* ---------------- rooms ---------------- */
// rooms: code -> Map(playerId -> { sock, state })
const rooms = new Map();

function roomCode(v) { return (String(v || 'lobby').toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, 24)) || 'lobby'; }
function playerId(v) { return String(v || '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 32) || ('p' + crypto.randomBytes(5).toString('hex')); }

function broadcast(code, fromId, msg) {
  const members = rooms.get(code); if (!members) return;
  const data = JSON.stringify(msg);
  for (const [id, m] of members) if (id !== fromId) m.sock.sendText(data);
}

function join(sock, code, id) {
  if (!rooms.has(code)) rooms.set(code, new Map());
  const members = rooms.get(code);
  // the same player reconnecting (phone lost signal for a moment) takes over their old spot
  const old = members.get(id);
  const me = { sock, state: old ? old.state : {} };
  members.set(id, me);
  if (old) old.sock.destroy();
  const peers = [];
  for (const [pid, m] of members) if (pid !== id) peers.push({ peer: pid, presence: m.state });
  sock.sendText(JSON.stringify({ t: 'hello', me: id, peers }));

  // simple flood guard: about 40 messages a second, bursts up to 80
  let budget = 80, last = Date.now();
  sock.onText = text => {
    const now = Date.now();
    budget = Math.min(80, budget + (now - last) * 0.04); last = now;
    if (budget < 1) return;
    budget--;
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || m.t !== 'presence' || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
    const next = m.full ? {} : Object.assign({}, me.state);
    for (const k of Object.keys(m.patch).slice(0, 40)) {
      if (!/^[A-Za-z_][A-Za-z0-9_]{0,31}$/.test(k)) continue;
      if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k];
    }
    if (JSON.stringify(next).length > MAX_STATE) return;
    me.state = next;
    broadcast(code, id, { t: 'peer', peer: id, presence: next });
  };
  sock.onClose = () => {
    if (members.get(id) !== me) return; // replaced by a reconnect
    members.delete(id);
    broadcast(code, id, { t: 'left', peer: id });
    if (!members.size) rooms.delete(code);
  };
}

/* ---------------- WebSocket (RFC 6455), just what the game needs ---------------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://localhost');
  const key = req.headers['sec-websocket-key'];
  if (url.pathname !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const code = roomCode(url.searchParams.get('room'));
  const id = playerId(url.searchParams.get('id'));
  const members = rooms.get(code);
  if ((!members && rooms.size >= MAX_ROOMS) || (members && members.size >= MAX_PER_ROOM && !members.has(id))) {
    socket.end('HTTP/1.1 503 Service Unavailable\r\n\r\n'); return;
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  join(wrap(socket), code, id);
});

function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | opcode;
  return Buffer.concat([head, payload]);
}

function wrap(socket) {
  let buf = Buffer.alloc(0), parts = [], closed = false, alive = true;
  const sock = {
    onText: null, onClose: null,
    sendText(text) { if (!closed && socket.writable) socket.write(frame(0x1, Buffer.from(text))); },
    destroy() { if (closed) return; closed = true; try { socket.destroy(); } catch (e) {} finish(); }
  };
  const finish = () => { clearInterval(beat); if (sock.onClose) { const f = sock.onClose; sock.onClose = null; f(); } };
  // ping every 25s; a phone that never answers gets dropped
  const beat = setInterval(() => {
    if (!alive) { sock.destroy(); return; }
    alive = false;
    if (socket.writable) socket.write(frame(0x9, Buffer.alloc(0)));
  }, 25000);

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = (buf[0] & 0x80) !== 0, op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; const big = buf.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME)) { sock.destroy(); return; } len = Number(big); off = 10; }
      if (len > MAX_FRAME || !masked) { sock.destroy(); return; } // browsers always mask
      if (buf.length < off + 4 + len) return;
      const mask = buf.subarray(off, off + 4), data = Buffer.from(buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      buf = buf.subarray(off + 4 + len);
      alive = true;
      if (op === 0x8) { if (socket.writable) socket.end(frame(0x8, Buffer.alloc(0))); sock.destroy(); return; }
      if (op === 0x9) { if (socket.writable) socket.write(frame(0xA, data)); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x0) {
        parts.push(data);
        if (parts.reduce((s, p) => s + p.length, 0) > MAX_FRAME) { sock.destroy(); return; }
        if (fin) { const text = Buffer.concat(parts).toString('utf8'); parts = []; if (sock.onText) sock.onText(text); }
      }
      // binary messages are ignored
    }
  });
  socket.on('close', () => { if (!closed) { closed = true; finish(); } });
  socket.on('error', () => { sock.destroy(); });
  return sock;
}

server.listen(PORT, () => console.log('Garbage Day running on port ' + PORT));
