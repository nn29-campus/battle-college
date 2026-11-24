#!/usr/bin/env node
/* 异能者 · 联机服务器
 * 零依赖：内置 HTTP 静态服务 + 手写 WebSocket 帧处理
 * 用法：node server.js [端口]
 * 同一个端口既提供网页，也提供对战通信。
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.argv[2] || process.env.PORT || '8080', 10);
const ROOT = path.resolve(process.argv[3] || __dirname);

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.json': 'application/json',
  '.m4a': 'audio/mp4', '.ico': 'image/x-icon'
};

/* ---------------- HTTP ---------------- */
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/' || p === '') p = '/index.html';
  const file = path.join(ROOT, path.normalize(p).replace(/^(\.\.[/\\])+/, ''));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end('forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': 'no-cache'
    });
    res.end(data);
  });
});

/* ---------------- WebSocket（手写帧） ---------------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map();          // code -> {host, guest}
const clients = new Set();

function send(conn, text) {
  if (!conn.alive) return;
  const payload = Buffer.from(text, 'utf8');
  let head;
  if (payload.length < 126) {
    head = Buffer.from([0x81, payload.length]);
  } else if (payload.length < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x81; head[1] = 126; head.writeUInt16BE(payload.length, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x81; head[1] = 127;
    head.writeBigUInt64BE(BigInt(payload.length), 2);
  }
  try { conn.socket.write(Buffer.concat([head, payload])); } catch (e) { conn.alive = false; }
}

function closeConn(conn) {
  conn.alive = false;
  clients.delete(conn);
  if (conn.code && rooms.has(conn.code)) {
    const r = rooms.get(conn.code);
    const other = r.host === conn ? r.guest : r.host;
    if (other && other.alive) { send(other, JSON.stringify({ t: 'peerLeft' })); other.code = null; }
    rooms.delete(conn.code);
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  const conn = { socket, alive: true, code: null };
  clients.add(conn);
  let buf = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const maskLen = masked ? 4 : 0;
      if (buf.length < off + maskLen + len) return;
      const mask = masked ? buf.slice(off, off + 4) : null;
      let data = buf.slice(off + maskLen, off + maskLen + len);
      if (masked) {
        const out = Buffer.alloc(len);
        for (let i = 0; i < len; i++) out[i] = data[i] ^ mask[i % 4];
        data = out;
      }
      buf = buf.slice(off + maskLen + len);

      if (opcode === 0x8) { try { socket.end(); } catch (e) {} return closeConn(conn); }
      if (opcode === 0x9) { try { socket.write(Buffer.from([0x8a, 0])); } catch (e) {} continue; }
      if (opcode === 0x1) {
        let msg;
        try { msg = JSON.parse(data.toString('utf8')); } catch (e) { continue; }
        handle(conn, msg);
      }
    }
  });
  socket.on('error', () => closeConn(conn));
  socket.on('close', () => closeConn(conn));
});

function newCode() {
  let c;
  do { c = String(Math.floor(1000 + Math.random() * 9000)); } while (rooms.has(c));
  return c;
}

function handle(conn, msg) {
  if (msg.t === 'create') {
    const code = newCode();
    rooms.set(code, { host: conn, guest: null });
    conn.code = code;
    send(conn, JSON.stringify({ t: 'created', code }));
  } else if (msg.t === 'join') {
    const r = rooms.get(String(msg.code));
    if (!r) return send(conn, JSON.stringify({ t: 'err', msg: '房间不存在' }));
    if (r.guest) return send(conn, JSON.stringify({ t: 'err', msg: '房间已满' }));
    r.guest = conn; conn.code = String(msg.code);
    send(conn, JSON.stringify({ t: 'joined', code: String(msg.code) }));
    send(r.host, JSON.stringify({ t: 'peerJoin' }));
  } else if (msg.t === 'msg') {
    if (!conn.code) return;
    const r = rooms.get(conn.code);
    if (!r) return;
    const other = r.host === conn ? r.guest : r.host;
    if (other && other.alive) send(other, JSON.stringify({ t: 'msg', d: msg.d }));
  } else if (msg.t === 'leave') {
    closeConn(conn);
  }
}

server.listen(PORT, '0.0.0.0', () => {
  console.log('[异能者] 联机服务器已启动');
  console.log('[异能者] 网页:  http://<服务器IP>:' + PORT + '/');
  console.log('[异能者] 房间:  WebSocket 同端口');
});
