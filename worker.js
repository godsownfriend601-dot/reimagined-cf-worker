export class SessionBroker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // peerId -> { ws, role: 'runner'|'client' }
    this.peers = new Map();
    // pending http proxy requests: reqId -> { resolve, reject, timeout }
    this.pendingRequests = new Map();
    this.nextPeerId = 1;
  }

  // Called when a request is routed to this Durable Object
  // We support both WebSocket upgrade and HTTP proxying via fetch
  async fetch(request) {
    const url = new URL(request.url);

    // WebSocket upgrade (signaling / control channel)
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocket(request);
    }

    // Otherwise this is an HTTP proxy endpoint from client: POST /proxy/:sessionId
    // We expect token to be provided in Authorization header: "Bearer <token>"
    if (url.pathname === '/proxy' && request.method === 'POST') {
      return this.handleProxyRequest(request);
    }

    return new Response('Not found', { status: 404 });
  }

  async handleWebSocket(request) {
    const url = new URL(request.url);
    const token = url.searchParams.get('token') || this._getBearerToken(request.headers);
    const role = url.searchParams.get('role') || 'client'; // default client
    const sessionId = url.searchParams.get('sessionId') || this.state.id.toString();

    if (!token) return new Response('Missing token', { status: 401 });

    // Validate token against sessions KV
    const valid = await this._validateToken(token, sessionId);
    if (!valid) return new Response('Invalid token', { status: 403 });

    const pair = new WebSocketPair();
    const [clientSide, objectSide] = Object.values(pair);
    objectSide.accept();

    const peerId = this.nextPeerId++;
    this.peers.set(peerId, { ws: objectSide, role });

    console.log(`SessionBroker ${this.state.id}: peer ${peerId} connected as ${role}`);

    objectSide.addEventListener('message', (evt) => {
      // messages expected to be JSON strings or objects
      this._onPeerMessage(peerId, evt.data).catch(err => {
        console.error('Error handling peer message', err);
      });
    });

    objectSide.addEventListener('close', () => {
      console.log(`peer ${peerId} disconnected`);
      this.peers.delete(peerId);
      // cleanup pending requests if no runner left
      if (![...this.peers.values()].some(p => p.role === 'runner')) {
        // optionally reject pending requests
        for (const [reqId, p] of this.pendingRequests.entries()) {
          p.reject(new Error('No runner available'));
          clearTimeout(p.timeout);
          this.pendingRequests.delete(reqId);
        }
      }
    });

    return new Response(null, { status: 101, webSocket: clientSide });
  }

  async _onPeerMessage(fromPeerId, raw) {
    let msg;
    try {
      msg = (typeof raw === 'string') ? JSON.parse(raw) : raw;
    } catch (e) {
      console.error('Invalid JSON message:', raw);
      return;
    }

    const fromPeer = this.peers.get(fromPeerId);
    if (!fromPeer) return;

    // Handle http-proxy-response from runner
    if (msg.type === 'http-proxy-response' && fromPeer.role === 'runner') {
      const { reqId, status, headers = {}, bodyB64 } = msg;
      const pending = this.pendingRequests.get(reqId);
      if (pending) {
        // Construct Response-like object to resolve client
        const body = bodyB64 ? this._base64ToArrayBuffer(bodyB64) : new ArrayBuffer(0);
        pending.resolve({ status, headers, body });
        clearTimeout(pending.timeout);
        this.pendingRequests.delete(reqId);
      }
      return;
    }

    // Forwards (SDP/ICE/input-event/offer/answer) — broadcast to all other peers except sender
    if (msg.type && ['offer','answer','ice-candidate','input-event'].includes(msg.type)) {
      for (const [peerId, p] of this.peers.entries()) {
        if (peerId === fromPeerId) continue;
        try {
          p.ws.send(JSON.stringify(msg));
        } catch (e) {
          console.error('Failed to forward message to peer', peerId, e);
        }
      }
      return;
    }

    // Other message types can be handled here
    console.warn('Unknown message type from peer:', msg.type);
  }

  async handleProxyRequest(request) {
    // Validate token
    const token = this._getBearerToken(request.headers);
    if (!token) return new Response('Missing token', { status: 401 });

    // sessionId is the durable object id name (we used idFromName when routing)
    const sessionId = this.state.id.toString();
    const valid = await this._validateToken(token, sessionId);
    if (!valid) return new Response('Invalid token', { status: 403 });

    // Check runner connection
    const runnerPeer = [...this.peers.values()].find(p => p.role === 'runner');
    if (!runnerPeer) {
      return new Response('No runner connected', { status: 502 });
    }

    // Read request body as Uint8Array then base64 encode
    let bodyArray = new Uint8Array(0);
    try {
      const buf = await request.arrayBuffer();
      bodyArray = new Uint8Array(buf);
    } catch (e) {
      // ignore
    }

    // Client must provide the actual target URL in JSON body: { targetUrl, method, headers }
    // For convenience allow forwarding raw HTTP by providing targetUrl + method + headers + bodyB64
    // If client POSTed JSON we can parse it; otherwise assume the raw body is the request body to send
    let parsed = null;
    try {
      const text = new TextDecoder().decode(bodyArray);
      parsed = JSON.parse(text);
    } catch (e) {
      // not JSON - treat as raw body and require 'x-target-url' header
    }

    let proxyReq = {};
    if (parsed && parsed.targetUrl) {
      proxyReq = {
        targetUrl: parsed.targetUrl,
        method: parsed.method || 'GET',
        headers: parsed.headers || {},
        bodyB64: parsed.bodyB64 || (bodyArray.length ? this._arrayBufferToBase64(bodyArray.buffer) : undefined),
      };
    } else {
      // fallback: use x-target-url header
      const targetUrl = request.headers.get('x-target-url');
      if (!targetUrl) return new Response('Missing targetUrl', { status: 400 });
      proxyReq = {
        targetUrl,
        method: request.method,
        headers: Object.fromEntries(request.headers.entries()),
        bodyB64: bodyArray.length ? this._arrayBufferToBase64(bodyArray.buffer) : undefined,
      };
    }

    // Add a request ID and forward to runner
    const reqId = this._randomId(12);
    const message = { type: 'http-proxy-request', reqId, ...proxyReq };

    // Send to the runner
    try {
      runnerPeer.ws.send(JSON.stringify(message));
    } catch (e) {
      console.error('Failed to send proxy request to runner', e);
      return new Response('Runner send failed', { status: 502 });
    }

    // Wait for response (with timeout)
    const response = await new Promise((resolve, reject) => {
      const timeoutMs = 30_000; // 30s
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(reqId);
        reject(new Error('proxy timeout'));
      }, timeoutMs);
      this.pendingRequests.set(reqId, { resolve, reject, timeout });
    }).catch(err => ({ error: err.message }));

    if (response && response.error) {
      return new Response(response.error, { status: 504 });
    }

    // Build response to the original client
    const headers = new Headers(response.headers || {});
    // Convert ArrayBuffer body to Response body
    const body = response.body && response.body.byteLength ? response.body : undefined;
    return new Response(body, { status: response.status || 200, headers });
  }

  /* ---------------- helpers ---------------- */

  _getBearerToken(headers) {
    const auth = headers.get ? headers.get('Authorization') : null;
    if (!auth) return null;
    const m = auth.match(/Bearer (.+)/i);
    return m ? m[1] : null;
  }

  async _validateToken(token, sessionId) {
    // token format: `${sessionId}:${expires}:${sigHex}`
    // We'll verify HMAC with SESSION_SECRET and also check expiry in KV `SESSIONS`
    try {
      const parts = token.split(':');
      const sigHex = parts.slice(-1)[0];
      const payload = parts.slice(0, -1).join(':'); // sessionId:expires
      const [tokSessionId, expiresStr] = payload.split(':');
      if (!tokSessionId || !expiresStr) return false;
      if (tokSessionId !== sessionId) return false;
      const expires = Number(expiresStr);
      if (Date.now() > expires) {
        // cleanup
        try { await this.env.SESSIONS.delete(sessionId); } catch {}
        return false;
      }
      // compute HMAC and compare
      const secret = this.env.SESSION_SECRET;
      const enc = new TextEncoder();
      const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
      const sigBuf = this._hexToArrayBuffer(sigHex);
      const ok = await crypto.subtle.verify('HMAC', key, sigBuf, enc.encode(payload));
      return ok;
    } catch (e) {
      console.error('token validation error', e);
      return false;
    }
  }

  _randomId(len = 12) {
    const arr = new Uint8Array(len);
    crypto.getRandomValues(arr);
    return Array.from(arr).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  _arrayBufferToBase64(buf) {
    let binary = '';
    const bytes = new Uint8Array(buf);
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
      binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
  }
  _base64ToArrayBuffer(b64) {
    const binary = atob(b64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes.buffer;
  }
  _hexToArrayBuffer(hex) {
    const len = hex.length / 2;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
    }
    return bytes.buffer;
  }
}
