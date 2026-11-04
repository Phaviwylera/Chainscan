// Web Worker: reads a local file of any size and keeps only the addresses.
// 0x addresses become 20 raw bytes each (about half the size of the text, and none of the other
// text on the line is ever sent). Bitcoin addresses are sent as plain lines.
//
// in : {cmd:"start", file, firstCol, have:{evm:[seq..], btc:[seq..]}}   have = parts already on the server
//      {cmd:"ack"}                                                         one uploaded part finished
// out: {type:"part", kind, seq, buf, skip}  {type:"progress", ...}  {type:"done", counts, stats}  {type:"error"}
"use strict";
const CH = 16 * 1024 * 1024;
const EVM_PART = 20 * 409600;           // 8 MB of packed addresses per part
const BTC_PART = 4 * 1024 * 1024;
const MAX_OUTSTANDING = 8;

const HEX = new Int8Array(256).fill(-1);
for (let i = 0; i < 10; i++) HEX[48 + i] = i;
for (let i = 0; i < 6; i++) { HEX[97 + i] = 10 + i; HEX[65 + i] = 10 + i; }
const ALNUM = new Uint8Array(256);
for (let c = 48; c < 58; c++) ALNUM[c] = 1;
for (let c = 65; c < 91; c++) ALNUM[c] = 1;
for (let c = 97; c < 123; c++) ALNUM[c] = 1;
const B58 = new Uint8Array(256);
for (const ch of "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz") B58[ch.charCodeAt(0)] = 1;
const B32 = new Uint8Array(256);
for (const ch of "023456789acdefghjklmnpqrstuvwxyz") B32[ch.charCodeAt(0)] = 1;

let outstanding = 0, wake = null;
self.onmessage = (e) => {
  const m = e.data;
  if (m.cmd === "ack") { outstanding--; if (wake && outstanding < MAX_OUTSTANDING) { const w = wake; wake = null; w(); } }
  else if (m.cmd === "start") run(m).catch(err => self.postMessage({ type: "error", message: String(err && err.message || err) }));
};

async function run({ file, firstCol, have }) {
  const haveSet = { evm: new Set((have && have.evm) || []), btc: new Set((have && have.btc) || []) };
  const st = { lines: 0, evm: 0, btc: 0, invalid_hex: 0, lines_without_address: 0 };
  let evmBuf = new Uint8Array(EVM_PART), evmLen = 0, evmSeq = 0;
  let btcBuf = new Uint8Array(BTC_PART + 128), btcLen = 0, btcSeq = 0;
  let sentBytes = 0;

  async function emit(kind, buf, len) {
    const seq = kind === "evm" ? evmSeq++ : btcSeq++;
    const skip = haveSet[kind].has(seq);
    if (skip) { self.postMessage({ type: "part", kind, seq, skip: true, bytes: len }); return; }
    const out = buf.slice(0, len).buffer;
    sentBytes += len;
    outstanding++;
    self.postMessage({ type: "part", kind, seq, buf: out, bytes: len }, [out]);
    if (outstanding >= MAX_OUTSTANDING) await new Promise(r => { wake = r; });
  }

  // classify one token b[s..e); returns 1 if it was an address
  async function token(b, s, e) {
    const n = e - s;
    if (n >= 3 && b[s] === 48 && (b[s + 1] === 120 || b[s + 1] === 88)) {
      for (let i = s + 2; i < e; i++) if (HEX[b[i]] < 0) return 0;
      if (n !== 42) { st.invalid_hex++; return 0; }
      for (let i = s + 2, o = evmLen; i < e; i += 2, o++) evmBuf[o] = (HEX[b[i]] << 4) | HEX[b[i + 1]];
      evmLen += 20; st.evm++;
      if (evmLen >= EVM_PART) { await emit("evm", evmBuf, evmLen); evmBuf = new Uint8Array(EVM_PART); evmLen = 0; }
      return 1;
    }
    let ok = false;
    if (n >= 14 && n <= 74 && b[s] === 98 && b[s + 1] === 99 && b[s + 2] === 49) {      // bc1…
      ok = true; for (let i = s + 3; i < e; i++) if (!B32[b[i]]) { ok = false; break; }
    } else if (n >= 26 && n <= 35 && (b[s] === 49 || b[s] === 51)) {                    // 1… / 3…
      ok = true; for (let i = s + 1; i < e; i++) if (!B58[b[i]]) { ok = false; break; }
    }
    if (!ok) return 0;
    btcBuf.set(b.subarray(s, e), btcLen); btcLen += n; btcBuf[btcLen++] = 10; st.btc++;
    if (btcLen >= BTC_PART) { await emit("btc", btcBuf, btcLen); btcBuf = new Uint8Array(BTC_PART + 128); btcLen = 0; }
    return 1;
  }

  async function processBuf(b, end) {
    if (firstCol) {
      let i = 0;
      while (i < end) {
        let j = i;
        if (b[j] === 0xef && b[j + 1] === 0xbb && b[j + 2] === 0xbf) j += 3;
        while (j < end && (b[j] === 32 || b[j] === 9 || b[j] === 34 || b[j] === 39)) j++;
        let k = j;
        while (k < end && ALNUM[b[k]]) k++;
        let nl = k;
        while (nl < end && b[nl] !== 10) nl++;
        if (nl > i || nl < end) {
          st.lines++;
          if (!(k > j && await token(b, j, k))) {
            // a malformed 0x first value is counted in invalid_hex, not as "no address"
            const isHex = k - j >= 3 && b[j] === 48 && (b[j + 1] === 120 || b[j + 1] === 88);
            if (!isHex) st.lines_without_address++;
          }
        }
        i = nl + 1;
      }
    } else {
      let i = 0;
      while (i < end) {
        while (i < end && !ALNUM[b[i]]) { if (b[i] === 10) st.lines++; i++; }
        let k = i;
        while (k < end && ALNUM[b[k]]) k++;
        if (k > i) await token(b, i, k);
        i = k;
      }
    }
  }

  const size = file.size;
  let pos = 0, carry = new Uint8Array(0), lastProg = 0;
  const t0 = performance.now();
  while (pos < size) {
    const data = new Uint8Array(await file.slice(pos, Math.min(size, pos + CH)).arrayBuffer());
    pos += data.length;
    let b = data;
    if (carry.length) { b = new Uint8Array(carry.length + data.length); b.set(carry); b.set(data, carry.length); }
    let end = b.length;
    if (pos < size) {
      let cut = end - 1;
      if (firstCol) { while (cut >= 0 && b[cut] !== 10) cut--; }
      else { const lim = Math.max(0, end - 4096); while (cut > lim && ALNUM[b[cut]]) cut--; }
      if (cut < 0) { carry = b; continue; }      // a line longer than one slice: keep reading
      end = cut + 1;
      carry = b.slice(end);
    } else carry = new Uint8Array(0);
    await processBuf(b, end);
    const now = performance.now();
    if (now - lastProg > 150 || pos >= size) {
      lastProg = now;
      self.postMessage({ type: "progress", bytesRead: pos, total: size, sentBytes, ...st, seconds: (now - t0) / 1000 });
    }
  }
  if (evmLen) await emit("evm", evmBuf, evmLen);
  if (btcLen) await emit("btc", btcBuf, btcLen);
  self.postMessage({ type: "progress", bytesRead: size, total: size, sentBytes, ...st, seconds: (performance.now() - t0) / 1000 });
  self.postMessage({ type: "done", counts: { evm: evmSeq, btc: btcSeq }, stats: { ...st } });
}
