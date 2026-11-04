// Live 3D view of a scan. Each chain is a grid of its buckets (256 for EVM chains, 64 for Bitcoin):
// grey = waiting, pulsing = being scanned, chain colour = done; height grows with funded wallets.
// Sparks rise when a bucket reports funded wallets. Particles stream in while data is uploading.
// Drag to rotate, scroll or pinch to zoom, hover a column for its details.
(function () {
  "use strict";
  const css = (v, fb) => (getComputedStyle(document.documentElement).getPropertyValue(v) || fb).trim() || fb;

  function Scan3D(container, tooltip) {
    if (!window.THREE) { container.innerHTML = '<div class="empty">3D view unavailable (could not load three.js).</div>'; return; }
    const T = window.THREE;
    const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
    const renderer = new T.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(2, devicePixelRatio || 1));
    container.appendChild(renderer.domElement);
    const scene = new T.Scene();
    const camera = new T.PerspectiveCamera(42, 1, 0.1, 500);
    scene.add(new T.AmbientLight(0xffffff, 0.55));
    const sun = new T.DirectionalLight(0xffffff, 0.75); sun.position.set(8, 20, 12); scene.add(sun);

    const chains = new Map();     // key -> {mesh, n, side, origin, color, label, state:[], funded:[], heights:[]}
    const box = new T.BoxGeometry(0.82, 1, 0.82); box.translate(0, 0.5, 0);
    const dummy = new T.Object3D();
    const tmpColor = new T.Color();
    let theta = 0, phi = 0.92, radius = 34, baseTheta = 0, target = new T.Vector3(0, 0, 0);
    let dragging = false, lastX = 0, lastY = 0, idleSince = performance.now(), streamRate = 0;

    // ---------- sparks ----------
    const MAXP = 1500;
    const pGeo = new T.BufferGeometry();
    const pPos = new Float32Array(MAXP * 3), pCol = new Float32Array(MAXP * 3);
    const pVel = new Float32Array(MAXP * 3), pLife = new Float32Array(MAXP);
    pGeo.setAttribute("position", new T.BufferAttribute(pPos, 3));
    pGeo.setAttribute("color", new T.BufferAttribute(pCol, 3));
    const points = new T.Points(pGeo, new T.PointsMaterial({ size: 0.28, vertexColors: true, transparent: true, opacity: 0.9, depthWrite: false }));
    scene.add(points);
    let pNext = 0;
    function spark(x, y, z, color, n, stream) {
      for (let k = 0; k < n; k++) {
        const i = pNext; pNext = (pNext + 1) % MAXP;
        pPos[i * 3] = x + (Math.random() - .5) * .6; pPos[i * 3 + 1] = y; pPos[i * 3 + 2] = z + (Math.random() - .5) * .6;
        if (stream) { pVel[i * 3] = 0.25 + Math.random() * .1; pVel[i * 3 + 1] = (Math.random() - .5) * .02; pVel[i * 3 + 2] = (Math.random() - .5) * .05; }
        else { pVel[i * 3] = (Math.random() - .5) * .06; pVel[i * 3 + 1] = .08 + Math.random() * .12; pVel[i * 3 + 2] = (Math.random() - .5) * .06; }
        pLife[i] = stream ? 1.6 : 1.0;
        pCol[i * 3] = color.r; pCol[i * 3 + 1] = color.g; pCol[i * 3 + 2] = color.b;
      }
    }

    function label(text, color) {
      const c = document.createElement("canvas"); c.width = 768; c.height = 128;
      const g = c.getContext("2d");
      g.font = "600 64px 'IBM Plex Sans', system-ui, sans-serif"; g.fillStyle = color; g.textAlign = "center"; g.textBaseline = "middle";
      g.fillText(text, 256, 48);
      const s = new T.Sprite(new T.SpriteMaterial({ map: new T.CanvasTexture(c), transparent: true, depthWrite: false }));
      s.scale.set(12, 2, 1);
      return s;
    }

    function layout() {
      const list = [...chains.values()];
      let x = 0;
      const widths = list.map(c => c.side + 3);
      const total = widths.reduce((a, b) => a + b, 0) - 3;
      list.forEach((c, i) => {
        c.origin = new T.Vector3(-total / 2 + x, 0, -c.side / 2);
        c.mesh.position.copy(c.origin);
        c.label.position.set(c.origin.x + c.side / 2 - .5, 0.2, c.origin.z + c.side + 1.8);
        x += widths[i];
      });
      radius = Math.max(18, total * 0.62 + 6);
      target.set(0, 0, 1.5);
    }

    function ensure(key, name, n, colorStr) {
      if (chains.has(key)) return chains.get(key);
      const side = Math.ceil(Math.sqrt(n));
      const mat = new T.MeshStandardMaterial({ roughness: .55, metalness: .1 });
      const mesh = new T.InstancedMesh(box, mat, n);
      mesh.instanceMatrix.setUsage(T.DynamicDrawUsage);
      const c = { key, name, n, side, mesh, color: new T.Color(colorStr), label: label(name, colorStr),
                  state: new Array(n).fill("."), funded: new Array(n).fill(0), heights: new Array(n).fill(0.12) };
      for (let i = 0; i < n; i++) { mesh.setColorAt(i, tmpColor.set(css("--track", "#e6e8f0"))); }
      scene.add(mesh); scene.add(c.label);
      chains.set(key, c); layout();
      return c;
    }

    // chains: [{key, name, color, grid:"..012..", grid_funded:[..]}]
    this.update = function (list, opts) {
      streamRate = (opts && opts.streamRate) || 0;
      const keep = new Set(list.map(c => c.key));
      for (const [k, c] of chains) if (!keep.has(k)) { scene.remove(c.mesh); scene.remove(c.label); chains.delete(k); layout(); }
      for (const ch of list) {
        const n = ch.grid ? ch.grid.length : (ch.key === "btc" ? 64 : 256);
        const c = ensure(ch.key, ch.name, n, ch.color);
        const g = ch.grid || ".".repeat(n);
        for (let i = 0; i < n; i++) {
          const f = (ch.grid_funded && ch.grid_funded[i]) || 0;
          if (f > c.funded[i] && g[i] === "2") {
            const col = i % c.side, row = Math.floor(i / c.side);
            spark(c.origin.x + col, c.heights[i] + .3, c.origin.z + row, c.color, Math.min(40, 4 + Math.ceil(Math.log2(f - c.funded[i] + 1)) * 4), false);
          }
          c.funded[i] = f; c.state[i] = g[i];
        }
      }
    };

    // ---------- interaction ----------
    const el = renderer.domElement;
    el.style.touchAction = "none"; el.style.cursor = "grab";
    const pointers = new Map(); let pinch = 0;
    el.addEventListener("pointerdown", e => { pointers.set(e.pointerId, e); dragging = true; lastX = e.clientX; lastY = e.clientY; el.setPointerCapture(e.pointerId); el.style.cursor = "grabbing"; idleSince = performance.now(); });
    el.addEventListener("pointerup", e => { pointers.delete(e.pointerId); if (!pointers.size) { dragging = false; el.style.cursor = "grab"; } pinch = 0; });
    el.addEventListener("pointercancel", e => { pointers.delete(e.pointerId); dragging = pointers.size > 0; });
    el.addEventListener("pointermove", e => {
      if (pointers.has(e.pointerId)) pointers.set(e.pointerId, e);
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        const d = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
        if (pinch) radius = Math.min(140, Math.max(10, radius * pinch / d));
        pinch = d; idleSince = performance.now(); return;
      }
      if (dragging) {
        theta -= (e.clientX - lastX) * 0.008; phi = Math.min(1.45, Math.max(0.25, phi - (e.clientY - lastY) * 0.006));
        lastX = e.clientX; lastY = e.clientY; idleSince = performance.now();
      }
      hover(e);
    });
    el.addEventListener("pointerleave", () => { if (tooltip) tooltip.hidden = true; });
    el.addEventListener("wheel", e => { e.preventDefault(); radius = Math.min(140, Math.max(10, radius * (1 + Math.sign(e.deltaY) * 0.08))); idleSince = performance.now(); }, { passive: false });

    const ray = new T.Raycaster(), mouse = new T.Vector2();
    const NAMES = { ".": "no addresses", "0": "waiting", "1": "scanning now", "2": "done" };
    function hover(e) {
      if (!tooltip) return;
      const r = el.getBoundingClientRect();
      mouse.set(((e.clientX - r.left) / r.width) * 2 - 1, -((e.clientY - r.top) / r.height) * 2 + 1);
      ray.setFromCamera(mouse, camera);
      let best = null;
      for (const c of chains.values()) {
        const hit = ray.intersectObject(c.mesh)[0];
        if (hit && (!best || hit.distance < best.hit.distance)) best = { c, hit };
      }
      if (!best || best.hit.instanceId == null) { tooltip.hidden = true; return; }
      const i = best.hit.instanceId, c = best.c;
      tooltip.hidden = false;
      tooltip.textContent = `${c.name} · bucket ${c.n === 256 ? "0x" + i.toString(16).padStart(2, "0") : i} · ${NAMES[c.state[i]] || ""}` +
        (c.state[i] === "2" ? ` · ${c.funded[i].toLocaleString("en-IN")} funded` : "");
      tooltip.style.left = (e.clientX - r.left + 14) + "px"; tooltip.style.top = (e.clientY - r.top + 10) + "px";
    }

    function resize() {
      const w = container.clientWidth, h = container.clientHeight;
      renderer.setSize(w, h, false); renderer.domElement.style.width = w + "px"; renderer.domElement.style.height = h + "px";
      camera.aspect = w / Math.max(1, h); camera.updateProjectionMatrix();
    }
    new ResizeObserver(resize).observe(container); resize();

    // ---------- frame loop ----------
    let last = performance.now(), visible = true;
    document.addEventListener("visibilitychange", () => { visible = !document.hidden; });
    const pending = new T.Color(), active = new T.Color(), fundedZero = new T.Color();
    function frame(now) {
      requestAnimationFrame(frame);
      if (!visible) return;
      const dt = Math.min(0.05, (now - last) / 1000); last = now;
      if (dragging || now - idleSince < 2500) baseTheta = theta - 0.32 * Math.sin(now / 9000);
      else if (!reduce) theta = baseTheta + 0.32 * Math.sin(now / 9000);   // gentle sway while idle
      camera.position.set(target.x + radius * Math.sin(phi) * Math.sin(theta), target.y + radius * Math.cos(phi), target.z + radius * Math.sin(phi) * Math.cos(theta));
      camera.lookAt(target);
      pending.set(css("--track", "#e6e8f0")); active.set(css("--accent", "#4655d4"));
      const pulse = reduce ? 0.7 : 0.55 + 0.45 * Math.sin(now / 180);
      for (const c of chains.values()) {
        fundedZero.copy(c.color).lerp(pending, 0.55);
        for (let i = 0; i < c.n; i++) {
          const s = c.state[i];
          let h, col;
          if (s === "2") { h = 0.35 + Math.min(4.5, Math.log10(c.funded[i] + 1) * 1.4); col = c.funded[i] ? c.color : fundedZero; }
          else if (s === "1") { h = 0.5 + pulse * 0.9; col = active; }
          else if (s === "0") { h = 0.18; col = pending; }
          else { h = 0.04; col = pending; }
          c.heights[i] += (h - c.heights[i]) * Math.min(1, dt * 6);
          dummy.position.set(i % c.side, 0, Math.floor(i / c.side));
          dummy.scale.set(1, c.heights[i], 1); dummy.updateMatrix();
          c.mesh.setMatrixAt(i, dummy.matrix); c.mesh.setColorAt(i, col);
        }
        c.mesh.instanceMatrix.needsUpdate = true; if (c.mesh.instanceColor) c.mesh.instanceColor.needsUpdate = true;
      }
      if (streamRate > 0 && chains.size && !reduce) {
        const first = [...chains.values()][0];
        const n = Math.min(6, Math.ceil(streamRate * dt * 8));
        for (let k = 0; k < n; k++) spark(first.origin.x - 8, 1 + Math.random() * 3, first.origin.z + Math.random() * first.side, active, 1, true);
      }
      for (let i = 0; i < MAXP; i++) {
        if (pLife[i] <= 0) { pPos[i * 3 + 1] = -999; continue; }
        pLife[i] -= dt * 0.9;
        pPos[i * 3] += pVel[i * 3]; pPos[i * 3 + 1] += pVel[i * 3 + 1]; pPos[i * 3 + 2] += pVel[i * 3 + 2];
        pVel[i * 3 + 1] -= 0.002;
      }
      pGeo.attributes.position.needsUpdate = true; pGeo.attributes.color.needsUpdate = true;
      renderer.render(scene, camera);
    }
    requestAnimationFrame(frame);
  }
  window.Scan3D = Scan3D;
})();
