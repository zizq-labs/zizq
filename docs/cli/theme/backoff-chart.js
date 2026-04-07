(function(){
  const $ = id => document.getElementById(id);
  const canvas = $('bc-canvas');
  if (!canvas) return; // Not on the backoff page.

  const ctx = canvas.getContext('2d');
  const tooltip = $('bc-tooltip');

  // Full detail: 3d20h9m41s
  function fmtDur(sec) {
    if (sec < 0) sec = 0;
    const d = Math.floor(sec / 86400);
    const h = Math.floor((sec % 86400) / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    const parts = [];
    if (d > 0) parts.push(d + 'd');
    if (h > 0) parts.push(h + 'h');
    if (m > 0) parts.push(m + 'm');
    if (s > 0 || parts.length === 0) parts.push(s + 's');
    return parts.join('');
  }

  // Most significant unit only: ~4d, ~21h, ~45m, 30s
  function fmtDurShort(sec) {
    if (sec < 0) sec = 0;
    const d = sec / 86400;
    const h = sec / 3600;
    const m = sec / 60;
    if (d >= 1) return '~' + Math.round(d) + 'd';
    if (h >= 1) return '~' + Math.round(h) + 'h';
    if (m >= 1) return '~' + Math.round(m) + 'm';
    return Math.round(sec) + 's';
  }

  // Paired input syncing: number <-> range
  const params = [
    { num: 'bc-base-num', range: 'bc-base' },
    { num: 'bc-exp-num', range: 'bc-exp' },
    { num: 'bc-jitter-num', range: 'bc-jitter' },
    { num: 'bc-retries-num', range: 'bc-retries' },
  ];

  params.forEach(({ num, range }) => {
    const numEl = $(num), rangeEl = $(range);
    numEl.addEventListener('input', () => {
      // Clamp the range slider to its max, but let the number go higher.
      rangeEl.value = Math.min(+numEl.value, +rangeEl.max);
      draw();
    });
    rangeEl.addEventListener('input', () => {
      numEl.value = rangeEl.value;
      draw();
    });
  });

  function getParams() {
    return {
      B: +$('bc-base-num').value,
      E: +$('bc-exp-num').value,
      J: +$('bc-jitter-num').value,
      N: Math.max(1, Math.round(+$('bc-retries-num').value)),
    };
  }

  function computeCurves(p) {
    const min = [], max = [], cumMin = [], cumMax = [];
    let totalMin = 0, totalMax = 0;
    for (let a = 0; a < p.N; a++) {
      const base = p.B + Math.pow(a, p.E);
      const lo = base;
      const hi = base + a * p.J;
      min.push(lo);
      max.push(hi);
      totalMin += lo;
      totalMax += hi;
      cumMin.push(totalMin);
      cumMax.push(totalMax);
    }
    return { min, max, cumMin, cumMax };
  }

  let scrubX = null;

  function draw() {
    const p = getParams();

    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    const W = rect.width, H = rect.height;

    const curves = computeCurves(p);
    const yMax = Math.max(curves.max[curves.max.length - 1] || 1, 1);

    // Layout
    const padL = 56, padR = 16, padT = 16, padB = 36;
    const cW = W - padL - padR, cH = H - padT - padB;

    // Colours from CSS vars or sensible defaults for dark theme
    const cs = getComputedStyle(document.documentElement);
    const fg = cs.getPropertyValue('--fg').trim() || '#cdd6f4';
    const fgMuted = cs.getPropertyValue('--sidebar-fg').trim() || '#585b70';
    const accent = cs.getPropertyValue('--zizq-bright-cyan').trim() || '#89b4fa';
    const accent2 = cs.getPropertyValue('--zizq-bright-magenta').trim() || '#f38ba8';

    function xPos(i) { return padL + (i / Math.max(p.N - 1, 1)) * cW; }
    function yPos(v) { return padT + cH - (v / yMax) * cH; }

    ctx.clearRect(0, 0, W, H);

    // Grid lines & Y labels (short format)
    const nTicks = 6;
    ctx.font = '11px monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= nTicks; i++) {
      const v = (i / nTicks) * yMax;
      const y = yPos(v);
      ctx.strokeStyle = fgMuted;
      ctx.globalAlpha = 0.25;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(padL + cW, y);
      ctx.stroke();
      ctx.globalAlpha = 1;
      ctx.fillStyle = fg;
      ctx.fillText(fmtDurShort(v), padL - 6, y);
    }

    // X labels
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    const xStep = p.N <= 25 ? 5 : p.N <= 50 ? 10 : 20;
    for (let i = 0; i < p.N; i += xStep) {
      ctx.fillStyle = fg;
      ctx.fillText(i + 1, xPos(i), padT + cH + 6);
    }
    if ((p.N - 1) % xStep !== 0) {
      ctx.fillText(p.N, xPos(p.N - 1), padT + cH + 6);
    }
    ctx.fillText('attempt', padL + cW / 2, padT + cH + 22);

    // Fill between curves
    ctx.beginPath();
    for (let i = 0; i < p.N; i++) ctx.lineTo(xPos(i), yPos(curves.min[i]));
    for (let i = p.N - 1; i >= 0; i--) ctx.lineTo(xPos(i), yPos(curves.max[i]));
    ctx.closePath();
    ctx.fillStyle = accent;
    ctx.globalAlpha = 0.15;
    ctx.fill();
    ctx.globalAlpha = 1;

    // Min line
    ctx.beginPath();
    for (let i = 0; i < p.N; i++) ctx.lineTo(xPos(i), yPos(curves.min[i]));
    ctx.strokeStyle = accent;
    ctx.lineWidth = 2;
    ctx.stroke();

    // Max line
    ctx.beginPath();
    for (let i = 0; i < p.N; i++) ctx.lineTo(xPos(i), yPos(curves.max[i]));
    ctx.strokeStyle = accent2;
    ctx.lineWidth = 2;
    ctx.stroke();

    // Scrub line
    if (scrubX !== null && scrubX >= padL && scrubX <= padL + cW) {
      const idx = Math.round(((scrubX - padL) / cW) * (p.N - 1));
      const sx = xPos(idx);

      ctx.strokeStyle = fg;
      ctx.lineWidth = 1;
      ctx.globalAlpha = 0.5;
      ctx.beginPath();
      ctx.moveTo(sx, padT);
      ctx.lineTo(sx, padT + cH);
      ctx.stroke();
      ctx.globalAlpha = 1;

      // Dots
      [curves.min[idx], curves.max[idx]].forEach((v, vi) => {
        ctx.beginPath();
        ctx.arc(sx, yPos(v), 4, 0, Math.PI * 2);
        ctx.fillStyle = vi === 0 ? accent : accent2;
        ctx.fill();
      });

      // Tooltip (full detail + cumulative)
      tooltip.style.display = 'block';
      tooltip.innerHTML =
        '<strong>Attempt ' + (idx + 1) + '</strong><br>' +
        '<span style="color:' + accent + '">Min:</span> ' + fmtDur(curves.min[idx]) + '<br>' +
        '<span style="color:' + accent2 + '">Max:</span> ' + fmtDur(curves.max[idx]) + '<br>' +
        '<span style="opacity:0.7">Total: ' + fmtDur(curves.cumMin[idx]) +
        ' – ' + fmtDur(curves.cumMax[idx]) + '</span>';

      let tx = sx + 12, ty = yPos((curves.min[idx] + curves.max[idx]) / 2) - 30;
      if (tx + 140 > W) tx = sx - 140;
      if (ty < 0) ty = 4;
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
    } else {
      tooltip.style.display = 'none';
    }
  }

  function pointerMove(e) {
    const rect = canvas.getBoundingClientRect();
    scrubX = (e.touches ? e.touches[0].clientX : e.clientX) - rect.left;
    draw();
  }
  function pointerLeave() { scrubX = null; draw(); }

  canvas.addEventListener('mousemove', pointerMove);
  canvas.addEventListener('mouseleave', pointerLeave);
  canvas.addEventListener('touchmove', function(e) { e.preventDefault(); pointerMove(e); }, { passive: false });
  canvas.addEventListener('touchend', pointerLeave);

  window.addEventListener('resize', draw);
  draw();
})();
