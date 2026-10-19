"""Daily chart report from the telemetry store: a self-contained report.html (inline SVG, no external assets) plus
metrics.json (the numbers behind it, for a human or an LLM to synthesize from). Stdlib only, so it runs inside the
collector's sandbox.

Reads only `daily_rollup` (already aggregate numbers) and `deploy_reflog` (short shas + action verbs), so the report
can't show anything a snapshot couldn't.

    python3 -m observatory.charts --db telemetry.db --out charts/ [--day 2026-10-18] [--tz Asia/Tokyo] [--config env]

The collector calls `ensure()` once a day: TELEMETRY_DIR/charts/<day>/{report.html,metrics.json} + a `latest` link.
"""
import argparse, collections, datetime, html, json, math, os, pathlib, re, shutil, zoneinfo

from observatory import health
from observatory.rollup import rows
from observatory.telemetry import parse_ts

WINDOWS = (30, 90)
KEEP_DAYS = 60
DAY_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Each chart: one y-axis, at most 4 series (palette slots 1-4 in fixed order).
# series: (metric, label) pairs; `derive` metrics are computed in series_for(). health: check whose thresholds are drawn.
CHARTS = [
    {"id": "batches", "title": "Batch attempts per day", "kind": "stack", "fmt": "int",
     "series": [("completed", "Completed"), ("retryable", "Retryable")]},
    {"id": "retry_share", "title": "Retryable share of batch attempts", "kind": "line", "fmt": "pct",
     "series": [("retryable_share", "Retryable share")], "health": "batch_retryable_share_24h"},
    {"id": "retry_reasons", "title": "Retry reasons", "kind": "stack", "fmt": "int", "series": "retry_reason:"},
    {"id": "whisper", "title": "Whisper ready time (s)", "kind": "line", "fmt": "s", "scale": 0.001,
     "series": [("whisper_ready_ms_p50", "p50"), ("whisper_ready_ms_p90", "p90")], "health": "whisper_ready_p90_s_24h"},
    {"id": "steps", "title": "Letta steps per day", "kind": "stack", "fmt": "int",
     "series": [("ok_steps", "Succeeded"), ("failed_steps", "Failed")]},
    {"id": "failed_share", "title": "Failed share of Letta steps", "kind": "line", "fmt": "pct",
     "series": [("failed_step_share", "Failed-step share")], "health": "letta_failed_step_share_24h"},
    {"id": "cache", "title": "Prompt cache-hit share", "kind": "line", "fmt": "pct", "series": [("cache_hit_share", "Cache-hit share")]},
    {"id": "tokens", "title": "Prompt tokens per day", "kind": "stack", "fmt": "compact",
     "series": [("cached_tokens", "Cached"), ("uncached_tokens", "Uncached")]},
    {"id": "backlog", "title": "Transcript lines past the cursor", "kind": "line", "fmt": "compact",
     "series": [("backlog_lines", "Backlog lines")]},
    {"id": "backlog_sessions", "title": "Sessions with a backlog", "kind": "line", "fmt": "int",
     "series": [("backlog_sessions", "Sessions")]},
    {"id": "intents", "title": "Remember intents per day", "kind": "line", "fmt": "int",
     "series": [("intents_captured", "Captured"), ("intents_resolved", "Resolved")]},
    {"id": "cost", "title": "opencode cost per day (USD)", "kind": "stack", "fmt": "usd",
     "series": [("opencode_hit_cost", "Cache-hit requests"), ("opencode_miss_cost", "Cache-miss requests")]},
    {"id": "pg", "title": "Letta database size", "kind": "line", "fmt": "bytes",
     "series": [("pg_bytes:_database", "Whole database"), ("pg_bytes:provider_traces", "provider_traces")]},
]


# ---------------------------------------------------------------- data

def day_range(last_day, n):
    last = datetime.date.fromisoformat(last_day)
    return [(last - datetime.timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)]


def load(db, days):
    """{scope: {metric: [value per day]}} for scope `all` and per-model scopes."""
    index = {d: i for i, d in enumerate(days)}
    out = collections.defaultdict(lambda: collections.defaultdict(lambda: [None] * len(days)))
    for r in rows(db, "select day, scope, metric, value from daily_rollup where day >= ? and day <= ? "
                      "and (scope = 'all' or scope like 'model:%')", (days[0], days[-1])):
        out[r["scope"]][r["metric"]][index[r["day"]]] = r["value"]
    return out


def minus(a, b):
    return [x - (y or 0) if x is not None else None for x, y in zip(a, b)]


def derived(metrics, n):
    get = lambda m: metrics.get(m, [None] * n)
    return {"ok_steps": minus(get("steps"), get("failed_steps")),
            "uncached_tokens": minus(get("prompt_tokens"), get("cached_tokens")),
            "opencode_hit_cost": minus(get("opencode_cost"), get("opencode_miss_cost"))}


def series_for(chart, metrics, n):
    """[(label, values)] for a chart; the `retry_reason:` prefix becomes the top 3 reasons + Other."""
    pool = {**metrics, **derived(metrics, n)}
    if isinstance(chart["series"], str):
        prefix = chart["series"]
        found = sorted(((m, v) for m, v in metrics.items() if m.startswith(prefix)),
                       key=lambda mv: -sum(x or 0 for x in mv[1]))
        out = [(m[len(prefix):], v) for m, v in found[:3]]
        if len(found) > 3:
            rest = [sum((v[i] or 0) for _, v in found[3:]) or None for i in range(n)]
            out.append(("other", rest))
        return out
    scale = chart.get("scale", 1)
    return [(label, [v * scale if v is not None else None for v in pool.get(m, [None] * n)]) for m, label in chart["series"]]


def deploys(db, tz, days):
    """Deploy markers: the newest reflog sha per checkout per day (short sha + action verb only)."""
    first, last, out = days[0], days[-1], {}
    for r in rows(db, "select checkout_idx, sha, at, action from deploy_reflog"):
        t = parse_ts(r["at"])
        if not t or not r["sha"]:
            continue
        d = t.astimezone(tz).date().isoformat()
        key = (d, r["checkout_idx"])
        if first <= d <= last and (key not in out or t > out[key][0]):
            out[key] = (t, str(r["sha"])[:7], r["action"])
    return sorted(({"day": d, "checkout": c, "sha": sha, "action": action} for (d, c), (_, sha, action) in out.items()),
                  key=lambda x: (x["day"], x["checkout"] or 0))


def summary(metrics, n):
    """Last 7 days vs the 7 before, from daily values."""
    get = lambda m: metrics.get(m, [None] * n)
    tot = lambda m, a, b: sum(v for v in get(m)[a:b] if v is not None)

    def window(a, b):
        attempts, steps = tot("attempts", a, b), tot("steps", a, b)
        p90s = sorted(v for v in get("whisper_ready_ms_p90")[a:b] if v is not None)
        return {"attempts": attempts,
                "retryable_share": tot("retryable", a, b) / attempts if attempts else None,
                "steps": steps,
                "failed_step_share": tot("failed_steps", a, b) / steps if steps else None,
                "whisper_p90_s_median_day": p90s[len(p90s) // 2] / 1000 if p90s else None,
                "opencode_cost": tot("opencode_cost", a, b) if any(v is not None for v in get("opencode_cost")[a:b]) else None}

    # Growth per day between the newest size sample and the newest one at least 7 days older
    known = [(i, v) for i, v in enumerate(get("pg_bytes:provider_traces")) if v is not None]
    older = [(i, v) for i, v in known if known and i <= known[-1][0] - 7]
    growth = (known[-1][1] - older[-1][1]) / (known[-1][0] - older[-1][0]) if older else None
    return {"last_7d": window(n - 7, n), "prev_7d": window(n - 14, n - 7),
            "provider_traces_bytes": known[-1][1] if known else None,
            "provider_traces_bytes_per_day_7d": growth}


# ---------------------------------------------------------------- formatting

def compact(v):
    for div, unit in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"{v / div:.1f}".rstrip("0").rstrip(".") + unit
    return f"{v:.0f}"


def fmt(kind, v):
    if v is None:
        return "–"
    if kind == "pct":
        return f"{v * 100:.1f}%"
    if kind == "s":
        return f"{v:.0f}s" if v >= 10 or v == 0 else f"{v:.1f}s"
    if kind == "usd":
        return f"${v:.2f}"
    if kind == "bytes":
        for div, unit in ((1 << 30, "GB"), (1 << 20, "MB"), (1 << 10, "KB")):
            if v >= div:
                return f"{v / div:.1f} {unit}"
        return f"{v:.0f} B"
    if kind == "compact":
        return compact(v)
    return f"{v:,.0f}"


def nice_ticks(top):
    """0..top in 3-5 round steps."""
    if top <= 0:
        return [0, 1]
    raw = top / 4
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    return [i * step for i in range(int(math.ceil(top / step - 1e-9)) + 1)]


# ---------------------------------------------------------------- svg

W, H, L, R, T, B = 720, 210, 52, 84, 14, 26
PW, PH = W - L - R, H - T - B
E = html.escape


def bar_path(x, y, w, h, r):
    """Column with a rounded top (data end) and a square base."""
    r = min(r, w / 2, h)
    return (f"M{x:.1f},{y + h:.1f}V{y + r:.1f}Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f}H{x + w - r:.1f}"
            f"Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f}V{y + h:.1f}Z")


def svg_chart(chart, days, series, deploy_list, bands):
    n, kind, unit = len(days), chart["kind"], chart["fmt"]
    band_w = PW / n
    xc = lambda i: L + band_w * (i + 0.5)
    if kind == "stack":
        top = max((sum(v[i] or 0 for _, v in series) for i in range(n)), default=0)
    else:
        top = max((x for _, v in series for x in v if x is not None), default=0)
    if bands and bands[0] is not None and bands[0] <= top * 1.5:  # show the warn zone when it is near the data
        top = max(top, bands[0] * 1.15)
    ticks = nice_ticks(top)
    ymax = ticks[-1] or 1
    y = lambda v: T + PH - (v / ymax) * PH
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{E(chart["title"])}">']
    # threshold zones (status colors, labelled: never color alone)
    warn, alert = bands or (None, None)
    for lo, hi, cls, label in ((warn, alert, "z-warn", "warn"), (alert, None, "z-alert", "alert")):
        if lo is None or lo >= ymax:
            continue
        hi = min(hi if hi is not None else ymax, ymax)
        out.append(f'<rect class="{cls}" x="{L}" y="{y(hi):.1f}" width="{PW}" height="{y(lo) - y(hi):.1f}">'
                   f'<title>{label} ≥ {E(fmt(unit, lo))}</title></rect>')
    for t in ticks:
        out.append(f'<line class="grid" x1="{L}" x2="{L + PW}" y1="{y(t):.1f}" y2="{y(t):.1f}"/>'
                   f'<text class="tick" x="{L - 6}" y="{y(t) + 4:.1f}" text-anchor="end">{E(fmt(unit, t) if unit != "int" else compact(t))}</text>')
    every = max(1, math.ceil(n / 6))
    for i in range(n - 1, -1, -every):
        out.append(f'<text class="tick" x="{xc(i):.1f}" y="{H - 8}" text-anchor="middle">{days[i][5:]}</text>')
    marks = {}
    for dep in deploy_list:
        if dep["day"] in days:
            marks.setdefault(days.index(dep["day"]), []).append(dep["sha"])
    for i, shas in marks.items():
        out.append(f'<line class="deploy" x1="{xc(i):.1f}" x2="{xc(i):.1f}" y1="{T}" y2="{T + PH}"/>'
                   f'<path class="deploy-tip" d="M{xc(i) - 4:.1f},{T - 1}h8l-4,6z"><title>deploy {E(", ".join(shas))}</title></path>')
    if kind == "stack":
        bw = min(24, band_w * 0.7)
        for i in range(n):
            base, parts = 0, [(k, v[i]) for k, (_, v) in enumerate(series) if v[i]]
            for j, (k, v) in enumerate(parts):
                y0, y1 = y(base + v), y(base)
                gap = 2 if j > 0 else 0  # surface gap between stacked segments
                h = max(0.0, y1 - y0 - gap)
                if h > 0:
                    radius = 4 if j == len(parts) - 1 else 0
                    out.append(f'<path class="f{k + 1}" d="{bar_path(xc(i) - bw / 2, y0, bw, h, radius)}"/>')
                base += v
    else:
        labels = []
        for k, (name, v) in enumerate(series):
            path, pen = [], False
            for i, x in enumerate(v):
                if x is None:
                    pen = False
                    continue
                path.append(f'{"L" if pen else "M"}{xc(i):.1f},{y(x):.1f}')
                pen = True
            if not path:
                continue
            out.append(f'<path class="l{k + 1}" d="{"".join(path)}"/>')
            last = max(i for i, x in enumerate(v) if x is not None)
            out.append(f'<circle class="d{k + 1}" cx="{xc(last):.1f}" cy="{y(v[last]):.1f}" r="4"/>')
            labels.append((y(v[last]), f"{name} {fmt(unit, v[last])}" if len(series) > 1 else fmt(unit, v[last])))
        placed = []
        for ly, text in sorted(labels):
            if all(abs(ly - p) >= 13 for p in placed):  # drop a colliding end label (legend + tooltip still carry it)
                placed.append(ly)
                out.append(f'<text class="end" x="{L + PW + 8}" y="{ly + 4:.1f}">{E(text)}</text>')
    out.append(f'<line class="xh" x1="0" x2="0" y1="{T}" y2="{T + PH}" visibility="hidden"/>'
               f'<rect class="hit" x="{L}" y="{T}" width="{PW}" height="{PH}"/></svg>')
    return "".join(out)


def chart_block(chart, days, series, deploy_list, bands):
    unit = chart["fmt"]
    dep = {}
    for d in deploy_list:
        dep.setdefault(d["day"], []).append(d["sha"])
    data = {"days": days, "l": L, "pw": PW,
            "series": [{"name": name, "k": k + 1, "v": [fmt(unit, x) if x is not None else None for x in v]}
                       for k, (name, v) in enumerate(series)],
            "deploys": dep}
    keys = [f'<span><i class="sw f{k + 1}"></i>{E(name)}</span>' for k, (name, _) in enumerate(series)] if len(series) > 1 else []
    keys += [f'<span><i class="sw z-{label}"></i>{label} ≥ {E(fmt(unit, lo))}</span>'
             for label, lo in zip(("warn", "alert"), bands or ()) if lo is not None]
    legend = '<div class="legend">' + "".join(keys) + "</div>" if keys else ""
    head = "".join(f"<th>{E(name)}</th>" for name, _ in series)
    body = "".join(f"<tr><td>{d}</td>" + "".join(f"<td>{fmt(unit, v[i])}</td>" for _, v in series) + "</tr>"
                   for i, d in reversed(list(enumerate(days))) if any(v[i] is not None for _, v in series))
    return (f'<figure class="chart" data-chart=\'{E(json.dumps(data, separators=(",", ":")))}\'>'
            f'<figcaption>{E(chart["title"])}</figcaption>{legend}'
            f'{svg_chart(chart, days, series, deploy_list, bands)}<div class="tip" hidden></div>'
            f'<details><summary>Table</summary><table><tr><th>Day</th>{head}</tr>{body}</table></details></figure>')


# ---------------------------------------------------------------- page

CSS = """
.viz-root{color-scheme:light;--surface-1:#fcfcfb;--page:#f9f9f7;--text-primary:#0b0b0b;--text-secondary:#52514e;
--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--border:rgba(11,11,11,.10);
--series-1:#2a78d6;--series-2:#eb6834;--series-3:#1baf7a;--series-4:#eda100;--warn:#fab219;--alert:#d03b3b}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])) .viz-root{color-scheme:dark;--surface-1:#1a1a19;
--page:#0d0d0d;--text-primary:#f0efec;--text-secondary:#c3c2b7;--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);
--series-1:#3987e5;--series-2:#d95926;--series-3:#199e70;--series-4:#c98500}}
:root[data-theme="dark"] .viz-root{color-scheme:dark;--surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#f0efec;
--text-secondary:#c3c2b7;--grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);
--series-1:#3987e5;--series-2:#d95926;--series-3:#199e70;--series-4:#c98500}
body{margin:0}
.viz-root{background:var(--page);color:var(--text-primary);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif;
min-height:100vh;padding:24px}
.wrap{max-width:1520px;margin:0 auto}
h1{font-size:20px;margin:0 0 4px}.sub{color:var(--text-secondary);margin:0 0 16px}
.bar{display:flex;gap:8px;align-items:center;margin:0 0 16px;color:var(--text-secondary)}
.bar button{font:inherit;border:1px solid var(--border);background:var(--surface-1);color:var(--text-primary);
border-radius:6px;padding:4px 10px;cursor:pointer}.bar button[aria-pressed="true"]{font-weight:600;border-color:var(--text-secondary)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:0 0 20px}
.tile{background:var(--surface-1);border:1px solid var(--border);border-radius:10px;padding:12px 14px}
.tile .k{color:var(--text-secondary);font-size:13px}.tile .v{font-size:22px;font-weight:600}
.tile .d{color:var(--muted);font-size:12px}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(560px,1fr));gap:16px}
.chart{position:relative;margin:0;background:var(--surface-1);border:1px solid var(--border);border-radius:10px;padding:12px 14px}
figcaption{font-weight:600;margin:0 0 4px}
.legend{display:flex;gap:14px;flex-wrap:wrap;color:var(--text-secondary);font-size:12px;margin:0 0 2px}
.sw{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
svg{width:100%;height:auto;display:block}
.grid{stroke:var(--grid);stroke-width:1}.tick,.zl,.end{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.end{fill:var(--text-secondary)}
.z-warn{fill:var(--warn);opacity:.10}.z-alert{fill:var(--alert);opacity:.09}
.deploy{stroke:var(--muted);stroke-width:1;opacity:.6}.deploy-tip{fill:var(--muted)}
.xh{stroke:var(--text-secondary);stroke-width:1}.hit{fill:transparent}
.l1,.l2,.l3,.l4{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.l1{stroke:var(--series-1)}.l2{stroke:var(--series-2)}.l3{stroke:var(--series-3)}.l4{stroke:var(--series-4)}
.d1,.d2,.d3,.d4{stroke:var(--surface-1);stroke-width:2}
.f1,.d1{fill:var(--series-1)}.f2,.d2{fill:var(--series-2)}.f3,.d3{fill:var(--series-3)}.f4,.d4{fill:var(--series-4)}
.sw.f1{background:var(--series-1)}.sw.f2{background:var(--series-2)}.sw.f3{background:var(--series-3)}.sw.f4{background:var(--series-4)}
.sw.z-warn,.sw.z-alert{opacity:1}.sw.z-warn{background:color-mix(in srgb,var(--warn) 30%,var(--surface-1))}.sw.z-alert{background:color-mix(in srgb,var(--alert) 25%,var(--surface-1))}
.empty{color:var(--muted);padding:40px 0;text-align:center}
.tip{position:absolute;pointer-events:none;background:var(--surface-1);border:1px solid var(--border);border-radius:8px;
padding:6px 9px;font-size:12px;box-shadow:0 2px 8px rgba(0,0,0,.12);white-space:nowrap;z-index:2}
.tip{text-align:left}.tip b{display:block;margin-bottom:2px}.tip .sw{width:8px;height:8px}
details{margin-top:6px;color:var(--text-secondary);font-size:12px}
table{border-collapse:collapse;margin-top:6px;font-variant-numeric:tabular-nums}
td,th{padding:2px 10px 2px 0;text-align:right;border-bottom:1px solid var(--grid)}td:first-child,th:first-child{text-align:left}
.w90{display:none}.show90 .w90{display:block}.show90 .w30{display:none}
"""

JS = """
document.querySelectorAll('.bar button').forEach(b=>b.onclick=()=>{
  document.querySelector('.viz-root').classList.toggle('show90',b.dataset.w==='90');
  document.querySelectorAll('.bar button').forEach(x=>x.setAttribute('aria-pressed',x===b));});
document.querySelectorAll('.chart[data-chart]').forEach(fig=>{
  const d=JSON.parse(fig.dataset.chart),svg=fig.querySelector('svg'),tip=fig.querySelector('.tip'),xh=svg.querySelector('.xh');
  const n=d.days.length,bw=d.pw/n;
  svg.addEventListener('mousemove',e=>{
    const p=new DOMPoint(e.clientX,e.clientY).matrixTransform(svg.getScreenCTM().inverse());
    const i=Math.floor((p.x-d.l)/bw);if(i<0||i>=n){tip.hidden=true;xh.setAttribute('visibility','hidden');return;}
    const x=d.l+bw*(i+.5);xh.setAttribute('x1',x);xh.setAttribute('x2',x);xh.setAttribute('visibility','visible');
    let h='<b>'+d.days[i]+'</b>'+d.series.map(s=>'<div><i class="sw f'+s.k+'"></i>'+s.name+': '+(s.v[i]??'–')+'</div>').join('');
    if(d.deploys[d.days[i]])h+='<div>deploy '+d.deploys[d.days[i]].join(', ')+'</div>';
    tip.innerHTML=h;tip.hidden=false;const r=fig.getBoundingClientRect(),w=tip.offsetWidth;
    let left=e.clientX-r.left+12;if(left+w>r.width-4)left=e.clientX-r.left-w-12;
    tip.style.left=left+'px';tip.style.top=(e.clientY-r.top+12)+'px';});
  svg.addEventListener('mouseleave',()=>{tip.hidden=true;xh.setAttribute('visibility','hidden');});});
"""


def tiles(s):
    a, b = s["last_7d"], s["prev_7d"]
    items = [("Batch attempts", "int", "attempts"), ("Retryable share", "pct", "retryable_share"),
             ("Failed-step share", "pct", "failed_step_share"), ("Whisper p90 (median day)", "s", "whisper_p90_s_median_day"),
             ("opencode cost", "usd", "opencode_cost")]
    out = [f'<div class="tile"><div class="k">{k}</div><div class="v">{fmt(unit, a[m])}</div>'
           f'<div class="d">last 7 days · previous {fmt(unit, b[m])}</div></div>' for k, unit, m in items]
    growth = s["provider_traces_bytes_per_day_7d"]
    out.append(f'<div class="tile"><div class="k">provider_traces</div><div class="v">{fmt("bytes", s["provider_traces_bytes"])}</div>'
               f'<div class="d">{"+" + fmt("bytes", growth) + "/day (7d)" if growth is not None else "growth: needs 8 days of samples"}</div></div>')
    return '<div class="tiles">' + "".join(out) + "</div>"


def build(db, tz, day, thresholds, generated_at=None):
    """(report_html, metrics_dict) for the window ending on `day` (inclusive, local date)."""
    days = day_range(day, max(WINDOWS))
    scopes = load(db, days)
    metrics = scopes.get("all", {})
    n = len(days)
    deploy_list = deploys(db, tz, days)
    s = summary(metrics, n)
    sections = []
    for w in WINDOWS:
        wd = days[-w:]
        figs = []
        for chart in CHARTS:
            series = [(name, v[-w:]) for name, v in series_for(chart, metrics, n)]
            if not series or not any(x is not None for _, v in series for x in v):
                figs.append(f'<figure class="chart"><figcaption>{E(chart["title"])}</figcaption><div class="empty">No data in this window yet</div></figure>')
                continue
            figs.append(chart_block(chart, wd, series, deploy_list, thresholds.get(chart.get("health"))))
        sections.append(f'<div class="w{w}"><div class="grid2">{"".join(figs)}</div></div>')
    generated_at = generated_at or datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    page = (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>Subcon telemetry · {day}</title><style>{CSS}</style></head><body><div class="viz-root"><div class="wrap">'
            f'<h1>Subcon telemetry · through {day}</h1><p class="sub">Daily rollup ({tz}), aggregates only. '
            f'Shaded zones are healthcheck warn / alert levels; ▼ marks a deploy. Generated {E(generated_at)}.</p>'
            f'{tiles(s)}<div class="bar">Window <button data-w="30" aria-pressed="true">30 days</button>'
            f'<button data-w="90" aria-pressed="false">90 days</button></div>{"".join(sections)}'
            f'</div></div><script>{JS}</script></body></html>')
    data = {"schema": "subcon_charts_v1", "day": day, "tz": str(tz), "generated_at": generated_at, "days": days,
            "series": {scope: {m: v for m, v in ms.items() if any(x is not None for x in v)} for scope, ms in scopes.items()},
            "deploys": deploy_list, "summary": s,
            "thresholds": {name: {"warn": w, "alert": a} for name, (w, a) in thresholds.items()}}
    return page, data


def ensure(db, tz, day, root, thresholds, keep_days=KEEP_DAYS, force=False):
    """Write root/<day>/ once (or again with force), then publish `latest` and prune old days. True if written.
    Publishing runs on every call, so a crash between the directory rename and the link swap heals on the next run."""
    root = pathlib.Path(root)
    out = root / day
    written = False
    if force or not (out / "report.html").exists():
        root.mkdir(parents=True, exist_ok=True)
        page, data = build(db, tz, day, thresholds)
        tmp = root / f".{day}.{os.getpid()}.tmp"
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir()
        (tmp / "report.html").write_text(page)
        (tmp / "metrics.json").write_text(json.dumps(data, indent=1, sort_keys=True))
        for p in [tmp, *tmp.iterdir()]:
            os.chmod(p, 0o755 if p.is_dir() else 0o644)
        shutil.rmtree(out, ignore_errors=True)
        tmp.rename(out)
        written = True
    publish(root, keep_days)
    return written


def complete_days(root):
    return sorted(p.name for p in root.iterdir() if DAY_DIR.match(p.name) and not p.is_symlink() and (p / "report.html").exists())


def publish(root, keep_days=KEEP_DAYS):
    """Point root/latest at the newest complete day (idempotent) and drop days older than keep_days before it."""
    days = complete_days(root)
    if not days:
        return
    newest, latest = days[-1], root / "latest"
    if not (latest.is_symlink() and os.readlink(latest) == newest):
        link = root / f".latest.{os.getpid()}.tmp"
        link.unlink(missing_ok=True)
        link.symlink_to(newest)
        os.replace(link, latest)
    cutoff = (datetime.date.fromisoformat(newest) - datetime.timedelta(days=keep_days)).isoformat()
    for p in root.iterdir():
        if p.is_dir() and not p.is_symlink() and DAY_DIR.match(p.name) and p.name < cutoff:
            shutil.rmtree(p)


def main():
    import sqlite3
    from observatory.export import load_config
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--day", help="last day of the window (default: yesterday in --tz)")
    parser.add_argument("--tz", default="Asia/Tokyo")
    parser.add_argument("--config", help="collector config, for healthcheck threshold overrides")
    args = parser.parse_args()
    tz = zoneinfo.ZoneInfo(args.tz)
    day = args.day or (datetime.datetime.now(tz).date() - datetime.timedelta(days=1)).isoformat()
    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    ensure(db, tz, day, args.out, health.thresholds(load_config(args.config) if args.config else {}), force=True)
    print(pathlib.Path(args.out) / day / "report.html")


if __name__ == "__main__":
    main()
