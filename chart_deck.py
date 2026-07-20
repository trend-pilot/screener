# -*- coding: utf-8 -*-
"""
chart_deck.py — MarketSurge 스타일 베이스 카운트 차트덱 생성기
=====================================================================
base_count.py 의 분석 결과 + OHLCV 를 받아 샘플 차트덱과 동일한
단일 HTML(SVG) 을 만든다.

[샘플에서 그대로 가져온 디자인 토큰]
  --frame #eef1f5 / --paper #fff / --ink #1c2530 / --mut #5f6c7b
  --up #1b5fae(상승봉) / --down #d43f63(하락봉) / --arc #4f8fd4(컵 윤곽)
  --ma50 #e0413c / --ma200 #3c4450 / --ema21 #2e9e62 / --rs #2e6fbe

[렌더 요소] — 샘플 이미지 기준
  헤더(심볼·종가·등락) → 뱃지행(Stage·RS·베이스타입·피벗대비·직전상승)
  → 캔들 + 21EMA/50일선/200일선
  → 컵 윤곽선(좌측고점→저점→돌파) + 피벗 점선 + 스테이지 라벨
  → RS 라인 (굵은 구간 = RS 신고가 영역, 파란 점 = Blue Dot)
  → Ants 삼각형 → 거래량 + 50일 평균선
  → 범례 → 베이스 카운트 체인 카드 → 방법론
"""

from __future__ import annotations

import html as _html
import math
from datetime import datetime
from typing import Dict, List, Optional, Sequence

# ── 디자인 토큰 (샘플과 동일) ─────────────────────────────────────────
TOK = {
    "frame": "#eef1f5", "paper": "#ffffff", "ink": "#1c2530", "mut": "#5f6c7b",
    "line": "#e4eaf1", "border": "#d9e0e8", "up": "#1b5fae", "down": "#d43f63",
    "arc": "#4f8fd4", "ma50": "#e0413c", "ma200": "#3c4450", "ema21": "#2e9e62",
    "rs": "#2e6fbe", "amber": "#a86a14", "amberBg": "#fbf1de", "blueBg": "#e9f1fa",
}

TYPE_KO = {
    "Flat Base": "평평한 바닥 (Flat)",
    "Consolidation": "횡보 조정 (Consol)",
    "Cup With Handle": "컵 (핸들 있음)",
    "Cup Without Handle": "컵 (핸들 없음)",
}

W, H = 1720, 620          # 가격 차트 영역
VOL_H = 150               # 거래량 영역
PAD_L, PAD_R = 46, 96


def _e(s) -> str:
    return _html.escape(str(s if s is not None else ""))


def _ema(vals: Sequence[float], span: int) -> List[Optional[float]]:
    k, out, prev = 2 / (span + 1), [], None
    for v in vals:
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(round(prev, 4))
    return out


def _sma(vals: Sequence[float], n: int) -> List[Optional[float]]:
    out, run = [], 0.0
    for i, v in enumerate(vals):
        run += v
        if i >= n:
            run -= vals[i - n]
        out.append(round(run / n, 4) if i >= n - 1 else None)
    return out


def _fmt_vol(v: float) -> str:
    if v >= 1e8:
        return f"{v/1e8:.0f}억"
    if v >= 1e4:
        return f"{v/1e4:.0f}만"
    return f"{v:,.0f}"


def build_deck(items: List[Dict], title: str = "베이스 카운트 차트덱") -> str:
    """items: [{meta, dates, o,h,l,c,v, rs, rs_hi, bases, ants, blue_dots}]"""
    cards = "\n".join(_one(it) for it in items)
    return f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_e(title)}</title>
<style>
:root{{--frame:{TOK['frame']};--paper:{TOK['paper']};--ink:{TOK['ink']};--mut:{TOK['mut']};
--line:{TOK['line']};--border:{TOK['border']};--up:{TOK['up']};--down:{TOK['down']};
--arc:{TOK['arc']};--ma50:{TOK['ma50']};--ma200:{TOK['ma200']};--ema21:{TOK['ema21']};
--rs:{TOK['rs']};--amber:{TOK['amber']};--amberBg:{TOK['amberBg']};--blueBg:{TOK['blueBg']}}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Malgun Gothic',sans-serif;
background:var(--frame);color:var(--ink);padding:22px}}
.wrap{{max-width:1800px;margin:0 auto 34px;background:var(--paper);
border:1px solid var(--border);border-radius:14px;padding:26px 30px}}
.eyebrow{{font-size:11px;letter-spacing:.16em;color:var(--mut);font-weight:600}}
.hd{{display:flex;justify-content:space-between;align-items:flex-start;margin-top:6px}}
.code{{font-size:34px;font-weight:800;letter-spacing:-.01em}}
.sub{{font-size:12px;color:var(--mut);margin-top:5px}}
.px{{font-size:30px;font-weight:800;text-align:right}}
.chg{{font-size:13px;font-weight:700;text-align:right;margin-top:2px}}
.asof{{font-size:11px;color:var(--mut);text-align:right;margin-top:3px}}
.pos{{color:var(--up)}}.neg{{color:var(--down)}}
.chips{{display:flex;flex-wrap:wrap;gap:9px;margin:16px 0 6px}}
.chip{{font-size:12px;padding:7px 13px;border-radius:8px;border:1px solid var(--border);
background:#f7f9fc;color:var(--ink)}}
.chip.solid{{background:#1552a0;border-color:#1552a0;color:#fff;font-weight:700}}
.chip.warn{{background:var(--amberBg);border-color:#e6d3a8;color:var(--amber);font-weight:600}}
.card{{border:1px solid var(--border);border-radius:12px;margin-top:14px;overflow:hidden}}
.readout{{font-size:12.5px;padding:11px 16px;border-bottom:1px solid var(--line);color:var(--ink)}}
.readout b{{font-weight:700}}
.legend{{font-size:11px;color:var(--mut);padding:11px 16px;border-top:1px solid var(--line);
display:flex;flex-wrap:wrap;gap:14px;align-items:center}}
.legend i{{display:inline-block;width:16px;height:0;border-top:2.5px solid;margin-right:5px;
vertical-align:middle}}
.chain-title{{font-size:14px;font-weight:700;margin:22px 0 10px}}
.chain{{display:flex;gap:14px;flex-wrap:wrap}}
.cell{{flex:1;min-width:210px;border:1px solid var(--border);border-radius:11px;padding:14px 16px}}
.stg{{font-size:19px;font-weight:800;color:#1552a0}}
.typ{{font-size:13px;font-weight:600;margin-top:6px}}
.m{{font-size:12px;color:var(--mut);margin-top:3px}}
.note{{font-size:11.5px;color:var(--mut);line-height:1.85;margin-top:20px;
border-top:1px solid var(--line);padding-top:14px}}
.note b{{color:var(--ink)}}
svg{{display:block;width:100%;height:auto}}
</style></head><body>
{cards}
</body></html>"""


def _one(it: Dict) -> str:
    meta = it["meta"]
    dates, o, h, l, c, v = it["dates"], it["o"], it["h"], it["l"], it["c"], it["v"]
    bases = it.get("bases") or []
    n = len(c)
    last, prev = c[-1], (c[-2] if n > 1 else c[-1])
    chg, chgp = last - prev, (last / prev - 1) * 100 if prev else 0
    sw = meta.get("stage_weekly") or {}

    ema21, ma50, ma200 = _ema(c, 21), _sma(c, 50), _sma(c, 200)
    vol50 = _sma(v, 50)

    # ── 스케일 (로그) ────────────────────────────────────────────────
    lo, hi = min(l), max(h)
    lo, hi = lo * 0.94, hi * 1.06
    lg = lambda p: math.log(max(p, 1e-9))
    plotw = W - PAD_L - PAD_R
    X = lambda i: PAD_L + (i + 0.5) * (plotw / n)
    Y = lambda p: 30 + (lg(hi) - lg(p)) / (lg(hi) - lg(lo)) * (H - 60)
    bw = max(1.1, plotw / n * 0.62)

    parts: List[str] = []
    placed: List[tuple] = []      # 이미 배치된 라벨 사각형 (x0,y0,x1,y1)

    def place(x0: float, y0: float, w_: float, h_: float,
              step: float = 20.0, tries: int = 14) -> float:
        """겹치지 않는 y 를 찾아 반환. 라벨끼리 포개지는 것을 막는다."""
        y = y0
        for _ in range(tries):
            if not any(not (x0 + w_ < a or x1_ < x0 or y + h_ < b or y1_ < y)
                       for (a, b, x1_, y1_) in placed):
                break
            y += step
        placed.append((x0, y, x0 + w_, y + h_))
        return y

    # ── 배경: 가격 눈금 · 현재가 점선 · 연도 구분선 ──────────────────
    def nice_ticks(a: float, b: float, cnt: int = 9):
        """로그 스케일에 어울리는 눈금값 생성 (1·2·2.5·5 계열)."""
        out, span = [], b / max(a, 1e-9)
        step = 10 ** math.floor(math.log10(max(b, 1e-9)))
        while True:
            for m in (1, 1.3, 1.6, 2, 2.5, 3.2, 4, 5, 6.5, 8):
                t = step * m
                if a <= t <= b:
                    out.append(t)
            step /= 10
            if step < a / 100 or len(out) > 40:
                break
        out = sorted(set(out))
        if len(out) > cnt:
            k = max(1, len(out) // cnt)
            out = out[::k]
        return out

    for t in nice_ticks(lo, hi):
        yy = Y(t)
        parts.append(f'<line x1="{PAD_L}" y1="{yy:.1f}" x2="{W-PAD_R}" y2="{yy:.1f}" '
                     f'stroke="{TOK["line"]}" stroke-width="1"/>')
        parts.append(f'<text x="{W-PAD_R+8}" y="{yy+4:.1f}" font-size="11" '
                     f'fill="{TOK["mut"]}">{t:,.6g}</text>')

    # 연도 구분 세로 점선
    for i in range(1, n):
        if dates[i][:4] != dates[i - 1][:4]:
            parts.append(f'<line x1="{X(i):.1f}" y1="26" x2="{X(i):.1f}" y2="{H-26}" '
                         f'stroke="#c8d2de" stroke-width="1" stroke-dasharray="4 4"/>')
            parts.append(f'<text x="{X(i)+5:.1f}" y="{H-30}" font-size="11.5" '
                         f'font-weight="700" fill="{TOK["mut"]}">{dates[i][:4]}</text>')

    # 현재가 수평 점선
    parts.append(f'<line x1="{PAD_L}" y1="{Y(last):.1f}" x2="{W-PAD_R}" y2="{Y(last):.1f}" '
                 f'stroke="#8b98a8" stroke-width="1" stroke-dasharray="5 4"/>')

    # 캔들
    for i in range(n):
        up = c[i] >= (c[i - 1] if i else o[i])
        col = TOK["up"] if up else TOK["down"]
        x = X(i)
        parts.append(f'<line x1="{x:.1f}" y1="{Y(h[i]):.1f}" x2="{x:.1f}" y2="{Y(l[i]):.1f}" '
                     f'stroke="{col}" stroke-width="1"/>')
        y1, y2 = Y(max(o[i], c[i])), Y(min(o[i], c[i]))
        parts.append(f'<rect x="{x-bw/2:.1f}" y="{y1:.1f}" width="{bw:.1f}" '
                     f'height="{max(1,y2-y1):.1f}" fill="{col}"/>')

    # 이동평균
    def path(series, color, wid=1.6):
        pts = [f"{X(i):.1f},{Y(val):.1f}" for i, val in enumerate(series) if val]
        if len(pts) > 1:
            parts.append(f'<polyline points="{" ".join(pts)}" fill="none" '
                         f'stroke="{color}" stroke-width="{wid}"/>')
    path(ema21, TOK["ema21"]); path(ma50, TOK["ma50"]); path(ma200, TOK["ma200"], 1.8)

    # ── 베이스 윤곽 + 피벗 + 라벨 ───────────────────────────────────
    idx = {d: i for i, d in enumerate(dates)}
    for b in bases:
        li, lo_i = idx.get(b["left_date"]), idx.get(b["low_date"])
        bi = idx.get(b["brk_date"]) if b.get("brk_date") else n - 1
        if li is None or lo_i is None or bi is None:
            continue
        # 컵 윤곽 — 실제 저가 경로를 따라가도록 그린다.
        #   (기존엔 2차 베지어 하나로 이어서 베이스가 길면 화면을 가로지르는
        #    거대한 U자가 그려졌다. 참고본은 가격에 밀착한 곡선이다.)
        span = max(1, bi - li)
        step_i = max(1, span // 26)
        pts_cup = []
        for k in range(li, bi + 1, step_i):
            w0, w1 = max(li, k - step_i), min(bi, k + step_i)
            pts_cup.append((X(k), Y(min(l[w0:w1 + 1]))))
        pts_cup.insert(0, (X(li), Y(b["left_high"])))
        pts_cup.append((X(bi), Y(b["pivot"])))
        # 3점 이동평균으로 완만하게
        sm = []
        for k, (px_, py_) in enumerate(pts_cup):
            ys = [pts_cup[j][1] for j in range(max(0, k - 1), min(len(pts_cup), k + 2))]
            sm.append(f"{px_:.1f},{sum(ys)/len(ys):.1f}")
        parts.append(f'<polyline points="{" ".join(sm)}" fill="none" '
                     f'stroke="{TOK["arc"]}" stroke-width="2.4" opacity=".9" '
                     f'stroke-linejoin="round"/>')
        # 피벗 점선
        parts.append(f'<line x1="{X(li):.1f}" y1="{Y(b["pivot"]):.1f}" x2="{X(bi):.1f}" '
                     f'y2="{Y(b["pivot"]):.1f}" stroke="{TOK["arc"]}" stroke-width="1.2" '
                     f'stroke-dasharray="5 4"/>')
        # 돌파 화살표
        parts.append(f'<path d="M {X(bi):.1f} {Y(b["pivot"])-6:.1f} l -6 11 l 12 0 z" '
                     f'fill="#1552a0"/>')
        # 스테이지 라벨 박스
        lx = X((li + bi) // 2)
        ly = Y(b["low"]) + 26
        label = f'{b["stage"]} · {b["type"]}'
        sub = f'{b["len_w"]}주 · −{b["depth"]}% · P {b["pivot"]}'
        wbox = max(len(label), len(sub)) * 6.6 + 22
        ly = place(lx - wbox / 2, ly, wbox, 34)
        parts.append(
            f'<g><rect x="{lx-wbox/2:.1f}" y="{ly:.1f}" width="{wbox:.1f}" height="34" rx="7" '
            f'fill="#fff" stroke="{TOK["arc"]}" stroke-width="1.2"/>'
            f'<text x="{lx:.1f}" y="{ly+14:.1f}" text-anchor="middle" font-size="11.5" '
            f'font-weight="700" fill="#1552a0">{_e(label)}</text>'
            f'<text x="{lx:.1f}" y="{ly+27:.1f}" text-anchor="middle" font-size="10" '
            f'fill="{TOK["mut"]}">{_e(sub)}</text></g>')

    # ── RS 라인 (하단 오버레이) ─────────────────────────────────────
    rs = it.get("rs") or []
    if rs:
        rmin, rmax = min(rs), max(rs)
        band_top, band_h = H - 210, 150
        RY = lambda r: band_top + (rmax - r) / max(rmax - rmin, 1e-9) * band_h
        # 52주 신고가 영역 = 굵게 (카카오톡 설명: "볼드선 구간은 rs가 신고가 영역")
        hi_flags = []
        for i in range(len(rs)):
            w0 = max(0, i - 252)
            hi_flags.append(rs[i] >= max(rs[w0:i + 1]) - 1e-12)
        seg, segs = [], []
        for i, f in enumerate(hi_flags):
            if f:
                seg.append(i)
            elif seg:
                segs.append(seg); seg = []
        if seg:
            segs.append(seg)
        pts = [f"{X(i):.1f},{RY(r):.1f}" for i, r in enumerate(rs)]
        parts.append(f'<polyline points="{" ".join(pts)}" fill="none" '
                     f'stroke="{TOK["rs"]}" stroke-width="1.4"/>')
        for sg in segs:
            if len(sg) < 2:
                continue
            p2 = [f"{X(i):.1f},{RY(rs[i]):.1f}" for i in sg]
            parts.append(f'<polyline points="{" ".join(p2)}" fill="none" '
                         f'stroke="{TOK["rs"]}" stroke-width="3.4"/>')
        # Blue Dot
        for d in (it.get("blue_dots") or []):
            i = idx.get(d)
            if i is not None:
                parts.append(f'<circle cx="{X(i):.1f}" cy="{RY(rs[i]):.1f}" r="3.1" '
                             f'fill="{TOK["rs"]}"/>')
        # 월별 RS 점수 마커 (참고본: RS 라인 위에 47·48·32·65 처럼 숫자 표기)
        marks = it.get("rs_marks") or []
        prev_sc = None
        for mk in marks:
            mi = idx.get(mk.get("d"))
            if mi is None:
                continue
            sc = mk.get("s")
            if sc == prev_sc:      # 99 99 99 처럼 같은 값 연속이면 생략
                continue
            prev_sc = sc
            col = TOK["rs"] if (sc or 0) >= 60 else TOK["down"]
            parts.append(f'<circle cx="{X(mi):.1f}" cy="{RY(rs[mi]):.1f}" r="2.2" fill="{col}"/>')
            parts.append(f'<text x="{X(mi):.1f}" y="{RY(rs[mi])-7:.1f}" text-anchor="middle" '
                         f'font-size="9.5" font-weight="600" fill="{col}">{sc}</text>')
        parts.append(f'<text x="{X(n-1)+8:.1f}" y="{RY(rs[-1])+4:.1f}" font-size="12.5" '
                     f'font-weight="800" fill="{TOK["rs"]}">RS {meta.get("rs_score","—")}</text>')

    # ── HTF (폴 화살선 + 깃발 박스) ─────────────────────────────────
    for f in (it.get("htf") or []):
        pi, fi = idx.get(f["pole_top_date"]), idx.get(f["flag_end"])
        si = idx.get(f["pole_start"])
        if pi is None or fi is None:
            continue
        if si is not None:   # 폴: 저점 → 고점 화살선
            parts.append(f'<line x1="{X(si):.1f}" y1="{Y(l[si]):.1f}" x2="{X(pi):.1f}" '
                         f'y2="{Y(f["pole_top"]):.1f}" stroke="#8b5cf6" stroke-width="2.2" '
                         f'marker-end="url(#htfArrow)"/>')
        # 깃발: 폴 고점 이후 조정 구간을 연보라 박스로
        y0 = Y(f["pole_top"]); y1_ = Y(f["pole_top"] * (1 - f["flag_depth"] / 100))
        parts.append(f'<rect x="{X(pi):.1f}" y="{y0:.1f}" width="{max(4,X(fi)-X(pi)):.1f}" '
                     f'height="{max(4,y1_-y0):.1f}" fill="#8b5cf6" opacity=".12" '
                     f'stroke="#8b5cf6" stroke-width="1" stroke-dasharray="3 3"/>')
        htxt = f'HTF +{f["pole_gain"]:.0f}% · P {f["pivot"]}'
        hy = place(X(pi), y0 - 18, len(htxt) * 5.6, 14)
        parts.append(f'<text x="{X(pi):.1f}" y="{hy+11:.1f}" font-size="10" font-weight="700" '
                     f'fill="#7c3aed">{_e(htxt)}</text>')

    # ── 3~4주 타이트 밴드 ───────────────────────────────────────────
    for tb in (it.get("tight") or []):
        a, b2 = idx.get(tb["start"]), idx.get(tb["end"])
        if a is None or b2 is None:
            continue
        ty = Y(tb["top"])
        parts.append(f'<rect x="{X(a):.1f}" y="{ty-16:.1f}" width="{max(6,X(b2)-X(a)):.1f}" '
                     f'height="16" fill="none" stroke="#7c5cd6" stroke-width="1.2" rx="3"/>')
        parts.append(f'<text x="{(X(a)+X(b2))/2:.1f}" y="{ty-19:.1f}" text-anchor="middle" '
                     f'font-size="9.5" font-weight="700" fill="#7c5cd6">{tb["weeks"]}주T</text>')

    # ── VCP 축소 표기 ───────────────────────────────────────────────
    vcp = it.get("vcp")
    if vcp:
        parts.append(f'<g><rect x="{PAD_L+14}" y="40" width="176" height="24" rx="6" '
                     f'fill="{TOK["amberBg"]}" stroke="#e6d3a8"/>'
                     f'<text x="{PAD_L+102}" y="56" text-anchor="middle" font-size="11.5" '
                     f'font-weight="700" fill="{TOK["amber"]}">'
                     f'VCP {vcp["n"]}축소 {vcp["from"]:.0f}→{vcp["to"]:.0f}%</text></g>')

    # ── ANTS ────────────────────────────────────────────────────────
    #   MVP 3조건(M·V·P) 충족 = 진한 큰 삼각형 / M+V 만 = 옅은 작은 삼각형
    #   클라이맥스(50일선 이격 과대) = 주황 역삼각형 ▽ 경고
    for a in (it.get("ants") or []):
        rec = a if isinstance(a, dict) else {"d": a, "kind": "weak", "mvp": False}
        i = idx.get(rec.get("d"))
        if i is None:
            continue
        y = Y(l[i]) + 12
        if rec.get("kind") == "climax":
            parts.append(f'<path d="M {X(i):.1f} {y+8:.1f} l -4.6 -8 l 9.2 0 z" '
                         f'fill="{TOK["amber"]}"/>')
        elif rec.get("mvp"):
            parts.append(f'<path d="M {X(i):.1f} {y:.1f} l -5 8 l 10 0 z" '
                         f'fill="{TOK["ema21"]}"/>')
        else:
            parts.append(f'<path d="M {X(i):.1f} {y+1:.1f} l -3.2 5.5 l 6.4 0 z" '
                         f'fill="{TOK["ema21"]}" opacity=".45"/>')

    # 마지막 종가 태그
    parts.append(f'<rect x="{W-PAD_R+4}" y="{Y(last)-11:.1f}" width="76" height="22" rx="4" fill="#1c2530"/>'
                 f'<text x="{W-PAD_R+42}" y="{Y(last)+4:.1f}" text-anchor="middle" font-size="12" '
                 f'font-weight="700" fill="#fff">{last:,.2f}</text>')

    # ── 거래량 ──────────────────────────────────────────────────────
    vparts: List[str] = []
    vmax = max(v) or 1
    VY = lambda val: VOL_H - 24 - (val / vmax) * (VOL_H - 46)
    for i in range(n):
        up = c[i] >= (c[i - 1] if i else o[i])
        col = "#9dbde0" if up else "#efaebd"
        vparts.append(f'<rect x="{X(i)-bw/2:.1f}" y="{VY(v[i]):.1f}" width="{bw:.1f}" '
                      f'height="{VOL_H-24-VY(v[i]):.1f}" fill="{col}"/>')
    vp = [f"{X(i):.1f},{VY(val):.1f}" for i, val in enumerate(vol50) if val]
    if len(vp) > 1:
        vparts.append(f'<polyline points="{" ".join(vp)}" fill="none" stroke="{TOK["ma50"]}" stroke-width="1.4"/>')

    # 날짜 눈금
    ticks = []
    step = max(1, n // 22)
    for i in range(0, n, step):
        dt = dates[i]
        ticks.append(f'<text x="{X(i):.1f}" y="{VOL_H-6}" text-anchor="middle" font-size="10.5" '
                     f'fill="{TOK["mut"]}">{dt[2:7].replace("-",".")}</text>')

    # ── 체인 카드 ───────────────────────────────────────────────────
    cells = []
    for k, b in enumerate(bases):
        arrow = '<span style="color:#9fb2c6;margin-right:6px">→</span>' if k else ""
        brk = f'돌파 {b["brk_date"]}' if b.get("brk_date") else "진행 중"
        pu = f' · 직전 {b["prior_up"]:+.1f}%' if b.get("prior_up") is not None else ""
        cells.append(
            f'<div class="cell"><div class="stg">{arrow}{_e(b["stage"])}</div>'
            f'<div class="typ">{_e(TYPE_KO.get(b["type"], b["type"]))}</div>'
            f'<div class="m">{b["len_w"]}주 · 깊이 −{b["depth"]}%</div>'
            f'<div class="m">피벗 {b["pivot"]}</div>'
            f'<div class="m">{_e(brk)}{_e(pu)}</div></div>')

    # ── 뱃지 ────────────────────────────────────────────────────────
    pf = sw.get("pct_from_pivot")
    chips = [
        f'<div class="chip solid">Weekly Stage {_e(sw.get("stage","—"))} · '
        f'{"돌파 완료" if sw.get("status")=="completed" else "형성 중"}</div>',
        f'<div class="chip">RS점수 {_e(meta.get("rs_score","—"))} · '
        f'{"최상위" if (meta.get("rs_score") or 0)>=90 else "강세" if (meta.get("rs_score") or 0)>=70 else "보통"} (vs S&amp;P500)</div>',
        f'<div class="chip">베이스 타입 {_e(sw.get("base_type","—"))} · '
        f'깊이 {_e(sw.get("base_depth_pct","—"))}% · {_e(sw.get("base_len_weeks","—"))}주</div>',
        f'<div class="chip">피벗 {_e(sw.get("pivot","—"))} · 피벗 대비 '
        f'{pf:+.1f}% ({"확장" if (pf or 0)>0 else "미달"})</div>' if pf is not None else "",
    ]
    if (sw.get("prior_uptrend_pct") is not None):
        chips.append(f'<div class="chip">직전 상승 {sw["prior_uptrend_pct"]:+.1f}%</div>')
    if (sw.get("stage_n") or (bases[-1]["stage_n"] if bases else 0)) >= 3:
        chips.append('<div class="chip warn">후기 스테이지(3rd+) — 신규 베이스 실패 확률 증가 (O\'Neil)</div>')

    d0 = dates[0][:10]
    return f"""<section class="wrap">
<div class="eyebrow">BASE COUNT CHART · MARKETSURGE-STYLE · DAILY</div>
<div class="hd">
  <div><div class="code">{_e(meta.get("symbol"))}</div>
  <div class="sub">US · 일봉 · 로그 스케일 · {_e(d0)} ~ {_e(dates[-1])}</div></div>
  <div><div class="px">{last:,.2f}</div>
  <div class="chg {'pos' if chg>=0 else 'neg'}">{chg:+.2f} ({chgp:+.2f}%)</div>
  <div class="asof">{_e(dates[-1])} 종가 · USD</div></div>
</div>
<div class="chips">{''.join(chips)}</div>
<div class="card">
  <div class="readout">{_e(dates[-1])} &nbsp; 시 <b>{o[-1]:,.2f}</b> &nbsp; 고 <b>{h[-1]:,.2f}</b>
   &nbsp; 저 <b>{l[-1]:,.2f}</b> &nbsp; 종 <b>{last:,.2f}</b> &nbsp; 거래량 <b>{_fmt_vol(v[-1])}</b></div>
  <svg viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">
    <defs><marker id="htfArrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5"
      orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="#8b5cf6"/></marker></defs>
    {''.join(parts)}</svg>
  <svg viewBox="0 0 {W} {VOL_H}" preserveAspectRatio="xMidYMid meet">
    <text x="{PAD_L}" y="16" font-size="11.5" fill="{TOK['mut']}">거래량 · 50일 평균선</text>
    {''.join(vparts)}{''.join(ticks)}</svg>
  <div class="legend">
    <span><i style="border-color:{TOK['ema21']}"></i>21일 EMA</span>
    <span><i style="border-color:{TOK['ma50']}"></i>50일선</span>
    <span><i style="border-color:{TOK['ma200']}"></i>200일선</span>
    <span><i style="border-color:{TOK['rs']}"></i>RS 라인 (대 S&amp;P500) · <b>굵은 구간 = RS 신고가 영역</b></span>
    <span><i style="border-color:{TOK['arc']}"></i>베이스 윤곽(컵) · 점선 = 피벗(매수점)</span>
    <span>▲ ANTS <b>MVP</b> (15일중 12↑ · 15일 +20% · 거래량 50일평균×1.2)</span>
    <span style="opacity:.55">▲ ANTS 약식 (M+V만)</span>
    <span style="color:#a86a14">▽ 클라이맥스 경고 (50일선 이격 과대)</span>
    <span>● Blue Dot (RS 52주 신고가)</span>
  </div>
</div>
<div class="chain-title">베이스 카운트 체인</div>
<div class="chain">{''.join(cells) if cells else '<div class="cell m">검출된 베이스 없음</div>'}</div>
<div class="note"><b>방법론</b> — 베이스 검출·카운팅은 자체 엔진 <b>base_count.py</b>(MarketSurge Weekly Stage 재현)로 계산했으며,
컵 윤곽과 스테이지 라벨은 검출된 좌측 고점 → 저점 → 돌파(피벗 +0.1)를 잇는 시각화입니다. MarketSurge 원본 차트가 아닙니다.
카운팅 규칙: 직전 피벗 대비 +20% 초과 상승 후 새 베이스 = 스테이지 N+1, 이하 = base-on-base(letter), 직전 저점 붕괴(undercut) = 1로 리셋.
RS 라인 = 종가/S&amp;P500 · RS점수 = 3/6/9/12개월 가중수익률(40/20/20/20)의 벤치마크 상대비를 1~99 로지스틱 매핑한 자체 점수(IBD 유니버스 백분위 아님).
생성 {datetime.now():%Y-%m-%d}.<br>
<b>RS 읽는 법</b> — RS 라인은 종가÷S&amp;P500 비율 원시값이라 <b>상한이 없고</b>, RS 점수는 1~99로 압축되어 상단에서 포화됩니다
(시장 대비 +40%≈87 · +100%≈98 · 그 이상은 99 고정). 따라서 <b>점수가 99인데 라인이 계속 오르는 것은 정상</b>이며 오히려 최강 신호입니다.
반대로 점수 99인데 라인이 옆으로 눕기 시작하면 상대강도 둔화 경고입니다.</div>
</section>"""
