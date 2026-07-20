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
        # 컵 윤곽 (좌측고점 → 저점 → 돌파)
        parts.append(
            f'<path d="M {X(li):.1f} {Y(b["left_high"]):.1f} '
            f'Q {X((li+lo_i)//2):.1f} {Y(b["low"]):.1f} {X(lo_i):.1f} {Y(b["low"]):.1f} '
            f'Q {X((lo_i+bi)//2):.1f} {Y(b["low"]):.1f} {X(bi):.1f} {Y(b["pivot"]):.1f}" '
            f'fill="none" stroke="{TOK["arc"]}" stroke-width="2.4" opacity=".85"/>')
        # 피벗 점선
        parts.append(f'<line x1="{X(li):.1f}" y1="{Y(b["pivot"]):.1f}" x2="{X(bi):.1f}" '
                     f'y2="{Y(b["pivot"]):.1f}" stroke="{TOK["arc"]}" stroke-width="1.2" '
                     f'stroke-dasharray="5 4"/>')
        # 돌파 화살표
        parts.append(f'<path d="M {X(bi):.1f} {Y(b["pivot"])-6:.1f} l -6 11 l 12 0 z" '
                     f'fill="#1552a0"/>')
        # 스테이지 라벨 박스
        lx, ly = X((li + bi) // 2), Y(b["low"]) + 26
        label = f'{b["stage"]} · {b["type"]}'
        sub = f'{b["len_w"]}주 · −{b["depth"]}% · P {b["pivot"]}'
        wbox = max(len(label), len(sub)) * 6.6 + 22
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
        parts.append(f'<text x="{X(n-1)+8:.1f}" y="{RY(rs[-1])+4:.1f}" font-size="12.5" '
                     f'font-weight="800" fill="{TOK["rs"]}">RS {meta.get("rs_score","—")}</text>')

    # Ants (삼각형)
    for d in (it.get("ants") or []):
        i = idx.get(d)
        if i is not None:
            y = Y(l[i]) + 12
            parts.append(f'<path d="M {X(i):.1f} {y:.1f} l -4.2 7 l 8.4 0 z" fill="{TOK["ema21"]}"/>')

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
    step = max(1, n // 14)
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
  <svg viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">{''.join(parts)}</svg>
  <svg viewBox="0 0 {W} {VOL_H}" preserveAspectRatio="xMidYMid meet">
    <text x="{PAD_L}" y="16" font-size="11.5" fill="{TOK['mut']}">거래량 · 50일 평균선</text>
    {''.join(vparts)}{''.join(ticks)}</svg>
  <div class="legend">
    <span><i style="border-color:{TOK['ema21']}"></i>21일 EMA</span>
    <span><i style="border-color:{TOK['ma50']}"></i>50일선</span>
    <span><i style="border-color:{TOK['ma200']}"></i>200일선</span>
    <span><i style="border-color:{TOK['rs']}"></i>RS 라인 (대 S&amp;P500) · <b>굵은 구간 = RS 신고가 영역</b></span>
    <span><i style="border-color:{TOK['arc']}"></i>베이스 윤곽(컵) · 점선 = 피벗(매수점)</span>
    <span>▲ Ants (매집: 15봉중 12↑·거래량↑)</span>
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
