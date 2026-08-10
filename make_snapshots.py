# -*- coding: utf-8 -*-
"""
make_snapshots.py — 종목 스냅샷 미니차트 생성기
=====================================================================
screener_data.json 유니버스(기본 RS≥70 + ETF + 전략실 보유)의 6~7개월
일봉을 yfinance 배치로 받아, 종목당 1개의 컴팩트 SVG 미니차트를 렌더해
snapshots.json 으로 내보낸다. 대시보드 종목 상세 패널이 lazy-load 해서
TradingView 위젯 아래(또는 위젯 차단 시 대체)로 표시한다.

  [SVG 구성 — dashboard_public 스냅샷과 동일 규격 390×294]
    가격 라인(추세색) + 50MA(빨강) + 200MA(회색) + 거래량 바 + 월 눈금
    + 마지막 종가 태그

  [사용법]
    python make_snapshots.py                          # ./screener_data.json
    python make_snapshots.py --from-json output/screener_data.json
    옵션: --min-rs 70 · --limit 1200 · --out snapshots.json · --months 7

  [출력]  snapshots.json:
    {"meta": {"asof": "...", "n": 1234},
     "AAPL": {"svg": "<svg .../>", "name": "...", "last": 123.45, "chg_pct": 1.2}, ...}
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime

W, H = 390, 294
PRICE_TOP, PRICE_BOT = 8, 168          # 가격 영역
VOL_TOP, VOL_BOT = 182, 270            # 거래량 영역
AXIS_Y = 292
UP, DN = "#3B6D11", "#A32D2D"
MA50_C, MA200_C = "#e0413c", "#3c4450"


def _sma(vals, n):
    out, run = [], 0.0
    for i, v in enumerate(vals):
        run += v
        if i >= n:
            run -= vals[i - n]
        out.append(run / n if i >= n - 1 else None)
    return out


def render_snapshot(dates, closes, vols):
    """일봉 → 컴팩트 SVG 문자열 (순수 함수 — 네트워크 없음)."""
    n = len(closes)
    if n < 30:
        return None
    lo, hi = min(closes), max(closes)
    pad = (hi - lo) * 0.06 or 1.0
    lo, hi = lo - pad, hi + pad
    X = lambda i: round(4 + i * (W - 8) / max(1, n - 1), 1)
    Y = lambda p: round(PRICE_TOP + (hi - p) / (hi - lo) * (PRICE_BOT - PRICE_TOP), 1)
    parts = []

    # 월 눈금
    ticks = []
    for i in range(1, n):
        if dates[i][5:7] != dates[i - 1][5:7]:
            ticks.append(i)
    for i in ticks:
        mm = int(dates[i][5:7])
        label = f"'{dates[i][2:4]}.{mm}" if mm == 1 else f"{mm}월"
        parts.append(f'<line x1="{X(i)}" y1="{PRICE_BOT}" x2="{X(i)}" y2="{VOL_BOT}" '
                     f'stroke="#eceae2" stroke-width="0.5"/>')
        parts.append(f'<text x="{X(i)}" y="{AXIS_Y}" text-anchor="middle" '
                     f'font-size="7.5" fill="#b4b2a9">{label}</text>')

    # 거래량 (컴팩트 path: M x,VOL_BOT V y — 바 하나당 세그먼트)
    vmax = max(vols) or 1
    vp = []
    for i in range(n):
        vh = (vols[i] / vmax) * (VOL_BOT - VOL_TOP)
        vp.append(f"M{X(i)} {VOL_BOT}V{round(VOL_BOT - vh, 1)}")
    parts.append(f'<path d="{"".join(vp)}" stroke="#d8dde4" stroke-width="1.4"/>')

    # 이동평균
    for series, col in ((_sma(closes, 50), MA50_C), (_sma(closes, 200), MA200_C)):
        pts = " ".join(f"{X(i)},{Y(v)}" for i, v in enumerate(series) if v is not None)
        if pts.count(" ") > 0:
            parts.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="1"/>')

    # 종가 라인 (추세색)
    col = UP if closes[-1] >= closes[0] else DN
    pts = " ".join(f"{X(i)},{Y(c)}" for i, c in enumerate(closes))
    parts.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="1.6"/>')
    parts.append(f'<circle cx="{X(n-1)}" cy="{Y(closes[-1])}" r="2" fill="{col}"/>')

    # 마지막 종가 태그
    ytag = max(PRICE_TOP + 8, min(PRICE_BOT - 4, Y(closes[-1])))
    parts.append(f'<text x="{W-4}" y="{ytag}" text-anchor="end" font-size="9" '
                 f'font-weight="700" fill="{col}">{closes[-1]:,.2f}</text>')

    return (f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
            f'style="border-radius:6px;border:0.5px solid #e0dfd8;display:block;'
            f'background:#fafaf8;max-width:100%">'
            f'<line x1="0" y1="{VOL_BOT+3}" x2="{W}" y2="{VOL_BOT+3}" '
            f'stroke="#e8e7e0" stroke-width="0.5"/>' + "".join(parts) + "</svg>")


def pick_universe(sd, min_rs, limit):
    rows = sd.get("stocks", [])
    stocks = [s for s in rows if s.get("asset_type") == "STOCK"
              and isinstance(s.get("rs"), (int, float)) and s["rs"] >= min_rs]
    etfs = [s for s in rows if s.get("asset_type") == "ETF"]
    stocks.sort(key=lambda s: -(s.get("rs") or 0))
    # 전략실 보유·신호 종목은 RS 무관 항상 포함
    extra = []
    try:
        srj = json.load(open("strategy_room.json", encoding="utf-8"))
        keep = {h.get("ticker") for h in (srj.get("holdings") or [])}
        keep |= {g.get("ticker") for g in (srj.get("signals") or [])}
        byt = {s.get("ticker"): s for s in rows}
        extra = [byt[t] for t in keep if t in byt]
    except Exception:
        pass
    seen, uni = set(), []
    for s in extra + stocks[:limit] + etfs:
        t = s.get("ticker")
        if t and t not in seen:
            seen.add(t)
            uni.append(s)
    return uni[:limit + len(etfs) + len(extra)]


def fetch_batch(tickers, months):
    """yfinance 배치 다운로드 → {ticker: (dates, closes, vols)}."""
    import yfinance as yf
    out = {}
    CH = 50
    for k in range(0, len(tickers), CH):
        chunk = tickers[k:k + CH]
        try:
            df = yf.download(" ".join(chunk), period=f"{months}mo", interval="1d",
                             auto_adjust=True, group_by="ticker",
                             threads=True, progress=False)
        except Exception as e:
            print(f"  ! 배치 {k//CH+1} 실패: {e}")
            continue
        for tk in chunk:
            try:
                sub = df[tk] if len(chunk) > 1 else df
                sub = sub.dropna(subset=["Close"])
                if len(sub) < 30:
                    continue
                out[tk] = ([d.strftime("%Y-%m-%d") for d in sub.index],
                           [float(x) for x in sub["Close"]],
                           [float(x) for x in sub["Volume"]])
            except Exception:
                continue
        print(f"  [{min(k+CH, len(tickers))}/{len(tickers)}] 수집 {len(out)}종목")
        time.sleep(1.0)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-json", dest="from_json", default="screener_data.json")
    ap.add_argument("--min-rs", type=int, default=70)
    ap.add_argument("--limit", type=int, default=1200)
    ap.add_argument("--months", type=int, default=7)
    ap.add_argument("--out", default="snapshots.json")
    a = ap.parse_args()

    with open(a.from_json, encoding="utf-8") as f:
        sd = json.load(f)
    uni = pick_universe(sd, a.min_rs, a.limit)
    print(f"유니버스 {len(uni)}종목 (RS≥{a.min_rs} + ETF + 보유/신호)")

    data = fetch_batch([s["ticker"] for s in uni], a.months)
    meta_by = {s["ticker"]: s for s in uni}
    out = {"meta": {"asof": (sd.get("meta") or {}).get("updated_at", "")[:10]
                    or datetime.now().strftime("%Y-%m-%d"),
                    "n": 0, "min_rs": a.min_rs}}
    for tk, (dates, closes, vols) in data.items():
        svg = render_snapshot(dates, closes, vols)
        if not svg:
            continue
        s = meta_by.get(tk, {})
        out[tk] = {"svg": svg, "name": s.get("name") or s.get("company") or "",
                   "last": round(closes[-1], 2),
                   "chg_pct": round((closes[-1] / closes[-2] - 1) * 100, 2) if len(closes) > 1 else None}
        out["meta"]["n"] += 1

    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    kb = os.path.getsize(a.out) // 1024
    print(f"✅ {a.out} — {out['meta']['n']}종목 · {kb:,} KB")
    if out["meta"]["n"] < len(uni) * 0.5:
        print("⚠ 수집률 50% 미만 — yfinance rate limit 가능성, 재실행 권장")
        sys.exit(1)


if __name__ == "__main__":
    main()
