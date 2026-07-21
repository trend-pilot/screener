# -*- coding: utf-8 -*-
"""
make_chart_deck.py — 베이스 카운트 차트덱 실행 스크립트
=====================================================================
base_count.py(분석) + chart_deck.py(렌더) 를 묶어 실제 HTML 을 만든다.

[사용법]
    # 티커 직접 지정
    python make_chart_deck.py AAPL NVDA PLTR

    # screener_data.json 에서 조건에 맞는 종목 자동 선별
    python make_chart_deck.py --from-json output/screener_data.json --top 15
    python make_chart_deck.py --from-json output/screener_data.json --mode rs-accel --top 21
    python make_chart_deck.py --from-json output/screener_data.json --mode phase4 --top 15

    # 옵션
    --out 파일명.html     (기본: 차트덱_YYYY-MM-DD.html)
    --years 2            (기본 2년치 일봉)
    --min-rs 80          --from-json 사용 시 RS 하한 (기본 80)

[출력]
    단일 HTML 파일. 브라우저로 열면 종목별 차트가 세로로 나열된다.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base_count import (analyze, compute_ants, compute_blue_dots,  # noqa: E402
                        compute_htf, compute_rs_score, compute_tight_bands,
                        compute_vcp, sma)
from chart_deck import build_deck  # noqa: E402

BENCH = "SPY"          # RS 라인 기준 (차트덱 방법론: 종가/S&P500)


def fetch(ticker: str, years: int):
    """일봉 OHLCV 로드. 실패 시 None."""
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=int(365.25 * years) + 40)
    try:
        df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
    except Exception as e:
        print(f"  ! {ticker} 다운로드 실패: {e}")
        return None
    if df is None or len(df) < 260:
        print(f"  ! {ticker} 데이터 부족 ({0 if df is None else len(df)}일)")
        return None
    return {
        "dates": [d.strftime("%Y-%m-%d") for d in df.index],
        "o": [float(x) for x in df["Open"]],
        "h": [float(x) for x in df["High"]],
        "l": [float(x) for x in df["Low"]],
        "c": [float(x) for x in df["Close"]],
        "v": [float(x) for x in df["Volume"]],
    }


def align(stock: dict, bench: dict):
    """벤치마크를 종목 날짜에 맞춰 정렬 → RS 라인(종가/벤치마크) 생성."""
    bmap = dict(zip(bench["dates"], bench["c"]))
    bc_, keep = [], []
    last = None
    for i, d in enumerate(stock["dates"]):
        val = bmap.get(d, last)
        if val is None:
            continue
        last = val
        bc_.append(val)
        keep.append(i)
    if len(keep) != len(stock["dates"]):
        for k in ("dates", "o", "h", "l", "c", "v"):
            stock[k] = [stock[k][i] for i in keep]
    rs_line = [round(c / b, 6) for c, b in zip(stock["c"], bc_)]
    return bc_, rs_line


def _rs_accelerating(s: dict) -> bool:
    """RS 라인 가속 = 최근 구간일수록 상대강도 개선폭이 큼 (1주 > 3주 > 6주)."""
    a, b, c = s.get("rs_line_1w"), s.get("rs_line_3w"), s.get("rs_line_6w")
    return None not in (a, b, c) and a > b > c


def pick_from_json(path: str, top: int, min_rs: int, mode: str = "stage2"):
    """
    screener_data.json 에서 차트를 그릴 종목을 선별한다.

    mode
      stage2    (기본) RS 상위 + 추세템플릿 Stage2 — 정석 진입 후보
      rs-accel  RS 라인 가속(1w>3w>6w) + RS 라인 52주 신고가
                → 참고 덱 '21종목' 의 선별 기준
      phase4    베이스 단계 Phase 4 (돌파 임박/직후)
                → 참고 덱 '15종목' 의 선별 기준.
                  주의: phase 값은 '01','2','3','4','4plus','5','67' 문자열이라
                  '4' 와 '4plus' 를 함께 잡아야 한다.
      all       필터 없이 RS 상위
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = [s for s in data.get("stocks", []) if s.get("asset_type") == "STOCK"]

    if mode == "rs-accel":
        sel = [s for s in rows if _rs_accelerating(s) and s.get("rs_line_high")]
        key = lambda s: (s.get("rs_line_score") or 0, s.get("rs") or 0)
    elif mode == "phase4":
        sel = [s for s in rows if str(s.get("phase")) in ("4", "4plus")]
        key = lambda s: (s.get("rs") or 0, s.get("rs_line_score") or 0)
    elif mode == "all":
        sel = [s for s in rows if (s.get("rs") or 0) >= min_rs]
        key = lambda s: (s.get("rs") or 0, s.get("rs_line_score") or 0)
    else:  # stage2
        sel = [s for s in rows
               if (s.get("rs") or 0) >= min_rs and s.get("is_stage2")]
        key = lambda s: (s.get("rs_line_score") or 0, s.get("rs") or 0)

    sel.sort(key=key, reverse=True)
    picked = [s["ticker"] for s in sel[:top]]
    print(f"[{mode}] 조건 충족 {len(sel)}종목 → 상위 {len(picked)}종목 선별")
    if picked:
        print("   " + ", ".join(picked))
    return picked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*", help="티커 목록")
    ap.add_argument("--from-json", dest="from_json")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--min-rs", type=int, default=80)
    ap.add_argument("--mode", default="stage2",
                    choices=["stage2", "rs-accel", "phase4", "all"],
                    help="선별 기준 (--from-json 사용 시)")
    ap.add_argument("--years", type=int, default=2)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    tickers = list(a.tickers)
    if a.from_json:
        tickers += pick_from_json(a.from_json, a.top, a.min_rs, a.mode)
    if not tickers:
        ap.error("티커를 지정하거나 --from-json 을 사용하세요")

    print(f"벤치마크({BENCH}) 로드 중...")
    bench = fetch(BENCH, a.years)
    if not bench:
        print("벤치마크 로드 실패 — 중단"); sys.exit(1)

    items = []
    for i, tk in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] {tk}")
        s = fetch(tk, a.years)
        if not s:
            continue
        bench_c, rs_line = align(s, bench)
        res = analyze(tk, s["dates"], s["o"], s["h"], s["l"], s["c"], s["v"])
        last_base = res["bases"][-1] if res["bases"] else None
        # VCP: 마지막 베이스 안에서 먼저 찾고, 없으면 전 구간에서 탐색
        vcp = (compute_vcp(s["dates"], s["h"], s["l"], s["c"], last_base)
               or compute_vcp(s["dates"], s["h"], s["l"], s["c"]))
        # 월별 RS 점수 마커 — 각 달의 마지막 거래일에 그 시점 RS 점수를 표기
        rs_marks = []
        for k in range(1, len(s["dates"])):
            if s["dates"][k][:7] != s["dates"][k - 1][:7] and k >= 260:
                sc = compute_rs_score(s["c"][:k], bench_c[:k])
                if sc is not None:
                    rs_marks.append({"d": s["dates"][k - 1], "s": sc})
        items.append({
            "meta": {
                "symbol": tk,
                "rs_score": compute_rs_score(s["c"], bench_c),
                "stage_weekly": res["stage_weekly"],
            },
            **s,
            "rs": rs_line,
            "bases": res["bases"],
            # ANTS: 참고 차트덱과 동일하게 M+V 기준으로 표시하되,
            #   원전 MVP(P +20% 포함) 충족분은 mvp=True 로 구분해 크게 그린다.
            "ants": compute_ants(s["dates"], s["c"], s["v"],
                                 ma50=sma(s["c"], 50), strict=False),
            "blue_dots": compute_blue_dots(s["dates"], rs_line),
            "rs_marks": rs_marks,
            "htf": compute_htf(s["dates"], s["h"], s["l"], s["c"]),
            "tight": compute_tight_bands(s["dates"], s["h"], s["l"], s["c"]),
            "vcp": vcp,
            "vcp_points": (vcp or {}).get("points"),
        })
        time.sleep(0.25)

    if not items:
        print("생성할 종목이 없습니다"); sys.exit(1)

    out = a.out or f"차트덱_{len(items)}종목_{date.today():%Y-%m-%d}.html"
    html = build_deck(items, f"베이스 카운트 차트덱 · {len(items)}종목")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n✅ 완료 → {out}  ({len(html)//1024:,} KB · {len(items)}종목)")
    for it in items:
        sw = it["meta"]["stage_weekly"] or {}
        print(f"   {it['meta']['symbol']:<7} RS {str(it['meta']['rs_score']):>3} · "
              f"Stage {sw.get('stage','—'):<9} · 베이스 {len(it['bases'])}개")


if __name__ == "__main__":
    main()
