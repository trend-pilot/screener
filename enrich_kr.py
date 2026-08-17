# -*- coding: utf-8 -*-
"""
enrich_kr.py — 스탁이지 종목의 일봉 파생지표 생성 (Phase 1)
================================================================================
stockeasy_kr.json 의 보유/이탈 종목만 pykrx 로 일봉을 받아, 전략실 엔진이
요구하는 파생 필드를 계산해 kr_ohlcv_extra.json 으로 저장한다.

왜 별도 파일인가:
  kr_screener_output.json 에 직접 써넣으면 스크리너를 다시 돌릴 때 날아간다.
  엔진이 두 파일을 병합해 쓰는 구조가 안전하다.

생성 필드:
  ma50 / ma200        청산 ④-2(50일선 이탈) · O'Neil 규칙3(200일선)
  vol_ratio_50d       RVOL 하드게이트 (진입 ≥1.5)
  high_52w / h52_pct  피벗 거리 가드 (-10% ~ +5%)
  ma50_break(_vol)    50일선 이탈 · 거래량 동반 여부
  dist_days_25        분배일 누적

실행:
    pip install pykrx
    python enrich_kr.py
    python enrich_kr.py --days 400 --sleep 0.3
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

try:
    from pykrx import stock
except ImportError:
    sys.exit("pykrx 가 필요합니다:  pip install pykrx")


# ─── 파생지표 계산 (순수 함수 — pykrx 없이 테스트 가능) ──────────────
def _sma(vals, n):
    if not vals or len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def compute_metrics(closes, highs, lows, vols, w52=252, dist_win=25):
    if not closes:
        return {}
    px = closes[-1]
    ma50 = _sma(closes, 50)
    ma200 = _sma(closes, 200)
    vol50 = _sma(vols, 50)
    h52 = max(highs[-w52:]) if highs else None
    l52 = min(lows[-w52:]) if lows else None

    dist = 0
    win = min(dist_win, len(closes) - 1)
    for i in range(len(closes) - win, len(closes)):
        if i < 1:
            continue
        chg = (closes[i] / closes[i - 1] - 1) * 100
        if chg <= -0.2 and vols[i] > vols[i - 1]:
            dist += 1

    return {
        "price": round(px, 2),
        "ma50": round(ma50, 2) if ma50 else None,
        "ma200": round(ma200, 2) if ma200 else None,
        "vol_ratio_50d": round(vols[-1] / vol50, 2) if vol50 else None,
        "high_52w": round(h52, 2) if h52 else None,
        "low_52w": round(l52, 2) if l52 else None,
        "h52_pct": round((px / h52 - 1) * 100, 2) if h52 else None,
        "ma50_dist_pct": round((px / ma50 - 1) * 100, 2) if ma50 else None,
        "ma50_break": (px < ma50) if ma50 else None,
        "ma50_break_vol": (px < ma50 and vol50 is not None and vols[-1] > vol50)
                          if ma50 else None,
        "above_ma200": (px >= ma200) if ma200 else None,
        "dist_days_25": dist,
        "bars": len(closes),
    }


# ─── pykrx 수집 ──────────────────────────────────────────────────────
def fetch_ohlcv(ticker, days):
    """최근 days 달력일의 일봉. 실패 시 None."""
    end = date.today()
    start = end - timedelta(days=days)
    df = stock.get_market_ohlcv(start.strftime("%Y%m%d"),
                                end.strftime("%Y%m%d"), ticker)
    if df is None or df.empty:
        return None
    # 거래정지일은 거래량 0 + 종가 유지로 들어온다 — 종가 0 인 행만 제거
    df = df[df["종가"] > 0]
    if df.empty:
        return None
    return {
        "dates": [d.strftime("%Y-%m-%d") for d in df.index],
        "closes": [float(v) for v in df["종가"]],
        "highs": [float(v) for v in df["고가"]],
        "lows": [float(v) for v in df["저가"]],
        "vols": [float(v) for v in df["거래량"]],
    }


def load_targets(path):
    """stockeasy_kr.json → [(ticker, name)] · 코드 없는 종목은 제외."""
    if not os.path.exists(path):
        sys.exit(f"[!] {path} 없음 — stockeasy_fetch.py 를 먼저 실행하세요.")
    with open(path, encoding="utf-8") as f:
        snap = json.load(f)
    seen, out = set(), []
    for bucket in ("holdings", "exits"):
        for r in snap.get(bucket, []):
            t = r.get("ticker")
            if t and t not in seen:
                seen.add(t)
                out.append((t, r.get("name", "")))
    return out, snap.get("data_date")


# ─── main ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="stockeasy_kr.json")
    ap.add_argument("--out", default="kr_ohlcv_extra.json")
    ap.add_argument("--days", type=int, default=400,
                    help="받아올 달력일 (252거래일 확보하려면 380일 이상)")
    ap.add_argument("--sleep", type=float, default=0.25,
                    help="종목간 대기 — KRX 부하 방지")
    a = ap.parse_args()

    targets, data_date = load_targets(a.src)
    if not targets:
        sys.exit("[!] 대상 종목 없음 — 종목코드 매핑을 확인하세요.")
    print(f"[대상] {len(targets)}종목 · 기준일 {data_date}")

    data, failed, thin = {}, [], []
    for i, (t, name) in enumerate(targets, 1):
        try:
            ohlcv = fetch_ohlcv(t, a.days)
        except Exception as e:
            print(f"  [{i:>2}/{len(targets)}] {t} {name} — 수집 실패: {e}")
            failed.append(f"{t} {name}")
            time.sleep(a.sleep)
            continue
        if not ohlcv:
            print(f"  [{i:>2}/{len(targets)}] {t} {name} — 데이터 없음")
            failed.append(f"{t} {name}")
            time.sleep(a.sleep)
            continue

        m = compute_metrics(ohlcv["closes"], ohlcv["highs"],
                            ohlcv["lows"], ohlcv["vols"])
        m["name"] = name
        m["last_date"] = ohlcv["dates"][-1]
        data[t] = m
        if m.get("ma50") is None:
            thin.append(f"{t} {name}({m['bars']}봉)")
        print(f"  [{i:>2}/{len(targets)}] {t} {name:<14} "
              f"{m['bars']:>3}봉 · ma50 {m['ma50']} · RVOL {m['vol_ratio_50d']}")
        time.sleep(a.sleep)

    out = {"as_of": date.today().isoformat(),
           "src_data_date": data_date,
           "generated_at": datetime.now().isoformat(timespec="seconds"),
           "n": len(data), "failed": failed, "thin": thin,
           "data": data}
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"\n[out] {a.out} · {len(data)}종목")
    if failed:
        print(f"  ⚠ 수집 실패 {len(failed)}: {', '.join(failed[:5])}")
    if thin:
        print(f"  ⚠ 이력 부족(ma50 계산 불가) {len(thin)}: {', '.join(thin[:5])}")

    # ── 오늘 엔진이 무엇을 할지 미리보기 ──────────────────────────
    brk = [(t, m) for t, m in data.items() if m.get("ma50_break")]
    brk_v = [x for x in brk if x[1].get("ma50_break_vol")]
    hot = [(t, m) for t, m in data.items()
           if (m.get("vol_ratio_50d") or 0) >= 1.5]
    piv = [(t, m) for t, m in data.items()
           if m.get("h52_pct") is not None and -10 <= m["h52_pct"] <= 5]
    dd = [(t, m) for t, m in data.items() if m.get("dist_days_25", 0) >= 4]

    print("\n" + "─" * 56)
    print("전략실 규칙 프리뷰 (아직 발주 아님)")
    print("─" * 56)
    print(f"  50일선 이탈           {len(brk):>2}종목  (거래량 동반 {len(brk_v)})")
    for t, m in brk[:8]:
        flag = " ⚠거래량동반" if m.get("ma50_break_vol") else ""
        print(f"      {m['name']:<14} {m['ma50_dist_pct']:>6.1f}%{flag}")
    print(f"  RVOL ≥ 1.5 (진입 통과) {len(hot):>2}종목")
    print(f"  피벗 거리 -10~+5%      {len(piv):>2}종목")
    print(f"  분배일 4개 이상        {len(dd):>2}종목")
    for t, m in dd[:5]:
        print(f"      {m['name']:<14} {m['dist_days_25']}개")


if __name__ == "__main__":
    main()
