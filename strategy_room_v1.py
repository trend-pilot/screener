# -*- coding: utf-8 -*-
"""
strategy_room_v1.py — 전략실 forward 페이퍼 트레이딩 엔진 (v1)
================================================================================
명세 STRATEGY_ROOM_LOGIC.md(v6.20) 를, 현재 screener_data.json 이 제공하는
필드 한도 내에서 구현한 1차 버전.

【구현됨】
  - 5게이트 AND: G0(시장 레짐) · G1(발화 테마) · G2(패턴) · G3(RS 가속+신고가) · G4(종합점수+추세)
  - RVOL 하드게이트 (§1-B, entry_min_rvol=1.5 — vol_ratio_50d 기반)
  - 재진입 정책 v6.20: 종목약화 손절만 10영업일 차단, 시장동반 손절은 즉시 재진입 허용
    (판별은 청산 시점 레짐 프록시 — STRATEGY_ROOM_LOGIC.md 원문 확보 시 대조 필요)
  - FTD(Follow-Through Day) 유효기간 레짐 (2026-07-21 대시보드 가이드 ⑦):
    yfinance 로 ^GSPC·^IXIC·^RUT 를 받아 반등시도/FTD 를 직접 탐지.
    골든윈도우(FTD 후 ≤7거래일) → Resumed 가 Under Pressure 선점 (DD 8+ Correction 은 유지),
    8~21거래일 → 보호 없음, >21거래일 → 유효창 만료(레짐 판정에서 제외).
    yfinance 불가/실패 시 현행(브레드스+분산일) 로직으로 자동 폴백. 결과는 market_ftd 로 출력.
  - 스위칭(weed the garden): cap 가득 시 최약 보유 vs 신규 강신호 total_score 차 ≥12점 교체
    (winner +10%·grace 10bd·8주룰·런너 보호, 일일 ≤2)
  - 런너 cap 5 강제 (초과 시 승격 보류 — 총 최대 17 유지)
  - total_score 랭킹 (Comp는 rs percentile로 대체)
  - 진입: 균등가중 · 12슬롯 + 런너 제외 · 레짐 사이징(진입개수/노출)
  - 보유: Lock 7단계 래칫 · 8주룰 · max-hold(40bd) · 스위칭(weed the garden)
  - 런너: Tier1(+100%) · Tier2(추세 +40%/giveback)
  - 회계: entry_px 불변 · avg_cost 손익 · NAV = cash + 평가액
  - forward 누적: 전일 strategy_room.json 읽어 당일 반영 → 재기록

【v2.0 활성 — earnings.json (snapshots 워크플로우 --earnings) 존재 시】
  - 실적 임박 가드 (D-7 이내 신규 진입 금지) · 실적 D-3 R-tier 부분정리 (손실 전량 / 0~2R ⅔ / 2~4R ⅓ / ≥4R 보유)

【v1 비활성 (데이터 없음 — screener.py 보강 시 활성)】
  - 실적 후 재매수 룰
  - 피라미딩 · 클라이맥스 트림 (v6.20 확장)
  - 벤치마크 NAV(SPY/QQQ/TQQQ) — 별도 yfinance 단계 필요 (TODO)

실행:  python strategy_room_v1.py  [screener_data.json]  [strategy_room.json]
출력:  strategy_room.json  (대시보드 window.__STRATEGY_ROOM 이 그대로 읽음)
"""

import json
import math, os, sys
from datetime import date, datetime

# ─── 파라미터 (명세 §5) ───────────────────────────────────────────────
RULES = {
    "initial_capital": 1.0,
    "stop_pct": -7.0,
    "be_threshold_pct": 15.0,
    "lock_tiers": [[25,10],[50,25],[100,70],[200,150],[300,250],[400,350],[500,400]],
    "hold_days": 40,
    "oneill_threshold_pct": 20.0, "oneill_trigger_bdays": 15, "oneill_hold_bdays": 40,
    "max_concurrent_positions": 12,
    "switch_edge_min": 12.0, "switch_protect_gain_pct": 10.0, "switch_max_per_day": 2, "switch_grace_bdays": 10,
    "runner_promote_gain_pct": 100.0, "runner_cap": 5,
    "runner_trend_min_gain_pct": 40.0, "runner_trend_giveback_max": 0.30,
    # 셀 시그널: 진입 후 grace 기간이 지나면 50일선 이탈 시 전량 청산.
    #   운용로직 v6.15 ④-2. (RS 다이버전스는 스크리너에 필드가 없어 미구현)
    "sell_grace_bdays": 10, "sell_ma50_break": True,
    # RVOL 하드게이트 (명세 §1-B): 진입 당일 거래량이 50일 평균의 1.5배 이상.
    #   "거래량 확인 없는 돌파는 가짜 돌파" — 게이트 통과 후 진입 직전에 거른다.
    "entry_min_rvol": 1.5,
    # 재진입 정책 v6.20: 종목약화 손절 후 이 기간 동안 같은 종목 재진입 차단.
    #   시장동반 손절(청산 당시 레짐이 pressure/rally/correction)은 차단하지 않음 —
    #   검증 리포트: "중간 베어에 −7% 손절로 전부 잘림 → 재진입이 필수 메커니즘".
    "reentry_block_bdays": 10,
    # 실적 룰 (v2.0 — earnings.json 활성 시): 임박 D-7 이내 신규 진입 금지,
    #   D-3 이내 R구간 부분정리 (손실 전량 / 0~2R ⅔ / 2~4R ⅓ / 4R+ 보유. R = gain/7%)
    "earnings_entry_guard_bdays": 7, "earnings_trim_bdays": 3,
    "cost_bps": 5.0,
    "track": "g3+g0+g1+g2", "logic_version": "v6.15(v2.0)",
}
GATES = ["G3 Trigger","G4 Guard","G0 Market","G1 Theme","G2 Pattern"]
# 레짐 사이징 (G0; screener market.overall = green/yellow/red 3단계로 단순화)
# ── 마켓펄스 5단계 레짐 (STRATEGY_ROOM_LOGIC.md §1-D) ────────────────
#   기존엔 screener 의 market.overall(green/yellow/red)을 그대로 썼는데,
#   그 값은 브레드스만 보는 데다 red 가 "조정"이 아니라 "과열(Euphoria)"이라
#   의미가 어긋나 있었다. 대시보드와 동일한 5단계로 통일한다.
REGIME = {
    "confirmed":  {"label":"🟢 Confirmed Uptrend",     "max_exp":1.00, "entries":99},
    "resumed":    {"label":"🟢 Uptrend Resumed",       "max_exp":1.00, "entries":99},
    "pressure":   {"label":"🟡 Uptrend Under Pressure","max_exp":0.50, "entries":2},
    "rally":      {"label":"🟠 Rally Attempt",         "max_exp":0.30, "entries":1},
    "correction": {"label":"🔴 Market in Correction",  "max_exp":0.25, "entries":0},
}
_REGIME_ORDER = ["correction", "rally", "pressure", "resumed", "confirmed"]

# ─── FTD (Follow-Through Day) — 대시보드 가이드 ①~⑦ 규칙 그대로 ──────
#   ① 저점 다음 첫 양봉 = d1  ② d1~d3 은 너무 이른 반등이라 제외
#   ③ FTD = d4 이후 종가 +임계% 이상 + 거래량 전일보다 증가
#   ④ 임계: S&P +1.25% / NASDAQ·Russell +1.50%
#   ⑤ 반등 시작 저점 붕괴 → FTD 무효 (최신 저점 기준으로 사이클 재시작)
#   ⑥ d7 크게 넘긴 후발 FTD 는 late 표시
#   ⑦ 유효 기간(거래일): ≤7 골든윈도우 / 8~21 골든 지남 / >21 만료
FTD_THRESHOLDS = {"sp": 1.25, "nasdaq": 1.50, "russell": 1.50}
FTD_TICKERS = {"sp": ("^GSPC", "S&P 500"), "nasdaq": ("^IXIC", "NASDAQ"),
               "russell": ("^RUT", "Russell 2000")}
FTD_GOLDEN_BD = 7          # 골든윈도우 (적극 진입 보호 구간)
FTD_VALID_BD = 21          # 유효창 — 넘기면 레짐 판정에서 제외
FTD_LOOKBACK = 140         # 저점 탐색 구간 (거래일)


def detect_ftd(dates, closes, lows, vols, thr_pct):
    """
    단일 지수의 현재 사이클 FTD 상태를 계산한다 (마지막 저점 기준).
    반환: {status: none|rally|ftd, rally_low, rally_low_date, rally_day,
           ftd_date, ftd_day, ftd_gain_pct, ftd_vol_chg_pct,
           age_bd, window: golden|stale|expired|None, late}
    """
    n = len(closes)
    if n < 10:
        return None
    s = max(0, n - FTD_LOOKBACK)
    # 반등 시작 저점 = 구간 내 최저 저가 (이후 더 낮은 저점이 생기면
    # 자동으로 그 저점 기준 사이클로 바뀜 = ⑤ 무효화 규칙과 동치)
    lo_i = min(range(s, n), key=lambda i: lows[i])
    out = {"rally_low": round(lows[lo_i], 2), "rally_low_date": dates[lo_i],
           "status": "none", "rally_day": None, "ftd_date": None, "ftd_day": None,
           "ftd_gain_pct": None, "ftd_vol_chg_pct": None,
           "age_bd": None, "window": None, "late": False}
    # d1 = 저점 다음 첫 양봉(전일 대비 상승 마감)
    d1_i = None
    for i in range(lo_i + 1, n):
        if closes[i] > closes[i - 1]:
            d1_i = i
            break
    if d1_i is None:
        return out                      # 아직 반등 시도 없음 (하락 지속)
    out["status"] = "rally"
    out["rally_day"] = n - 1 - d1_i + 1     # 오늘이 d몇일차인가
    # FTD 탐색: d4 이후, 종가 +thr% & 거래량 전일 초과
    ftd_i = None
    for i in range(d1_i, n):
        day_no = i - d1_i + 1
        if day_no < 4:
            continue
        if closes[i - 1] <= 0 or vols[i - 1] <= 0:
            continue
        gain = (closes[i] / closes[i - 1] - 1) * 100
        if gain >= thr_pct and vols[i] > vols[i - 1]:
            age = n - 1 - i
            ftd_i = i
            out.update({
                "status": "ftd", "ftd_date": dates[i], "ftd_day": day_no,
                "ftd_gain_pct": round(gain, 2),
                "ftd_vol_chg_pct": round((vols[i] / vols[i - 1] - 1) * 100, 1),
                "age_bd": age, "late": day_no > 7,
                "window": ("golden" if age <= FTD_GOLDEN_BD else
                           "stale" if age <= FTD_VALID_BD else "expired"),
            })
            break
    # ── 랠리 데이 히스토리 (0808 샘플 "랠리 데이 히스토리" 표 — 저점 이후 일별) ──
    #   행: 날짜·일차(0=저점, d1=첫 양봉 이후 매 거래일 +1)·등락%·거래량·전일비%·비고
    rd = []
    for i in range(lo_i, n):
        day = 0 if i == lo_i else ((i - d1_i + 1) if (d1_i is not None and i >= d1_i) else None)
        chg = round((closes[i] / closes[i - 1] - 1) * 100, 2) if (i > 0 and closes[i - 1] > 0) else None
        volc = round((vols[i] / vols[i - 1] - 1) * 100, 1) if (i > 0 and vols[i - 1] > 0) else None
        if i == lo_i:
            note = "저점 (d0)"
        elif ftd_i is not None and i == ftd_i:
            note = "FTD 발생"
        elif ftd_i is not None and i > ftd_i:
            if chg is not None and chg < 0:
                note = "하락일"
            elif chg is not None and chg >= thr_pct:
                note = "거래량 미충족" if not (volc and volc > 0) else ""
            else:
                note = "상승폭·거래량 미충족" if not (volc and volc > 0) else "상승폭 미충족"
        elif day is not None and day < 4:
            note = ("조건충족·d4 전" if (chg is not None and chg >= thr_pct
                                        and volc is not None and volc > 0) else "d4 전")
        elif day is not None:
            if chg is not None and chg < 0:
                note = "하락일"
            elif chg is not None and chg >= thr_pct:
                note = "거래량 미충족"
            elif chg is not None and volc is not None and volc > 0:
                note = "상승폭 미충족"
            else:
                note = "상승폭·거래량 미충족"
        else:
            note = ""
        rd.append({"d": dates[i], "day": day, "chg": chg,
                   "vol": (round(vols[i]) if vols[i] else None), "volc": volc, "note": note})
    out["rally_days"] = rd[-20:]
    return out


def _sma_last(vals, n):
    """마지막 시점의 단순이동평균 (데이터 부족 시 None)."""
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def compute_idx_stats(dates, closes, vols):
    """
    지수 카드/DD 상세용 통계 — 샘플 dashboard_public 의 규칙 그대로:
      · DD 발생: 종가 ≤ -0.2% + 거래량 전일 대비 증가 (25거래일 윈도우)
      · 가중치: regular 1.0 / heavy(≤-2.0% & vol+20%↑) 1.5 / major(≤-3.0% & vol+20%↑) 2.0
      · 만료: 25거래일 경과 OR 그날 종가 대비 +6% 반등
      · Stage(주간 추세 근사): 200MA·50MA 상대 위치 4분면
    """
    n = len(closes)
    if n < 30:
        return None
    last, prev = closes[-1], closes[-2]
    def ret(days):
        return round((last / closes[-days - 1] - 1) * 100, 2) if n > days else None
    ma = {k: _sma_last(closes, k) for k in (21, 50, 200)}
    gaps = {f"ma{k}": (round((last / v - 1) * 100, 2) if v else None) for k, v in ma.items()}
    above50 = gaps["ma50"] is not None and gaps["ma50"] >= 0
    above200 = gaps["ma200"] is not None and gaps["ma200"] >= 0
    if above200 and above50:
        stage, stage_label = 2, "상승 추세"
    elif above200 and not above50:
        stage, stage_label = 3, "고점권 조정"
    elif not above200 and not above50:
        stage, stage_label = 4, "하락 추세"
    else:
        stage, stage_label = 1, "바닥 다지기"

    # DD 스캔 (최근 25거래일)
    dd_days, strip = [], []
    start = max(1, n - 25)
    for i in range(start, n):
        chg = closes[i] / closes[i - 1] - 1
        vol_up = vols[i] > vols[i - 1] > 0
        kind = None
        if chg <= -0.002 and vol_up:
            big_vol = vols[i] >= vols[i - 1] * 1.2
            kind = ("major" if (chg <= -0.03 and big_vol) else
                    "heavy" if (chg <= -0.02 and big_vol) else "regular")
            if last >= closes[i] * 1.06:      # +6% 반등 → 만료
                kind = None
        strip.append(kind)
        if kind:
            dd_days.append({"d": dates[i], "drop_pct": round(chg * 100, 2),
                            "vol_chg_pct": round((vols[i] / vols[i - 1] - 1) * 100, 1),
                            "kind": kind,
                            "w": {"regular": 1.0, "heavy": 1.5, "major": 2.0}[kind]})
    return {
        "last": round(last, 2), "chg_pct": round((last / prev - 1) * 100, 2),
        "w1": ret(5), "m1": ret(21),
        "gaps": gaps, "stage": stage, "stage_label": stage_label,
        "spark": [round(x, 2) for x in closes[-60:]],
        "dd_count": len(dd_days),
        "dd_weighted": round(sum(x["w"] for x in dd_days), 1),
        "dd_days": dd_days, "dd_strip": strip,
    }


# ─── 테마/섹터 점수 + 3층 노출도 (대시보드 빌더와 동일 산식) ──────────
#   [2026-08-10] 자동매매 사이저 통합: 대시보드에 표시되는 권장 비중(alloc3l)과
#   엔진의 실제 노출 상한이 같은 산식을 쓰도록 파이썬에 포팅.
#   업종 = 테마 종합점수 5성분: 베이즈 RS Line 30% + P4/5 비율 20% + 신고가 15%
#          + 1주 변화 15% + 지속성(추세템플릿 근사) 20% · PRIOR_K=10 · 3종목 미만 제외
#   섹터 = 업종 점수의 종목수 가중 · 강세 ≥52.5 / Leading 테마 ≥47.5
THEME_PRIOR_K = 10


def _n0(v):
    f = _num(v)
    return f if f is not None else 0.0


def compute_theme_stats(stocks_list):
    """강세 섹터 수 · Leading 테마 수 (3층 노출도 가드 판정용)."""
    pool = [s for s in stocks_list
            if isinstance(s.get("rs"), (int, float))
            and (s.get("sector") or "") not in ("", "기타")
            and (s.get("industry") or "") not in ("", "기타")]
    if not pool:
        return None
    g_rsl = sum(_n0(s.get("rs_line_score")) for s in pool) / len(pool)
    g_p45 = sum(1 for s in pool if str(s.get("phase")) in ("4", "4plus", "5")) / len(pool)

    def theme_score(rows):
        n = len(rows)
        bayes = (sum(_n0(s.get("rs_line_score")) for s in rows) + THEME_PRIOR_K * g_rsl) / (n + THEME_PRIOR_K)
        p45 = ((sum(1 for s in rows if str(s.get("phase")) in ("4", "4plus", "5"))
                + THEME_PRIOR_K * g_p45) / (n + THEME_PRIOR_K)) * 100
        nh = sum(1 for s in rows if s.get("h52_new") or s.get("rs_line_high")) / n * 100
        avg_w1 = sum(_n0(s.get("w1")) for s in rows) / n
        w1c = max(0.0, min(100.0, 50 + avg_w1 * 5))
        persist = sum(sum(1 for d in (s.get("pass_dots") or []) if d) / 7 * 100 for s in rows) / n
        return round(.30 * bayes + .20 * p45 + .15 * nh + .15 * w1c + .20 * persist, 1)

    by_ind = {}
    for s in pool:
        by_ind.setdefault((s["sector"], s["industry"]), []).append(s)
    ind_scores = {k: theme_score(v) for k, v in by_ind.items() if len(v) >= 3}
    lead_n = sum(1 for sc in ind_scores.values() if sc >= 47.5)
    by_sec = {}
    for (sec, _ind), sc in ind_scores.items():
        by_sec.setdefault(sec, []).append((sc, len(by_ind[(sec, _ind)])))
    strong = 0
    for rows in by_sec.values():
        w = sum(n for _sc, n in rows) or 1
        if sum(sc * n for sc, n in rows) / w >= 52.5:
            strong += 1
    return {"lead_n": lead_n, "strong_sectors": strong, "n_industries": len(ind_scores)}


def compute_alloc3l(regime_key, idx, market, theme):
    """3층 노출도 (0810 샘플 산식) — 레짐 라벨 무손상, 비중 밴드만 조정."""
    L1B = {"confirmed": (80, 100), "resumed": (75, 95), "pressure": (30, 50),
           "rally": (20, 30), "correction": (0, 25)}
    lo, hi = L1B.get(regime_key, (30, 50))
    stress = False
    if idx:
        for x in idx.values():
            w = (x or {}).get("dd_weighted")
            if isinstance(w, (int, float)) and w >= 4:
                stress = True
    else:
        dist = (market or {}).get("distribution") or {}
        dds = [v for v in (dist.get("sp_count"), dist.get("nasdaq_count"))
               if isinstance(v, (int, float))]
        if dds and max(dds) >= 6:
            stress = True
    l1lo, l1hi = max(0, lo - (15 if stress else 0)), max(0, hi - (15 if stress else 0))
    sub2 = 15 if (theme and theme["strong_sectors"] == 0) else 0
    sub3 = 20 if (theme and theme["lead_n"] < 10) else 0
    return {"lo": max(0, l1lo - sub2 - sub3), "hi": max(0, l1hi - sub2 - sub3),
            "sub2": sub2, "sub3": sub3, "sub_total": sub2 + sub3,
            "lv2": 1 if sub2 else 0, "lv3": 1 if sub3 else 0,
            "regime": regime_key, "stress": stress, "l1_lo": l1lo, "l1_hi": l1hi,
            "lead_n": theme["lead_n"] if theme else None,
            "strong_sectors": theme["strong_sectors"] if theme else None,
            "note": ("L1 %s~%s%%" % (l1lo, l1hi))
                    + (" −섹터%d" % sub2 if sub2 else "")
                    + (" −테마%d" % sub3 if sub3 else "")
                    + (" (스트레스 −15 반영)" if stress else "")}


BENCH_TICKERS = {"spy": "SPY", "qqq": "QQQ", "tqqq": "TQQQ"}


def fetch_market_data():
    """
    yfinance 로 지수 3종 + 벤치마크 3종을 받아
    (FTD 상태, 지수 통계, 벤치마크 종가맵) 을 한 번에 계산.
    실패 시 (None, None, None) — 대시보드/레짐은 기존 폴백으로 동작한다.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("[ftd] yfinance 없음 — FTD/지수/벤치마크 계산 생략 (폴백)")
        return None, None, None, None
    ftd_out, idx_out = {}, {}
    for key, (tk, name) in FTD_TICKERS.items():
        try:
            df = yf.Ticker(tk).history(period="12mo", auto_adjust=False)
            if df is None or len(df) < 30:
                print(f"[ftd] {tk} 데이터 부족 — 생략")
                continue
            dates = [d.strftime("%Y-%m-%d") for d in df.index]
            closes = [float(x) for x in df["Close"]]
            lows = [float(x) for x in df["Low"]]
            vols = [float(x) for x in df["Volume"]]
            r = detect_ftd(dates, closes, lows, vols, FTD_THRESHOLDS[key])
            if r:
                r["name"] = name
                r["threshold_pct"] = FTD_THRESHOLDS[key]
                ftd_out[key] = r
            s = compute_idx_stats(dates, closes, vols)
            if s:
                s["name"] = name
                s["ticker"] = tk
                idx_out[key] = s
        except Exception as e:
            print(f"[ftd] {tk} 실패({e}) — 생략")
    # 벤치마크 종가맵 {key: {date: close}} — NAV 리베이스용 (auto_adjust=True 로 배당 반영)
    bench_px = {}
    for key, tk in BENCH_TICKERS.items():
        try:
            df = yf.Ticker(tk).history(period="12mo", auto_adjust=True)
            if df is None or len(df) < 10:
                print(f"[bench] {tk} 데이터 부족 — 생략")
                continue
            bench_px[key] = {d.strftime("%Y-%m-%d"): float(c)
                             for d, c in zip(df.index, df["Close"])}
        except Exception as e:
            print(f"[bench] {tk} 실패({e}) — 생략")
    # 심리 지표: VIX (변동성 지수) — <15 안정 / 15~20 보통 / 20~30 경계 / 30+ 공황
    sent = None
    try:
        df = yf.Ticker("^VIX").history(period="3mo", auto_adjust=False)
        if df is not None and len(df) >= 5:
            c = [float(x) for x in df["Close"]]
            last, prev = c[-1], c[-2]
            ma20 = _sma_last(c, 20)
            sent = {"vix": {"last": round(last, 2),
                            "chg_pct": round((last / prev - 1) * 100, 2),
                            "ma20": round(ma20, 2) if ma20 else None,
                            "spark": [round(x, 2) for x in c[-60:]],
                            "level": ("공황" if last >= 30 else "경계" if last >= 20 else
                                      "보통" if last >= 15 else "안정")}}
    except Exception as e:
        print(f"[sent] ^VIX 실패({e}) — 생략")
    return (ftd_out or None), (idx_out or None), (bench_px or None), sent


def fetch_market_ftd():
    """(하위 호환) FTD 만 필요할 때."""
    ftd = fetch_market_data()[0]
    return ftd


def build_bench_history(nav_history, bench_px):
    """
    전략 nav_history 의 날짜열에 맞춰 벤치마크를 시작일 1.0 으로 리베이스.
    휴장/결측일은 직전 종가 carry-forward. 시작일 이전 종가가 없으면 None.
    반환: (bench_history 리스트, {key: 최종수익률%})
    """
    if not nav_history or not bench_px:
        return None, {}
    dates = [r.get("date") for r in nav_history if r.get("date")]
    if not dates:
        return None, {}
    hist, final = [], {}
    series = {}
    for key, px in bench_px.items():
        all_dates = sorted(px.keys())
        def close_on_or_before(d):
            last = None
            for ad in all_dates:
                if ad > d:
                    break
                last = ad
            return px[last] if last else None
        base = close_on_or_before(dates[0])
        vals = []
        for d in dates:
            c = close_on_or_before(d)
            vals.append(round(c / base, 4) if (c and base) else None)
        series[key] = vals
        last_v = next((v for v in reversed(vals) if v is not None), None)
        final[key] = round((last_v - 1) * 100, 2) if last_v is not None else None
    for i, d in enumerate(dates):
        hist.append({"date": d,
                     "spy": (series.get("spy") or [None]*len(dates))[i],
                     "qqq": (series.get("qqq") or [None]*len(dates))[i],
                     "tqqq": (series.get("tqqq") or [None]*len(dates))[i]})
    return hist, final


def calc_regime_key(market, ftd=None, idx=None):
    """
    [v1.6 — 샘플 dashboard_public '여정 29/30' 규칙 이식]
      idx(market_idx) 가 있으면:
        · Correction 은 가격 기준만 — S&P/NASDAQ 중 하나라도 50MA·200MA 모두 이탈 시.
          (가중 DD 단독 Correction 폐기)
        · 가중 DD: max(sp/nd/russell dd_weighted) ≥6 → Under Pressure 상한,
          ≥4 이고 해당 지수 21MA 미회복 → Under Pressure.
        · active FTD(유효창 내) → Resumed 우선 (가격 Correction 은 못 이김).
      idx 가 없으면(엔진 미실행/오프라인): 기존 브레드스+분산일 폴백 유지.

    분산일은 IBD 시장 타이밍의 핵심 지표이고, 이 시스템의 전략 가이드에도
    "6~7개 → Under Pressure 전환 / 8개+ → Correction 임박·진행" 이라고
    명시돼 있는데 계산에는 전혀 반영되지 않았다.
    (2026-07-20 실측: S&P·NASDAQ 분산일 8개인데 🟢 Uptrend Resumed 표시)

    STRATEGY_ROOM_LOGIC.md §1-A G0: "하락장(천장·분배·조정)이면 신규 진입 차단",
    §검증: G0 추가로 MDD −33%→−22%, Calmar 0.53→0.83.
    """
    m = market or {}
    ndfi = ((m.get("ndfi") or {}).get("value"))
    s5fi = ((m.get("s5fi") or {}).get("value"))
    vals = [v for v in (ndfi, s5fi) if isinstance(v, (int, float))]
    breadth = (sum(vals) / len(vals)) if vals else (m.get("avg") or 60)

    key = ("confirmed" if breadth >= 80 else
           "resumed"   if breadth >= 50 else
           "pressure"  if breadth >= 30 else
           "rally"     if breadth >= 15 else "correction")

    #   Resumed 선점은 골든윈도우(≤7거래일) FTD 만 — 가이드 ⑦과 일관:
    #   stale(8~21일)은 보호 없음(분산 쌓이면 Under Pressure 강등 대상).
    ftd_golden = bool(ftd) and any(
        isinstance(x, dict) and x.get("status") == "ftd"
        and x.get("window") == "golden" for x in (ftd or {}).values())

    if idx:
        # ── 신규 규칙 (여정 29/30) ──────────────────────────────────
        # 1) 가격 기준 Correction: S&P/NASDAQ 중 하나라도 50MA·200MA 모두 이탈
        px_correction = False
        for k in ("sp", "nasdaq"):
            g = (idx.get(k) or {}).get("gaps") or {}
            if (g.get("ma50") is not None and g.get("ma50") < 0
                    and g.get("ma200") is not None and g.get("ma200") < 0):
                px_correction = True
        # 2) 가중 DD → Under Pressure 상한 (Correction 으로는 못 내려감)
        wdd_cap = False
        for k in ("sp", "nasdaq", "russell"):
            x = idx.get(k) or {}
            w = x.get("dd_weighted")
            if not isinstance(w, (int, float)):
                continue
            g21 = ((x.get("gaps") or {}).get("ma21"))
            if w >= 6 or (w >= 4 and g21 is not None and g21 < 0):
                wdd_cap = True
        if px_correction:
            key = "correction"
        else:
            if wdd_cap and _REGIME_ORDER.index(key) > _REGIME_ORDER.index("pressure"):
                key = "pressure"
            if key == "correction":          # 브레드스發 correction 도 가격 기준으론 아님
                key = "rally"
            # 3) 골든윈도우 FTD → Resumed 우선 (pressure/rally 를 이김)
            if ftd_golden and _REGIME_ORDER.index(key) < _REGIME_ORDER.index("resumed"):
                key = "resumed"
        return key

    # ── 폴백 (market_idx 없음): 기존 브레드스+분산일 규칙 유지 ──────
    dist = m.get("distribution") or {}
    dds = [v for v in (dist.get("sp_count"), dist.get("nasdaq_count"))
           if isinstance(v, (int, float))]
    dd = max(dds) if dds else None
    if dd is not None:
        cap = "correction" if dd >= 8 else ("pressure" if dd >= 6 else None)
        if cap and _REGIME_ORDER.index(key) > _REGIME_ORDER.index(cap):
            key = cap
    if ftd:
        golden = any(isinstance(x, dict) and x.get("status") == "ftd"
                     and x.get("window") == "golden" for x in ftd.values())
        hard_correction = dd is not None and dd >= 8
        if golden and not hard_correction \
           and _REGIME_ORDER.index(key) < _REGIME_ORDER.index("resumed"):
            key = "resumed"
    return key



# ─────────────────────────────────────────────────────────────────────
# NaN 방어 (v1.1)
#   [왜] 파이썬에서 float('nan') 은 truthy 라, 가격이 NaN 인 종목을 보유하면
#        `if s.get("price")` 를 통과해 px=NaN → NAV 전체가 NaN 으로 오염된다.
#        NaN 은 표준 JSON 이 아니라 브라우저 JSON.parse() 가 파싱을 거부하고,
#        대시보드는 조용히 '샘플 데이터'로 폴백해 옛 날짜를 표시하게 된다.
# ─────────────────────────────────────────────────────────────────────
def _num(v, default=None):
    """유한 실수만 반환. NaN/Inf/None/문자열 → default"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default


def _ev(h):
    """진입 원가 — 없거나 비정상이면 shares×entry_px 로 대체 (NaN 방어)"""
    v = _num(h.get("entry_value"))
    if v is None:
        v = (_num(h.get("shares"), 0.0) or 0.0) * (_num(h.get("entry_px"), 0.0) or 0.0)
    return v


def _sanitize(obj):
    """NaN/Inf → None 재귀 치환 (JSON 유효성 보장)"""
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def _bdays(d1, d2):
    """두 'YYYY-MM-DD' 사이 영업일 근사 (달력일 ×5/7)."""
    try:
        a=datetime.strptime(d1,"%Y-%m-%d").date(); b=datetime.strptime(d2,"%Y-%m-%d").date()
        return int(abs((b-a).days)*5/7)
    except Exception:
        return 0

def _ratchet_stop(entry_px, max_gain_pct, rules):
    """max_gain 기준 단조 상향 스탑 (명세 §2-A)."""
    stop_pct = rules["stop_pct"]  # hard floor -7%
    if max_gain_pct >= rules["be_threshold_pct"]:
        stop_pct = max(stop_pct, 0.0)  # BE
    for gain, sp in rules["lock_tiers"]:
        if max_gain_pct >= gain:
            stop_pct = max(stop_pct, sp)
    return entry_px * (1 + stop_pct/100.0)


def total_score(s):
    """0~100 종합점수 (명세 §1-A, Comp는 rs percentile 대체 / 거래량·다이버전스 생략)."""
    comp = float(s.get("rs", 0) or 0)            # Comp 대체: RS percentile
    rs   = float(s.get("rs_now", 0) or 0)
    w1   = float(s.get("w1", 0) or 0)
    mom  = max(0, min(100, 50 + w1*2.5))         # 모멘텀: 1주 등락 스케일
    pdet = s.get("pattern_detail", {}) or {}
    bp   = s.get("best_pattern")
    q    = (pdet.get(bp, {}) or {}).get("quality") if bp else None
    pat  = {"high":95,"medium":75,"low":55}.get(q, 60 if bp else 0)
    trend = sum(s.get("pass_dots", [])) / 7.0 * 100
    sc = 0.25*comp + 0.25*rs + 0.15*mom + 0.15*pat + 0.10*trend
    if (pdet.get("rs_line_lead", {}) or {}).get("detected"): sc += 5    # RS 선행
    if s.get("rs_line_high") or s.get("h52_new"): sc += 2.5             # 신고가
    return round(min(100, sc), 2)


def passes_gates(s, fired_themes, regime_key):
    """5게이트 AND. (통과여부, 사유dict)"""
    # G0 — 조정장이면 신규 진입 차단 (§1-A). entries=0 과 이중 방어.
    g0 = regime_key != "correction"
    g3 = bool(s.get("acc2")) and bool(s.get("rs_line_high"))
    g1 = s.get("industry") in fired_themes
    g2 = (s.get("pattern_count", 0) or 0) > 0
    g4 = (sum(s.get("pass_dots", [])) >= 6)          # 추세템플릿 (A/D·중앙값은 score랭킹에서)
    ok = g0 and g3 and g1 and g2 and g4
    return ok, {"G0":g0,"G1":g1,"G2":g2,"G3":g3,"G4":g4}


def pivot_approx(s):
    """명시 피벗 없음 → 52주 고점 근사. dist = h52_pct."""
    return s.get("high_52w"), s.get("h52_pct")


# ─── 메인 ────────────────────────────────────────────────────────────
def run(screener_path, state_path):
    with open(screener_path, encoding="utf-8") as f:
        sd = json.load(f)
    stocks = {s["ticker"]: s for s in sd.get("stocks", []) if s.get("asset_type") != "ETF"}
    meta = sd.get("meta", {})
    data_date = (meta.get("updated_at") or date.today().isoformat())[:10]
    cat = sd.get("catalyst", {}) or {}
    fired_themes = set(c.get("cluster") for c in cat.get("clusters", []))
    market_ftd, market_idx, bench_px, market_sent = fetch_market_data()  # 실패 시 None들 → 폴백
    regime_key = calc_regime_key(sd.get("market", {}) or {}, market_ftd, market_idx)
    reg = REGIME.get(regime_key, REGIME["resumed"])
    # ── 3층 노출도: 대시보드와 동일 산식으로 계산해 노출 상한으로 사용 ──
    theme_stats = compute_theme_stats(list(stocks.values()))
    alloc3l = compute_alloc3l(regime_key, market_idx, sd.get("market") or {}, theme_stats)
    exp_cap = (alloc3l["hi"] / 100.0) if alloc3l else reg["max_exp"]

    # ── 일일 이력 축적 (v1.9) — 브리핑 02 베이스분포·10 테마카운트·Regime
    #    히스토리의 시계열 데이터. 최근 90건 유지, 같은 날짜 재실행은 덮어씀.
    _m2 = sd.get("market") or {}
    _bv = [v for v in (((_m2.get("ndfi") or {}).get("value")),
                       ((_m2.get("s5fi") or {}).get("value")))
           if isinstance(v, (int, float))]
    breadth_now = round(sum(_bv) / len(_bv), 1) if _bv else None
    _PH = {"4plus": "4+", "4": "4", "5": "5", "3": "3", "2": "2", "01": "1", "67": "6"}
    ph_cnt = {"4+": 0, "4": 0, "5+": 0, "5": 0, "3": 0, "2": 0, "1": 0, "6": 0, "7": 0, "0": 0}
    for _s in stocks.values():
        ph_cnt[_PH.get(str(_s.get("phase")), "0")] += 1
    _wdds = [x.get("dd_weighted") for x in (market_idx or {}).values()
             if isinstance((x or {}).get("dd_weighted"), (int, float))]
    daily_rec = {"date": data_date, "regime": regime_key, "breadth": breadth_now,
                 "wdd": (max(_wdds) if _wdds else None),
                 "alloc_lo": alloc3l["lo"] if alloc3l else None,
                 "alloc_hi": alloc3l["hi"] if alloc3l else None,
                 "stress": bool(alloc3l and alloc3l.get("stress")),
                 "lead_n": (theme_stats or {}).get("lead_n"),
                 "strong_sectors": (theme_stats or {}).get("strong_sectors"),
                 "t": len(stocks), "ph": ph_cnt}
    # (이력 병합은 state 로드 후 — 아래 참조)

    # 상태 로드 (forward 누적)
    state = {"holdings": [], "closed": [], "nav_history": [], "entries_recent": [],
             "_cash": RULES["initial_capital"]}
    if state_path and os.path.exists(state_path):
        try:
            prev = json.load(open(state_path, encoding="utf-8"))
            state["holdings"] = prev.get("_holdings", prev.get("holdings_state", []))
            state["closed"]   = prev.get("_closed", [])
            state["nav_history"] = prev.get("nav_history", [])
            state["entries_recent"] = prev.get("_entries_recent", [])
            # [fix] 현금 잔고 복원 — 빠지면 매 실행마다 cash가 initial_capital(1.0)로
            # 리셋되어 NAV가 매번 한 슬롯(≈1/cap=+8.33%)만큼 튀는 회계 버그가 생김.
            state["_cash"] = prev.get("_cash", RULES["initial_capital"])
            state["daily_history"] = prev.get("daily_history", [])   # v1.9 이력 축적

            # ── [자가치유] 과거 실행에서 NaN 이 섞여 저장된 상태를 자동 복구 ──
            #   NaN 이 하나라도 남으면 NAV 전체가 NaN 이 되고, 표준 JSON 이 아니라
            #   대시보드가 파싱에 실패해 '샘플 데이터'로 조용히 폴백한다.
            bad_cash = _num(state["_cash"]) is None
            if bad_cash:
                print(f"[state] ⚠ _cash 손상(NaN) — 재계산합니다")

            clean, dropped = [], []
            for h in state["holdings"]:
                sh, epx = _num(h.get("shares")), _num(h.get("entry_px"))
                if sh is None or epx is None or sh <= 0 or epx <= 0:
                    dropped.append(h.get("ticker"))
                    continue
                cur = _num(h.get("cur"))
                if cur is None or cur <= 0:
                    h["cur"] = epx
                if _num(h.get("entry_value")) is None:
                    h["entry_value"] = round(sh * epx, 6)
                for f2 in ("max_gain_pct", "current_stop", "days_in"):
                    if f2 in h and _num(h.get(f2)) is None:
                        h[f2] = 0
                clean.append(h)
            if dropped:
                print(f"[state] ⚠ 평가 불가 보유 제거(NaN shares/entry_px): {dropped}")
            state["holdings"] = clean

            # NaN 이 섞인 NAV 이력은 신뢰할 수 없으므로 유한값만 유지
            nh_all = state["nav_history"]
            state["nav_history"] = [r for r in nh_all if _num(r.get("nav")) is not None]
            if len(state["nav_history"]) != len(nh_all):
                print(f"[state] ⚠ NAV 이력 정리: {len(nh_all)} → {len(state['nav_history'])}건")

            if bad_cash:
                # 완전투자 가정(현금 0)으로 재출발 — 보유 평가액이 곧 NAV
                state["_cash"] = 0.0
        except Exception as e:
            print(f"[state] 로드 실패({e}) — 빈 상태로 시작")

    holdings = state["holdings"]
    closed   = state["closed"]
    cost = RULES["cost_bps"]/10000.0

    # ── 실적일 로드 (make_snapshots.py --earnings 가 생성 · 없으면 실적 룰 비활성) ──
    earn_map = {}
    try:
        if os.path.exists("earnings.json"):
            _ej = json.load(open("earnings.json", encoding="utf-8"))
            earn_map = {k: v for k, v in _ej.items()
                        if k != "meta" and isinstance(v, str)}
    except Exception as _e:
        print(f"[earnings] 로드 실패({_e}) — 실적 룰 비활성")

    def _dte(t):
        """다음 실적까지 영업일 (없거나 과거면 None)."""
        d = earn_map.get(t)
        if not d or d < data_date:
            return None
        return _bdays(data_date, d)

    # 일일 이력 병합 (같은 날짜 재실행은 덮어씀 · 최근 90건)
    daily_history = [r for r in state.get("daily_history", [])
                     if r.get("date") != data_date]
    daily_history.append(daily_rec)
    daily_history = daily_history[-90:]

    # ── 1) mark-to-market + 청산 ─────────────────────────────────────
    survivors = []
    for h in holdings:
        s = stocks.get(h["ticker"])
        # [NaN 방어] 가격이 NaN/None/0 이면 직전 종가(cur) → 진입가 순으로 폴백
        px = _num(s.get("price")) if s else None
        if px is None or px <= 0:
            px = _num(h.get("cur"), None) or _num(h.get("entry_px"), 0.0)
        h["cur"] = px
        # 스파크라인용 종가 이력 (하루 1회 append · 최근 30개 — 브리핑 09 spark)
        if h.get("px_hist_d") != data_date:
            h.setdefault("px_hist", []).append(round(px, 2))
            h["px_hist"] = h["px_hist"][-30:]
            h["px_hist_d"] = data_date
        gain = (px/h["entry_px"] - 1)*100
        h["max_gain_pct"] = max(h.get("max_gain_pct", 0), gain)
        h["days_in"] = _bdays(h["entry_date"], data_date)
        # 런너 승격 — runner_cap(5) 강제: 가득 차면 승격 보류(코어 유지, 다음날 재평가)
        #   운용로직: 12-cap 별도(런너 최대 5, 총 최대 17)
        n_runners = sum(1 for x in holdings if x.get("runner"))
        is_runner = h.get("runner", False)
        if not is_runner and n_runners >= RULES["runner_cap"]:
            pass                                     # cap 가득 — 승격 보류
        elif not is_runner and h["max_gain_pct"] >= RULES["runner_promote_gain_pct"]:
            h["runner"] = True; h["runner_tier"] = 1; is_runner = True
        elif not is_runner and h["max_gain_pct"] >= RULES["runner_trend_min_gain_pct"]:
            giveback = (h["max_gain_pct"] - gain)/max(h["max_gain_pct"],1)
            if (s and s.get("price",0) >= (s.get("ma50") or 0)) and giveback <= RULES["runner_trend_giveback_max"]:
                h["runner"] = True; h["runner_tier"] = 2; is_runner = True
        # 8주룰 활성화
        if not h.get("oneill_8w_active") and h["max_gain_pct"] >= RULES["oneill_threshold_pct"] \
           and h["days_in"] <= RULES["oneill_trigger_bdays"]:
            h["oneill_8w_active"] = True; h["oneill_trigger_date"] = data_date
        # 스탑 래칫
        h["current_stop"] = _ratchet_stop(h["entry_px"], h["max_gain_pct"], RULES)

        exit_cause = None
        if px <= h["current_stop"]:
            exit_cause = "손절" if h["max_gain_pct"] < RULES["be_threshold_pct"] else ("BE 청산" if h["current_stop"]<=h["entry_px"]*1.001 else "Lock 청산")
        elif (RULES["sell_ma50_break"]
              and h["days_in"] >= RULES["sell_grace_bdays"]
              and s and _num(s.get("ma50")) and px < _num(s.get("ma50"))):
            # 셀 시그널 — 진입 10영업일 grace 후 50일선 이탈 → 전량.
            #   런너도 동일 적용(운용로직: "런너는 50일선 이탈만" = 시간청산은
            #   면제되지만 50일선 이탈에는 걸린다).
            exit_cause = "50일선 이탈"
        else:
            # ── 실적 D-3 대응 (운용로직 ③ · v2.0) — 청산 우선순위: 스탑 > 셀시그널 > 실적 > max hold
            #   손실 → 전량 정리 / 0~2R → ⅔ 트림 / 2~4R → ⅓ 트림 / 4R+ → 보유 (R = gain/7%)
            #   같은 실적일에 한 번만 실행 (earn_trimmed 마킹)
            h["dte"] = _dte(h["ticker"])
            if (h["dte"] is not None and h["dte"] <= RULES["earnings_trim_bdays"]
                    and h.get("earn_trimmed") != earn_map.get(h["ticker"])):
                h["earn_trimmed"] = earn_map.get(h["ticker"])
                _R = gain / abs(RULES["stop_pct"])
                if gain < 0:
                    exit_cause = "실적전 정리"
                else:
                    _frac = (2.0/3.0) if _R < 2 else ((1.0/3.0) if _R < 4 else 0.0)
                    if _frac > 0:
                        _sold = (_num(h.get("shares"), 0.0) or 0.0) * _frac
                        _proceeds = px * _sold * (1 - cost)
                        state["_cash"] = _num(state.get("_cash"), 0.0) + _proceeds
                        closed.append({
                            "ticker": h["ticker"], "entry": h["entry_date"], "exit": data_date,
                            "days": h["days_in"], "entry_px": round(h["entry_px"],2),
                            "exit_px": round(px,2), "shares": _sold,
                            "realized": _proceeds - _ev(h) * _frac,
                            "cat": "실적 부분정리", "partial": int(round(_frac*100)),
                            "was_runner": bool(h.get("runner")), "runner_tier": h.get("runner_tier"),
                            "max_gain_pct": round(h.get("max_gain_pct", 0.0), 1),
                            "reentry": bool(h.get("reentry")),
                            "giveback_pct": round(max(0.0, h.get("max_gain_pct", 0.0) - gain), 1),
                            "exit_regime": regime_key, "mkt_driven": True,
                        })
                        h["shares"] = (_num(h.get("shares"), 0.0) or 0.0) * (1 - _frac)
                        h["entry_value"] = _ev(h) * (1 - _frac)
                        h["partial"] = int(round(_frac*100))
            if exit_cause is None:
                # 시간 청산 (런너/8주룰 면제)
                time_cap = RULES["hold_days"]
                if h.get("oneill_8w_active"):
                    time_cap = RULES["oneill_hold_bdays"] + _bdays(h.get("oneill_trigger_date",h["entry_date"]), data_date) - h["days_in"] + RULES["oneill_hold_bdays"]
                if not is_runner and h["days_in"] >= time_cap:
                    exit_cause = "max hold"

        if exit_cause:
            # ══ [치명적 버그 수정] 청산 대금을 현금으로 회수한다 ══
            #   기존 코드엔 `cash +=` 가 파일 전체에 단 한 줄도 없어서,
            #   종목을 팔면 closed[] 에 기록만 되고 자본이 증발했다.
            #   실측(2026-07-20 라이브): 12종목 전액투자 → 3건 청산 후
            #   회수됐어야 할 0.216 이 사라져 NAV 1.0 → 0.7484 (−25%).
            #   미실현이 −0.2%인데 NAV 가 −25%였던 이유가 이것이다.
            proceeds = px * (_num(h.get("shares"), 0.0) or 0.0) * (1 - cost)
            state["_cash"] = _num(state.get("_cash"), 0.0) + proceeds
            closed.append({
                "ticker": h["ticker"], "entry": h["entry_date"], "exit": data_date,
                "days": h["days_in"], "entry_px": round(h["entry_px"],2), "exit_px": round(px,2),
                "shares": h["shares"], "realized": (px*(_num(h.get("shares"),0.0) or 0.0)*(1-cost) - _ev(h)),
                "cat": exit_cause,
                # ── 사후 분석용 메타 (검증 리포트 권고: 튜닝 전에 측정부터) ──
                "was_runner": bool(h.get("runner")),          # 런너 승격 여부
                "runner_tier": h.get("runner_tier"),
                "max_gain_pct": round(h.get("max_gain_pct", 0.0), 1),
                "reentry": bool(h.get("reentry")),            # 재진입 건이었나
                "giveback_pct": round(max(0.0, h.get("max_gain_pct", 0.0) - gain), 1),
                # ── 재진입 정책 v6.20 판별용 ──
                #   시장동반 손절 = 청산 당시 레짐이 이미 약세(pressure 이하).
                #   종목약화 손절 = 시장은 멀쩡(confirmed/resumed)한데 혼자 -7% 도달.
                #   ※ 레짐 프록시 기준 — STRATEGY_ROOM_LOGIC.md 원문 확보 시 대조할 것.
                "exit_regime": regime_key,
                "mkt_driven": regime_key in ("pressure", "rally", "correction"),
            })
        else:
            survivors.append(h)
    holdings = survivors

    # ── 2) 진입 후보 (게이트 → 가드 → 랭킹) ─────────────────────────
    held = set(h["ticker"] for h in holdings)
    # 과거에 청산한 적 있는 티커 — 재진입 추적용.
    #   검증 리포트: "최초발화→peak 사이 −41~−88% 중간 베어가 있어 −7% 손절에
    #   전부 잘린다 → 재진입이 필수 메커니즘". 우리는 쿨다운이 없어 재진입이
    #   가능하지만, 실제로 몇 건이나 일어나는지 측정된 적이 없었다.
    prev_closed = {c["ticker"] for c in closed}

    # ── 재진입 차단 목록 (정책 v6.20) ────────────────────────────────
    #   종목약화 손절(cat=="손절" & mkt_driven=False)만 10영업일 차단.
    #   시장동반 손절·BE/Lock 청산·50일선 이탈·max hold 는 차단하지 않는다.
    #   과거 기록에 mkt_driven 필드가 없으면(구버전 데이터) 차단하지 않음.
    reentry_block = {}
    for c in closed:                     # 뒤로 갈수록 최신 → 최신 기록이 덮어씀
        if c.get("cat") == "손절" and c.get("mkt_driven") is False:
            reentry_block[c["ticker"]] = c.get("exit", "")
        elif c["ticker"] in reentry_block:
            reentry_block.pop(c["ticker"])   # 이후 다른 사유 청산이 있으면 해제
    reentry_blocked = 0

    cands = []
    rvol_blocked, rvol_missing, earn_blocked = 0, 0, 0
    for t, s in stocks.items():
        if t in held: continue
        # 재진입 차단 (종목약화 손절 후 10영업일)
        _bx = reentry_block.get(t)
        if _bx and _bdays(_bx, data_date) < RULES["reentry_block_bdays"]:
            reentry_blocked += 1
            continue
        # 실적 임박 가드 (D-7 이내 신규 진입 금지 · v2.0)
        _ed = _dte(t)
        if _ed is not None and _ed <= RULES["earnings_entry_guard_bdays"]:
            earn_blocked += 1
            continue
        ok, gd = passes_gates(s, fired_themes, regime_key)
        if not ok: continue
        # 피벗 거리 가드 (근사): 52주 고점 -10%~+5%
        _, dist = pivot_approx(s)
        if dist is not None and not (-10 <= dist <= 5): continue
        # ── RVOL 하드게이트 (명세 §1-B, entry_min_rvol=1.5) ─────────
        #   당일 거래량 / 50일 평균 < 1.5 → 진입 후보에서 제외.
        #   값이 없으면(NaN/None) 피벗 가드와 동일하게 통과시키되 개수를
        #   집계해 로그로 남긴다 — 데이터 결손이 전량 차단으로 이어지지
        #   않게 하는 기존 가드들의 fail-open 관행을 따름.
        rvol = _num(s.get("vol_ratio_50d"))
        if rvol is None:
            rvol_missing += 1
        elif rvol < RULES["entry_min_rvol"]:
            rvol_blocked += 1
            continue
        cands.append((total_score(s), s))
    cands.sort(key=lambda x: x[0], reverse=True)

    # ── 3) 레짐 사이징 + 12-cap 진입 ────────────────────────────────
    recent = [e for e in state["entries_recent"] if _bdays(e, data_date) < 5]
    week_quota = max(0, reg["entries"] - len(recent))
    cap = RULES["max_concurrent_positions"]
    core_held = [h for h in holdings if not h.get("runner")]
    slots = max(0, cap - len(core_held))

    # NAV 계산용 (진입 전 평가)
    def nav_now():
        hv = sum(h["cur"]*h["shares"] for h in holdings)
        return state.get("_cash", RULES["initial_capital"]) + hv
    cash = state.get("_cash", RULES["initial_capital"])
    nav_pre = cash + sum(h["cur"]*h["shares"] for h in holdings)
    # 노출 상한 = 3층 노출도 hi (레짐 밴드 − 스트레스 − 섹터 − 테마 가드)
    max_new_cash = max(0, nav_pre*exp_cap - sum(h["cur"]*h["shares"] for h in holdings))

    signals = []
    entered = 0

    # ── 사이징: per-position = 가용현금 ÷ 그날 통과 신규수 ──────────
    #   운용로직 v6.15 ⑤: "NAV÷슬롯 균등 아님 — v6.21 기각".
    #   신호가 몰린 날은 그날 들어갈 종목 수로 현금을 쪼개 넣어 상시 풀투자를
    #   유지한다. 흩어져 뜨면 첫날이 현금을 다 먹고 나머지는 청산 때까지 대기.
    #   (기존엔 nav_pre/cap 고정이라 신호가 적은 날 현금이 놀았다)
    n_plan = 0
    _cash_left, _exp_left = cash, max_new_cash
    for _sc, _s in cands:
        if n_plan >= min(week_quota, slots):
            break
        _px = _num(_s.get("price"))
        if _px is None or _px <= 0:
            continue
        n_plan += 1
    avail = max(0.0, min(cash, max_new_cash))
    # 왕복 비용(진입분)을 미리 반영해야 현금이 음수로 떨어지지 않는다.
    target_per = (avail / n_plan / (1 + cost)) if n_plan > 0 else 0.0

    for score, s in cands:
        t = s["ticker"]
        _, dist = pivot_approx(s)
        _rv = _num(s.get("vol_ratio_50d"))
        signals.append({"ticker": t, "industry": s.get("industry",""),
                        "pattern": (s.get("pattern_detail",{}).get(s.get("best_pattern"),{}) or {}).get("name", s.get("best_pattern","")),
                        "signal_close": round(float(s["price"]),2),
                        "pivot": round(float(s.get("high_52w") or s["price"]),2),
                        "dist": round(dist,1) if dist is not None else None,
                        "rvol": round(_rv, 2) if _rv is not None else None,
                        "score": score})
        if entered >= week_quota or slots <= 0: continue
        # cash/(1+cost) 로 상한을 둬 인출액(alloc*(1+cost))이 현금을 넘지 않게 한다
        alloc = min(target_per, cash / (1 + cost), max_new_cash)
        if alloc < nav_pre*0.005: continue  # 먼지 가드
        px = _num(s.get("price"))
        if px is None or px <= 0:
            continue   # 가격 이상 종목은 진입 스킵
        shares = alloc/px
        holdings.append({
            "ticker": t, "entry_date": data_date, "entry_px": px, "avg_cost_px": px,
            "shares": shares, "entry_value": alloc*(1+cost), "cur": px,
            "max_gain_pct": 0.0, "days_in": 0, "current_stop": px*(1+RULES["stop_pct"]/100),
            "runner": False, "oneill_8w_active": False,
            "reentry": t in prev_closed,
            "px_hist": [round(px, 2)], "px_hist_d": data_date,
        })
        cash -= alloc*(1+cost); max_new_cash -= alloc
        state.setdefault("entries_recent", []).append(data_date)
        entered += 1; slots -= 1

    # ── 3b) 스위칭 (weed the garden — 운용로직 ② 슬롯/스위칭) ────────
    #   슬롯 가득 시: 정체 최약 보유 vs 신규 강신호 total_score 차 ≥12점 → 교체.
    #   보호: 런너 · 8주룰 활성 · 수익 ≥ +10%(winner) · 진입 10영업일 이내(grace).
    #   일일 최대 2건. (조정장은 후보 자체가 G0 에서 걸러져 자연 차단)
    switched = 0
    if slots <= 0 and cands:
        held_now = {h["ticker"] for h in holdings}
        rest = [(sc, s) for sc, s in cands if s["ticker"] not in held_now]
        for sc, s in rest:
            if switched >= RULES["switch_max_per_day"] or entered >= week_quota:
                break
            elig = []
            for h in holdings:
                if h.get("runner") or h.get("oneill_8w_active"):
                    continue
                if (h["cur"]/h["entry_px"] - 1)*100 >= RULES["switch_protect_gain_pct"]:
                    continue                                   # winner 보호
                if h.get("days_in", 0) < RULES["switch_grace_bdays"]:
                    continue                                   # grace 보호
                hs = stocks.get(h["ticker"])
                elig.append((total_score(hs) if hs else 0.0, h))
            if not elig:
                break
            h_score, weakest = min(elig, key=lambda x: x[0])
            if sc - h_score < RULES["switch_edge_min"]:
                break                       # 최고 신규조차 엣지 부족 → 종료
            px_new = _num(s.get("price"))
            if px_new is None or px_new <= 0:
                continue
            # 최약 보유 청산 → 대금 회수
            px_w = weakest["cur"]
            sh_w = _num(weakest.get("shares"), 0.0) or 0.0
            proceeds = px_w * sh_w * (1 - cost)
            cash += proceeds
            gain_w = (px_w/weakest["entry_px"] - 1)*100
            closed.append({
                "ticker": weakest["ticker"], "entry": weakest["entry_date"],
                "exit": data_date, "days": weakest.get("days_in", 0),
                "entry_px": round(weakest["entry_px"],2), "exit_px": round(px_w,2),
                "shares": sh_w, "realized": proceeds - _ev(weakest),
                "cat": "스위칭 교체",
                "was_runner": False, "runner_tier": None,
                "max_gain_pct": round(weakest.get("max_gain_pct",0.0),1),
                "reentry": bool(weakest.get("reentry")),
                "giveback_pct": round(max(0.0, weakest.get("max_gain_pct",0.0)-gain_w),1),
                "exit_regime": regime_key,
                "mkt_driven": True,   # 스위칭은 재진입 차단 대상 아님
            })
            holdings.remove(weakest)
            # 해방된 현금으로 신규 진입
            alloc = min(proceeds, cash) / (1 + cost)
            if alloc < nav_pre*0.005:
                continue
            holdings.append({
                "ticker": s["ticker"], "entry_date": data_date, "entry_px": px_new,
                "avg_cost_px": px_new, "shares": alloc/px_new,
                "entry_value": alloc*(1+cost), "cur": px_new,
                "max_gain_pct": 0.0, "days_in": 0,
                "current_stop": px_new*(1+RULES["stop_pct"]/100),
                "runner": False, "oneill_8w_active": False,
                "reentry": s["ticker"] in prev_closed,
                "px_hist": [round(px_new, 2)], "px_hist_d": data_date,
            })
            cash -= alloc*(1+cost)
            state.setdefault("entries_recent", []).append(data_date)
            entered += 1; switched += 1

    # ── 4) NAV 누적 + 회계 ──────────────────────────────────────────
    hold_val = sum(h["cur"]*h["shares"] for h in holdings)
    nav = cash + hold_val
    state["_cash"] = cash
    nh = state["nav_history"]
    prev_nav = nh[-1]["nav"] if nh else RULES["initial_capital"]
    chg = (nav/prev_nav - 1)*100 if prev_nav else 0.0
    if not nh or nh[-1]["date"] != data_date:
        nh.append({"date": data_date, "nav": round(nav,4), "chg": round(chg,2)})
    else:
        nh[-1] = {"date": data_date, "nav": round(nav,4), "chg": round(chg,2)}

    # ── 5) 지표 계산 + UI 출력 shape ────────────────────────────────
    out = build_output(data_date, nav, holdings, closed, nh, signals, cash, regime_key)
    out["market_ftd"] = market_ftd          # 대시보드 FTD 상세 카드가 읽음 (없으면 null)
    out["market_idx"] = market_idx          # 주요 지수 현황·DD 상세 카드가 읽음 (없으면 null)
    out["alloc3l"] = alloc3l                # 3층 노출도 — 대시보드 권장 비중이 이 값을 우선 사용
    out["daily_history"] = daily_history    # 레짐·phase·테마 이력 (브리핑 02/10 시계열)
    out["market_sent"] = market_sent        # 심리 지표 (VIX)

    # ── 벤치마크 NAV (SPY/QQQ/TQQQ — 시작일 1.0 리베이스, α 산출) ──
    bench_hist, bench_final = build_bench_history(nh, bench_px)
    out["bench_history"] = bench_hist       # NAV 곡선·MDD 차트용 (없으면 null)
    out["benchmarks"] = {"strat": out.get("ret_pct"),
                         "spy": bench_final.get("spy"),
                         "qqq": bench_final.get("qqq"),
                         "tqqq": bench_final.get("tqqq")}
    if isinstance(out.get("risk"), dict):
        out["risk"]["alpha_voo"] = (round(out["ret_pct"] - bench_final["spy"], 2)
                                    if bench_final.get("spy") is not None else None)
        out["risk"]["alpha_qqq"] = (round(out["ret_pct"] - bench_final["qqq"], 2)
                                    if bench_final.get("qqq") is not None else None)
    if bench_final:
        print(f"   벤치마크: SPY {bench_final.get('spy')}% · QQQ {bench_final.get('qqq')}% · "
              f"TQQQ {bench_final.get('tqqq')}% | α(vs SPY) "
              f"{out['risk'].get('alpha_voo') if isinstance(out.get('risk'), dict) else '—'}%p")
    out["_holdings"] = holdings
    out["_closed"] = closed
    out["_cash"] = cash
    out["_entries_recent"] = state.get("entries_recent", [])[-30:]

    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(_sanitize(out), f, ensure_ascii=False, indent=2, allow_nan=False)

    print(f"[strategy_room] {data_date} | NAV {nav:.4f} ({chg:+.2f}%) | 보유 {len(holdings)} | 신규 {entered} | 청산누적 {len(closed)} | 레짐 {reg['label']}")
    if alloc3l:
        print(f"   3층 노출도: {alloc3l['lo']}~{alloc3l['hi']}% ({alloc3l['note']}) | "
              f"강세섹터 {alloc3l['strong_sectors']} · Leading테마 {alloc3l['lead_n']} → 노출상한 {exp_cap:.0%}")
    print(f"   RVOL 게이트(≥{RULES['entry_min_rvol']}): 차단 {rvol_blocked}건 · 데이터 없음(통과) {rvol_missing}건"
          f" | 재진입 차단(종목약화 {RULES['reentry_block_bdays']}bd) {reentry_blocked}건"
          f" | 실적 D-{RULES['earnings_entry_guard_bdays']} 차단 {earn_blocked}건"
          f"{'' if earn_map else ' (earnings.json 없음 — 실적 룰 비활성)'}"
          f" | 스위칭 {switched}건 · 후보 {len(cands)}건")
    if market_ftd:
        for k, f in market_ftd.items():
            if f.get("status") == "ftd":
                print(f"   FTD {f['name']}: {f['ftd_date']} d{f['ftd_day']} "
                      f"+{f['ftd_gain_pct']}% (경과 {f['age_bd']}bd · {f['window']}"
                      f"{' · 후발' if f.get('late') else ''})")
            elif f.get("status") == "rally":
                print(f"   FTD {f['name']}: 반등 시도 d{f['rally_day']} (저점 {f['rally_low_date']})")
            else:
                print(f"   FTD {f['name']}: 반등 대기 (저점 {f['rally_low_date']})")
    else:
        print("   FTD: 데이터 없음 — 브레드스+분산일 폴백")
    _rs = out.get("runner_stats") or {}
    if _rs:
        r, c_, re_ = _rs["runner_closed"], _rs["core_closed"], _rs["reentry"]
        print(f"   런너 청산 {r['n']}건 평균 {r['avg']:+.1f}% (승률 {r['winrate']}%, 최고 {r['best']:+.1f}%)"
              f" | 코어 {c_['n']}건 평균 {c_['avg']:+.1f}%"
              f" | 재진입 {re_['n']}건 평균 {re_['avg']:+.1f}%"
              f" | 보유중 런너 {_rs['runner_active']}·재진입 {_rs['reentry_active']}")
    return out


def build_output(data_date, nav, holdings, closed, nav_history, signals, cash, regime_key):
    cap = RULES["max_concurrent_positions"]
    runners = [h for h in holdings if h.get("runner")]
    core = [h for h in holdings if not h.get("runner")]
    hold_val = sum(h["cur"]*h["shares"] for h in holdings)
    unreal = sum(((_num(h.get("cur"),0.0) or 0.0)*(_num(h.get("shares"),0.0) or 0.0) - _ev(h))
                 for h in holdings)
    unreal_pct = unreal/nav*100 if nav else 0
    ret_pct = (nav-1.0)*100

    # 위험 지표
    rets = [r["chg"]/100 for r in nav_history[1:]] if len(nav_history) > 1 else []
    risk = _risk_metrics(nav_history, rets, ret_pct)
    # 트레이드 지표
    trades = _trade_metrics(closed)
    runner_stats = _runner_metrics(closed, holdings)
    # 청산 카테고리 집계
    cat_colors = {"손절":["#A32D2D","#FDECEC"],"실적 부분정리":["#854F0B","#FFF4E6"],"BE 청산":["#185FA5","#E6F1FB"],
                  "스위칭 교체":["#0F6E56","#E0F5F2"],"실적전 정리":["#73726c","#f0efea"],"RS 다이버":["#534AB7","#F3F0FF"],
                  "Lock 청산":["#1a8a4a","#E8F5E9"],"max hold":["#999","#f5f4ef"]}
    catcount = {}
    for c in closed: catcount[c["cat"]] = catcount.get(c["cat"],0)+1
    categories = [[k,v,cat_colors.get(k,["#999","#f5f4ef"])[0],cat_colors.get(k,["#999","#f5f4ef"])[1]] for k,v in catcount.items()]

    # 보유 표시
    hd = []
    for h in sorted(holdings, key=lambda x:(x["cur"]/x["entry_px"]-1), reverse=True):
        hd.append({"ticker":h["ticker"], "entry":h["entry_date"], "days":h.get("days_in",0),
                   "cd":("∞" if h.get("runner") else
                         (f"D-{h['dte']}" if isinstance(h.get("dte"), int) and h["dte"] <= 14 else "")),
                   "entry_px":round(h["entry_px"],2),
                   "cur":round(h["cur"],2), "runner":h.get("runner",False),
                   "partial":h.get("partial"), "dte":h.get("dte")})
    # 청산 표시 (최신순)
    cl = [{"ticker":c["ticker"],"entry":c["entry"],"exit":c["exit"],"days":c["days"],
           "entry_px":c["entry_px"],"exit_px":c["exit_px"],"cat":c["cat"]} for c in reversed(closed)][:30]

    return {
        "data_date": data_date, "track": RULES["track"], "logic_version": RULES["logic_version"],
        "nav": round(nav,4), "ret_pct": round(ret_pct,2), "unreal_pct": round(unreal_pct,2),
        "cap": cap, "runner_cap": RULES["runner_cap"], "core_count": len(core), "runner_count": len(runners),
        "regime": REGIME.get(regime_key,{}).get("label",""),
        "signals": signals[:10], "gates": GATES,
        "benchmarks": {"strat": round(ret_pct,2), "spy": None, "qqq": None, "tqqq": None},
        "risk": risk, "trades": trades, "nav_history": nav_history,
        "holdings": hd,
        "runner_stats": runner_stats,
        "closed_summary": {"n": trades["n"], "winrate": trades["winrate"], "avg": trades["expectancy"],
                           "best": trades["best"], "worst": trades["worst"], "categories": categories},
        "closed": cl,
    }


def _risk_metrics(nav_history, rets, ret_pct):
    days = len(nav_history)
    if len(rets) < 2:
        return {"days":days,"ret":round(ret_pct,2),"sharpe":None,"sortino":None,"vol":None,
                "mdd":None,"ret_mdd":None,"day_hi":None,"day_lo":None,"alpha_voo":None,"alpha_qqq":None}
    mean = sum(rets)/len(rets)
    var = sum((r-mean)**2 for r in rets)/len(rets)
    sd = math.sqrt(var)
    down = [r for r in rets if r<0]
    dsd = math.sqrt(sum(r*r for r in down)/len(down)) if down else 1e-9
    sharpe = (mean/sd*math.sqrt(252)) if sd else 0
    sortino = (mean/dsd*math.sqrt(252)) if dsd else 0
    # MDD
    peak=-1; mdd=0
    for r in nav_history:
        peak=max(peak,r["nav"]); mdd=min(mdd,(r["nav"]/peak-1))
    return {"days":days,"ret":round(ret_pct,2),"sharpe":round(sharpe,2),"sortino":round(sortino,2),
            "vol":round(sd*math.sqrt(252)*100,1),"mdd":round(mdd*100,1),
            "ret_mdd":round(abs(ret_pct/(mdd*100)),2) if mdd else None,
            "day_hi":round(max(rets)*100,1),"day_lo":round(min(rets)*100,1),
            "alpha_voo":None,"alpha_qqq":None}


def _runner_metrics(closed, holdings):
    """
    런너 기여도 · 재진입 통계 — '튜닝하기 전에 측정하라'는 검증 리포트의
    방법론을 그대로 따른다. 파라미터를 바꾸지 않으므로 과최적화 위험이 없다.

    검증 리포트 요지(8종목 실측): 런너 ON 평균 +587% vs OFF +498% (+89%p).
    무작위 150종목 교차검증에서도 중앙값 −0.5%p·승률 77 vs 79% 로 무해했고,
    이득은 전부 우측 꼬리(대형 승자)에서 나왔다 → 구조적 비대칭.
    우리 페이퍼 트레이딩에서도 같은 패턴이 나오는지 확인하기 위한 지표.
    """
    def _pl(c):
        e, x = c.get("entry_px") or 0, c.get("exit_px") or 0
        return (x / e - 1) * 100 if e else 0.0

    run_c  = [c for c in closed if c.get("was_runner")]
    core_c = [c for c in closed if not c.get("was_runner")]
    re_c   = [c for c in closed if c.get("reentry")]

    def _agg(rows):
        if not rows:
            return {"n": 0, "avg": 0.0, "winrate": 0.0, "best": 0.0}
        pls = [_pl(c) for c in rows]
        return {"n": len(rows),
                "avg": round(sum(pls) / len(pls), 1),
                "winrate": round(sum(1 for p in pls if p > 0) / len(pls) * 100),
                "best": round(max(pls), 1)}

    # 런너 승격 문턱을 넘었는데 결국 얼마나 되돌렸나 (락 사다리 점검용)
    gb = [c.get("giveback_pct", 0) for c in run_c if c.get("giveback_pct") is not None]
    return {
        "runner_closed": _agg(run_c),
        "core_closed":   _agg(core_c),
        "reentry":       _agg(re_c),
        "runner_active": sum(1 for h in holdings if h.get("runner")),
        "reentry_active": sum(1 for h in holdings if h.get("reentry")),
        "runner_avg_giveback": round(sum(gb) / len(gb), 1) if gb else 0.0,
    }


def _trade_metrics(closed):
    n=len(closed)
    if not n:
        return {"n":0,"winrate":None,"payoff":None,"pf":None,"avg_win":None,"avg_loss":None,
                "expectancy":None,"best":None,"worst":None}
    pls = [(c["exit_px"]/c["entry_px"]-1)*100 for c in closed]
    wins=[p for p in pls if p>0]; losses=[p for p in pls if p<=0]
    aw = sum(wins)/len(wins) if wins else 0
    al = sum(losses)/len(losses) if losses else 0
    gp = sum(wins); gl = abs(sum(losses))
    return {"n":n,"winrate":round(len(wins)/n*100,1),
            "payoff":round(aw/abs(al),2) if al else None,
            "pf":round(gp/gl,2) if gl else None,
            "avg_win":round(aw,1),"avg_loss":round(al,1),
            "expectancy":round(sum(pls)/n,2),
            "best":round(max(pls),1),"worst":round(min(pls),1)}


if __name__ == "__main__":
    sp = sys.argv[1] if len(sys.argv) > 1 else os.path.join("output","screener_data.json")
    st = sys.argv[2] if len(sys.argv) > 2 else "strategy_room.json"
    run(sp, st)
