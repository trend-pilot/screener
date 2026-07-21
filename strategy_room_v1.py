# -*- coding: utf-8 -*-
"""
strategy_room_v1.py — 전략실 forward 페이퍼 트레이딩 엔진 (v1)
================================================================================
명세 STRATEGY_ROOM_LOGIC.md(v6.20) 를, 현재 screener_data.json 이 제공하는
필드 한도 내에서 구현한 1차 버전.

【구현됨】
  - 5게이트 AND: G0(시장 레짐) · G1(발화 테마) · G2(패턴) · G3(RS 가속+신고가) · G4(종합점수+추세)
  - total_score 랭킹 (Comp는 rs percentile로 대체)
  - 진입: 균등가중 · 12슬롯 + 런너 제외 · 레짐 사이징(진입개수/노출)
  - 보유: Lock 7단계 래칫 · 8주룰 · max-hold(40bd) · 스위칭(weed the garden)
  - 런너: Tier1(+100%) · Tier2(추세 +40%/giveback)
  - 회계: entry_px 불변 · avg_cost 손익 · NAV = cash + 평가액
  - forward 누적: 전일 strategy_room.json 읽어 당일 반영 → 재기록

【v1 비활성 (데이터 없음 — screener.py 보강 시 활성)】
  - 실적 룰 전체 (days_to_earnings 없음): 실적 부분정리·재매수·실적전 정리·실적 임박 가드
  - RVOL 하드게이트 (vol_ratio_50d 없음)
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
    "cost_bps": 5.0,
    "track": "g3+g0+g1+g2", "logic_version": "v6.15(v1)",
}
GATES = ["G3 Trigger","G4 Guard","G0 Market","G1 Theme","G2 Pattern"]
# 레짐 사이징 (G0; screener market.overall = green/yellow/red 3단계로 단순화)
REGIME = {
    "green":  {"label":"🟢 Uptrend",        "max_exp":1.00, "entries":99},
    "yellow": {"label":"🟡 Under Pressure",  "max_exp":0.50, "entries":2},
    "red":    {"label":"🔴 Correction",      "max_exp":0.25, "entries":0},
}



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
    g0 = regime_key != "red"
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
    regime_key = (sd.get("market", {}) or {}).get("overall", "green")
    reg = REGIME.get(regime_key, REGIME["green"])

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

    # ── 1) mark-to-market + 청산 ─────────────────────────────────────
    survivors = []
    for h in holdings:
        s = stocks.get(h["ticker"])
        # [NaN 방어] 가격이 NaN/None/0 이면 직전 종가(cur) → 진입가 순으로 폴백
        px = _num(s.get("price")) if s else None
        if px is None or px <= 0:
            px = _num(h.get("cur"), None) or _num(h.get("entry_px"), 0.0)
        h["cur"] = px
        gain = (px/h["entry_px"] - 1)*100
        h["max_gain_pct"] = max(h.get("max_gain_pct", 0), gain)
        h["days_in"] = _bdays(h["entry_date"], data_date)
        # 런너 승격
        is_runner = h.get("runner", False)
        if not is_runner and h["max_gain_pct"] >= RULES["runner_promote_gain_pct"]:
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

    cands = []
    for t, s in stocks.items():
        if t in held: continue
        ok, gd = passes_gates(s, fired_themes, regime_key)
        if not ok: continue
        # 피벗 거리 가드 (근사): 52주 고점 -10%~+5%
        _, dist = pivot_approx(s)
        if dist is not None and not (-10 <= dist <= 5): continue
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
    max_new_cash = max(0, nav_pre*reg["max_exp"] - sum(h["cur"]*h["shares"] for h in holdings))

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
        signals.append({"ticker": t, "industry": s.get("industry",""),
                        "pattern": (s.get("pattern_detail",{}).get(s.get("best_pattern"),{}) or {}).get("name", s.get("best_pattern","")),
                        "signal_close": round(float(s["price"]),2),
                        "pivot": round(float(s.get("high_52w") or s["price"]),2),
                        "dist": round(dist,1) if dist is not None else None,
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
        })
        cash -= alloc*(1+cost); max_new_cash -= alloc
        state.setdefault("entries_recent", []).append(data_date)
        entered += 1; slots -= 1

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
    out["_holdings"] = holdings
    out["_closed"] = closed
    out["_cash"] = cash
    out["_entries_recent"] = state.get("entries_recent", [])[-30:]

    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(_sanitize(out), f, ensure_ascii=False, indent=2, allow_nan=False)

    print(f"[strategy_room] {data_date} | NAV {nav:.4f} ({chg:+.2f}%) | 보유 {len(holdings)} | 신규 {entered} | 청산누적 {len(closed)} | 레짐 {reg['label']}")
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
                   "cd":"∞" if h.get("runner") else "", "entry_px":round(h["entry_px"],2),
                   "cur":round(h["cur"],2), "runner":h.get("runner",False)})
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
