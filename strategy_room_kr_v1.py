# -*- coding: utf-8 -*-
"""
strategy_room_kr_v1.py — 국장 전략실 forward 페이퍼 트레이딩 엔진
================================================================================
진입은 스탁이지, 청산·리스크는 전략실(v6.15) 로직.

  입력  stockeasy_kr.json      스탁이지 스냅샷 + diff (신규/이탈)
        kr_ohlcv_extra.json    일봉 파생지표 (enrich_kr.py 산출)
  출력  strategy_room_kr.json       cap 무제한 트랙 (기본)
        strategy_room_kr_cap12.json cap 12 비교 트랙

T+1 체결 모델:
    T일 종가로 신호·청산 판정 → T+1 정규장 시가에 체결.
    signal_px(판정가)와 exec_px(체결가)를 분리 기록해 갭 비용을 측정한다.
    NXT 미사용 — 정규장 단일 경로.

미장 엔진(strategy_room_v1.py)과 파일·상태를 완전히 분리한다.

실행:
    python strategy_room_kr_v1.py
    python strategy_room_kr_v1.py --source-exit exit    # 소스 이탈 시 청산
"""

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta

# ─── 파라미터 ────────────────────────────────────────────────────────
RULES = {
    "initial_capital": 1.0,
    "stop_pct": -7.0,
    "be_threshold_pct": 15.0,
    "lock_tiers": [[25, 10], [50, 25], [100, 70], [200, 150],
                   [300, 250], [400, 350], [500, 400]],
    "hold_days": 40,
    "oneill_threshold_pct": 20.0, "oneill_trigger_bdays": 15,
    "oneill_hold_bdays": 40,
    # 0 = 무제한. 스탁이지 종목을 전부 추종하는 것이 기본 트랙이다.
    "max_concurrent_positions": 0,
    # 원본에 없던 상한. 후보가 1~2개인 날 한 종목에 몰리는 것을 막는다.
    "max_weight_per_position": 1.0 / 12,
    "runner_promote_gain_pct": 100.0, "runner_cap": 5,
    "runner_trend_min_gain_pct": 40.0, "runner_trend_giveback_max": 0.30,
    "sell_grace_bdays": 10, "sell_ma50_break": True,
    # 소스(스탁이지) 이탈 처리: watch = 기록만 / exit = 청산
    #   진입은 스탁이지, 청산은 전략실이라는 원칙에 따라 기본은 watch.
    "source_exit_action": "watch",
    # 국내는 매도 시 증권거래세가 붙어 미국의 2배 이상. 세율은 주기적 확인 필요.
    "cost_bps": 12.0,
    "track": "stockeasy:momentum",
    "logic_version": "kr-v1.0 (base v6.15)",
}


# ─── 순수 로직 ───────────────────────────────────────────────────────
def bdays(d1, d2):
    """두 ISO 날짜 사이 영업일 (주말만 제외 · 공휴일 무시)."""
    try:
        a = datetime.strptime(d1, "%Y-%m-%d").date()
        b = datetime.strptime(d2, "%Y-%m-%d").date()
    except Exception:
        return 0
    if a > b:
        a, b = b, a
    n, cur = 0, a
    while cur < b:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def ratchet_stop(entry_px, max_gain_pct, R):
    """원본 _ratchet_stop 과 동일 — 종가 판정용 단조 상향 스탑."""
    sp = R["stop_pct"]
    if max_gain_pct >= R["be_threshold_pct"]:
        sp = max(sp, 0.0)
    for gain, lock in R["lock_tiers"]:
        if max_gain_pct >= gain:
            sp = max(sp, lock)
    return entry_px * (1 + sp / 100.0)


def judge_exit(h, px, ma50, R, source_dropped=False):
    """종가 판정. 우선순위: 하드스톱/래칫 > 50일선 > 소스이탈 > max hold."""
    stop = h["current_stop"]
    if px <= stop:
        if h["max_gain_pct"] < R["be_threshold_pct"]:
            return "손절"
        return "BE 청산" if stop <= h["entry_px"] * 1.001 else "Lock 청산"

    if (R["sell_ma50_break"] and h["days_in"] >= R["sell_grace_bdays"]
            and ma50 and px < ma50):
        return "50일선 이탈"

    if source_dropped and R["source_exit_action"] == "exit":
        if not (h.get("runner") or h.get("oneill_8w_active")):
            return "소스 이탈"

    if not h.get("runner"):
        cap = R["hold_days"]
        if h.get("oneill_8w_active"):
            cap = R["oneill_hold_bdays"] * 2
        if h["days_in"] >= cap:
            return "max hold"
    return None


def size_entries(cash, nav, n_new, R):
    """종목당 배분액. 원본 '가용현금÷신규수'에 NAV 상한을 얹는다."""
    if n_new <= 0 or cash <= 0:
        return 0.0
    cost = R["cost_bps"] / 10000.0
    per = cash / n_new / (1 + cost)
    return max(0.0, min(per, nav * R["max_weight_per_position"]))


# ─── 하루치 진행 ─────────────────────────────────────────────────────
def run_day(state, day, R):
    """1) 대기주문 시가 체결 → 2) 종가 평가 → 3) 청산 판정 → 4) 신규 신호."""
    D = day["date"]
    cost = R["cost_bps"] / 10000.0
    log = []

    # ── 1) 대기 주문 체결 (오늘 시가) ────────────────────────────
    still = []
    for od in state["pending"]:
        t = od["ticker"]
        op = day["opens"].get(t)
        if op is None or op <= 0:
            still.append(od)                  # 거래정지 등 → 다음날 재시도
            log.append(f"  [보류] {t} 시가 없음")
            continue
        gap = (op / od["signal_px"] - 1) * 100 if od.get("signal_px") else 0.0

        if od["action"] == "sell":
            h = next((x for x in state["holdings"] if x["ticker"] == t), None)
            if not h:
                continue
            proceeds = op * h["shares"] * (1 - cost)
            state["cash"] += proceeds
            state["closed"].append({
                "ticker": t, "name": h.get("name"), "entry": h["entry_date"],
                "exit": D, "days": h["days_in"],
                "entry_px": round(h["entry_px"], 2), "exit_px": round(op, 2),
                "signal_px": round(od["signal_px"], 2), "gap_pct": round(gap, 2),
                "shares": h["shares"],
                "realized": proceeds - h["entry_value"],
                "ret_pct": round((op / h["entry_px"] - 1) * 100, 2),
                "cat": od["reason"], "was_runner": bool(h.get("runner")),
                "max_gain_pct": round(h.get("max_gain_pct", 0), 1),
            })
            state["holdings"] = [x for x in state["holdings"] if x["ticker"] != t]
            log.append(f"  [매도] {h.get('name') or t} {od['reason']} "
                       f"@ {op:,.0f} (갭 {gap:+.2f}%)")
        else:
            alloc = min(od["alloc"], max(0.0, state["cash"] / (1 + cost)))
            if alloc <= 0:
                log.append(f"  [취소] {t} 현금 부족")
                continue
            shares = alloc / op
            state["cash"] -= alloc * (1 + cost)
            state["holdings"].append({
                "ticker": t, "name": od.get("name"), "entry_date": D,
                "entry_px": op, "signal_px": od["signal_px"],
                "gap_pct": round(gap, 2), "shares": shares,
                "entry_value": alloc * (1 + cost), "cur": op,
                "max_gain_pct": 0.0, "days_in": 0,
                "current_stop": op * (1 + R["stop_pct"] / 100),
                "runner": False, "oneill_8w_active": False,
            })
            log.append(f"  [매수] {od.get('name') or t} @ {op:,.0f} "
                       f"(신호가 {od['signal_px']:,.0f} · 갭 {gap:+.2f}%)")
    state["pending"] = still

    # ── 2) 평가 (오늘 종가) ──────────────────────────────────────
    for h in state["holdings"]:
        px = day["closes"].get(h["ticker"]) or h["cur"]
        h["cur"] = px
        gain = (px / h["entry_px"] - 1) * 100
        h["max_gain_pct"] = max(h.get("max_gain_pct", 0), gain)
        h["days_in"] = bdays(h["entry_date"], D)
        n_run = sum(1 for x in state["holdings"] if x.get("runner"))
        if not h.get("runner") and n_run < R["runner_cap"]:
            if h["max_gain_pct"] >= R["runner_promote_gain_pct"]:
                h["runner"], h["runner_tier"] = True, 1
            elif h["max_gain_pct"] >= R["runner_trend_min_gain_pct"]:
                gb = (h["max_gain_pct"] - gain) / max(h["max_gain_pct"], 1)
                ma = day["ma50"].get(h["ticker"])
                if ma and px >= ma and gb <= R["runner_trend_giveback_max"]:
                    h["runner"], h["runner_tier"] = True, 2
        if (not h.get("oneill_8w_active")
                and h["max_gain_pct"] >= R["oneill_threshold_pct"]
                and h["days_in"] <= R["oneill_trigger_bdays"]):
            h["oneill_8w_active"] = True
        h["current_stop"] = ratchet_stop(h["entry_px"], h["max_gain_pct"], R)

    # ── 3) 청산 판정 (오늘 종가) → 내일 큐 ──────────────────────
    queued = {o["ticker"] for o in state["pending"]}
    for h in state["holdings"]:
        if h["ticker"] in queued:
            continue
        cause = judge_exit(h, h["cur"], day["ma50"].get(h["ticker"]), R,
                           h["ticker"] in day["dropped"])
        if cause:
            state["pending"].append({"action": "sell", "ticker": h["ticker"],
                                     "reason": cause, "signal_date": D,
                                     "signal_px": h["cur"]})
            log.append(f"  [청산예약] {h.get('name') or h['ticker']} "
                       f"{cause} (종가 {h['cur']:,.0f})")

    # ── 4) 신규 신호 → 내일 큐 ──────────────────────────────────
    held = {h["ticker"] for h in state["holdings"]}
    sell_q = {o["ticker"] for o in state["pending"] if o["action"] == "sell"}
    cands = [s for s in day["new"]
             if s["ticker"] not in held and s["ticker"] not in sell_q]

    if R["max_concurrent_positions"]:
        core = len([h for h in state["holdings"] if not h.get("runner")])
        cands = cands[:max(0, R["max_concurrent_positions"] - core)]

    nav = state["cash"] + sum(h["cur"] * h["shares"] for h in state["holdings"])
    per = size_entries(state["cash"], nav, len(cands), R)
    for s in cands:
        if per <= nav * 0.005:            # 먼지 가드
            break
        state["pending"].append({"action": "buy", "ticker": s["ticker"],
                                 "name": s.get("name"), "signal_date": D,
                                 "signal_px": s["signal_px"], "alloc": per})
        log.append(f"  [매수예약] {s.get('name') or s['ticker']} 배분 {per:.4f}")

    nav = state["cash"] + sum(h["cur"] * h["shares"] for h in state["holdings"])
    state["nav"] = nav
    state["date"] = D
    nh = [r for r in state["nav_history"] if r.get("date") != D]
    nh.append({"date": D, "nav": round(nav, 6)})
    state["nav_history"] = nh[-500:]
    return log


# ─── 입출력 ──────────────────────────────────────────────────────────
def load_day(snap_path, extra_path):
    """스탁이지 스냅샷 + 일봉 파생지표 → run_day 가 먹는 형태로."""
    if not os.path.exists(snap_path):
        sys.exit(f"[!] {snap_path} 없음 — stockeasy_fetch.py 를 먼저 실행하세요.")
    if not os.path.exists(extra_path):
        sys.exit(f"[!] {extra_path} 없음 — enrich_kr.py 를 먼저 실행하세요.")

    snap = json.load(open(snap_path, encoding="utf-8"))
    extra = json.load(open(extra_path, encoding="utf-8")).get("data", {})

    opens, closes, ma50 = {}, {}, {}
    for t, m in extra.items():
        if m.get("open"):
            opens[t] = m["open"]
        if m.get("price"):
            closes[t] = m["price"]
        if m.get("ma50"):
            ma50[t] = m["ma50"]

    d = snap.get("diff", {})
    new = [{"ticker": r["ticker"], "name": r.get("name"),
            "signal_px": r.get("src_entry_px") or r.get("cur")}
           for r in d.get("new", [])
           if r.get("ticker") and (r.get("src_entry_px") or r.get("cur"))]
    dropped = {r["ticker"] for r in d.get("dropped", []) if r.get("ticker")}

    return {"date": snap["data_date"], "opens": opens, "closes": closes,
            "ma50": ma50, "new": new, "dropped": dropped}, snap


def new_state():
    return {"cash": RULES["initial_capital"], "holdings": [], "closed": [],
            "pending": [], "nav_history": [], "nav": RULES["initial_capital"],
            "date": None}


def load_state(path):
    if not os.path.exists(path):
        return new_state()
    try:
        p = json.load(open(path, encoding="utf-8"))
        st = new_state()
        st.update({"cash": p.get("_cash", RULES["initial_capital"]),
                   "holdings": p.get("_holdings", []),
                   "closed": p.get("_closed", []),
                   "pending": p.get("_pending", []),
                   "nav_history": p.get("nav_history", []),
                   "nav": p.get("nav", RULES["initial_capital"]),
                   "date": p.get("data_date")})
        return st
    except Exception as e:
        print(f"[state] 로드 실패({e}) — 빈 상태로 시작")
        return new_state()


def build_output(state, R, day):
    nav = state["nav"]
    closed = state["closed"]
    wins = [c for c in closed if c.get("ret_pct", 0) > 0]
    gaps = [c["gap_pct"] for c in closed if c.get("gap_pct") is not None]
    return {
        "data_date": state["date"], "track": R["track"],
        "logic_version": R["logic_version"],
        "cap": R["max_concurrent_positions"] or "무제한",
        "source_exit_action": R["source_exit_action"],
        "nav": round(nav, 4),
        "ret_pct": round((nav / R["initial_capital"] - 1) * 100, 2),
        "cash": round(state["cash"], 4),
        "n_holdings": len(state["holdings"]),
        "n_pending": len(state["pending"]),
        "runner_count": sum(1 for h in state["holdings"] if h.get("runner")),
        "trades": {
            "n": len(closed),
            "winrate": round(len(wins) / len(closed) * 100, 1) if closed else None,
            "avg_ret": round(sum(c["ret_pct"] for c in closed) / len(closed), 2)
                       if closed else None,
            "avg_gap": round(sum(gaps) / len(gaps), 3) if gaps else None,
        },
        "holdings": [{"ticker": h["ticker"], "name": h.get("name"),
                      "entry": h["entry_date"], "days": h["days_in"],
                      "entry_px": round(h["entry_px"], 2),
                      "cur": round(h["cur"], 2),
                      "ret_pct": round((h["cur"] / h["entry_px"] - 1) * 100, 2),
                      "stop": round(h["current_stop"], 2),
                      "max_gain_pct": round(h.get("max_gain_pct", 0), 1),
                      "runner": bool(h.get("runner")),
                      "weight": round(h["cur"] * h["shares"] / nav * 100, 2)
                                if nav else None}
                     for h in state["holdings"]],
        "pending": state["pending"],
        "closed": closed[-50:],
        "nav_history": state["nav_history"],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        # 내부 상태 (다음 실행이 읽는다)
        "_cash": state["cash"], "_holdings": state["holdings"],
        "_closed": closed, "_pending": state["pending"],
    }


def run_track(day, out_path, R, label):
    state = load_state(out_path)
    if state["date"] == day["date"]:
        print(f"[{label}] {day['date']} 이미 처리됨 — 스킵")
        return None
    print(f"\n═══ {label} (cap {R['max_concurrent_positions'] or '무제한'}) ═══")
    for line in run_day(state, day, R):
        print(line)
    out = build_output(state, R, day)
    json.dump(out, open(out_path, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"  NAV {out['nav']} ({out['ret_pct']:+.2f}%) · 현금 {out['cash']} · "
          f"보유 {out['n_holdings']} · 대기 {out['n_pending']}")
    if out["trades"]["n"]:
        t = out["trades"]
        print(f"  누적 {t['n']}건 · 승률 {t['winrate']}% · "
              f"평균 {t['avg_ret']:+.2f}% · 평균갭 {t['avg_gap']:+.3f}%")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snap", default="stockeasy_kr.json")
    ap.add_argument("--extra", default="kr_ohlcv_extra.json")
    ap.add_argument("--out", default="strategy_room_kr.json")
    ap.add_argument("--out-cap12", default="strategy_room_kr_cap12.json")
    ap.add_argument("--source-exit", default="watch", choices=["watch", "exit"])
    ap.add_argument("--no-cap12", action="store_true", help="비교 트랙 생략")
    a = ap.parse_args()

    day, snap = load_day(a.snap, a.extra)
    print(f"[입력] {day['date']} · 신규 {len(day['new'])} · "
          f"소스이탈 {len(day['dropped'])} · 시가 {len(day['opens'])}종목")

    R = dict(RULES, source_exit_action=a.source_exit)
    run_track(day, a.out, R, "기본")

    if not a.no_cap12:
        R12 = dict(R, max_concurrent_positions=12)
        run_track(day, a.out_cap12, R12, "비교")


if __name__ == "__main__":
    main()
