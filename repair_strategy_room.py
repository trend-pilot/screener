# -*- coding: utf-8 -*-
"""
repair_strategy_room.py — NaN 오염된 strategy_room.json 1회성 복구
=====================================================================
[무슨 일이 있었나]
  screener_data.json 의 일부 종목 가격이 NaN 으로 기록되었고
  (예: 가격 이력이 없는 우선주), 전략실 엔진이
      px = float(s["price"]) if s and s.get("price") else ...
  로 값을 받았다. 파이썬에서 NaN 은 truthy 라 이 분기를 통과해
  px=NaN → shares=NaN → NAV=NaN 으로 상태가 오염됐다.
  NaN 은 표준 JSON 이 아니라서 브라우저 JSON.parse 가 실패하고,
  대시보드는 조용히 '샘플 데이터'로 폴백했다 (겉보기 날짜 2026-06-24).

[이 스크립트가 하는 일]
  1) nav_history 를 '마지막 정상(유한) NAV' 까지 잘라낸다.
  2) shares 가 NaN 인 보유 종목을 진입일 NAV 로 역산 복구한다.
       엔진 규칙: target_per = nav_at_entry / cap  (균등가중)
       → shares = (nav_at_entry / cap) / entry_px
     진입일 NAV 를 못 찾으면 마지막 정상 NAV 를 사용한다.
  3) _cash 를 재설정한다 (기본: 완전투자 가정 → 0).
  4) 모든 NaN/Inf 를 None 으로 치환하고 allow_nan=False 로 저장한다.

[사용법]
    python repair_strategy_room.py strategy_room.json
    python repair_strategy_room.py strategy_room.json --cash 0.05   # 현금 지정

  실행 후 strategy_room.json.bak 백업이 남는다.
"""

import json
import math
import shutil
import sys

CAP = 12  # 엔진 RULES["cap"] 과 동일 (균등가중 슬롯 수)


def finite(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def sanitize(o):
    if isinstance(o, float):
        return None if not math.isfinite(o) else o
    if isinstance(o, dict):
        return {k: sanitize(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [sanitize(v) for v in o]
    return o


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "strategy_room.json"
    cash_override = None
    if "--cash" in sys.argv:
        cash_override = float(sys.argv[sys.argv.index("--cash") + 1])

    with open(path, encoding="utf-8") as f:
        sr = json.load(f)          # 파이썬 json 은 NaN 을 읽을 수 있다

    shutil.copy(path, path + ".bak")
    print(f"백업: {path}.bak")

    # ── 1) nav_history 절단 ─────────────────────────────────────────
    nh = sr.get("nav_history") or []
    good = [r for r in nh if finite(r.get("nav")) is not None]
    last_good_nav = finite(good[-1]["nav"]) if good else 1.0
    last_good_date = good[-1]["date"] if good else "?"
    print(f"마지막 정상 NAV: {last_good_nav} ({last_good_date}) "
          f"— nav_history {len(nh)} → {len(good)}건으로 절단")
    sr["nav_history"] = good

    # 진입일 → NAV 매핑 (역산용)
    nav_by_date = {r["date"]: finite(r["nav"]) for r in good}

    def nav_at(d):
        if d in nav_by_date and nav_by_date[d]:
            return nav_by_date[d]
        # 진입일 이전의 가장 최근 정상 NAV
        prev = [r for r in good if r["date"] <= str(d)]
        return finite(prev[-1]["nav"]) if prev else last_good_nav

    # ── 2) 보유 종목 shares 복구 ────────────────────────────────────
    fixed, dropped = 0, []
    for key in ("holdings", "_holdings"):
        for h in (sr.get(key) or []):
            sh = finite(h.get("shares"))
            epx = finite(h.get("entry_px"))
            cur = finite(h.get("cur")) or epx
            if sh is None:
                if epx and epx > 0:
                    est_nav = nav_at(h.get("entry_date"))
                    h["shares"] = round((est_nav / CAP) / epx, 6)
                    fixed += 1
                else:
                    dropped.append(h.get("ticker"))
                    continue
            if cur:
                h["cur"] = cur
            # 진입 원가(entry_value) 재계산 — shares 복구 시 함께 깨져 있음
            sh2, epx2 = finite(h.get("shares")), finite(h.get("entry_px"))
            if finite(h.get("entry_value")) is None and sh2 and epx2:
                h["entry_value"] = round(sh2 * epx2, 6)
            # 파생 필드 정리
            for f2 in ("max_gain_pct", "current_stop", "days_in"):
                if finite(h.get(f2)) is None and f2 in h:
                    h[f2] = 0
        if dropped:
            sr[key] = [h for h in (sr.get(key) or []) if h.get("ticker") not in dropped]

    print(f"shares 복구: {fixed}건" + (f" · 복구불가 제거: {dropped}" if dropped else ""))

    # ── 3) 현금 재설정 ──────────────────────────────────────────────
    hold_val = 0.0
    for h in (sr.get("_holdings") or sr.get("holdings") or []):
        c, s2 = finite(h.get("cur")), finite(h.get("shares"))
        if c and s2:
            hold_val += c * s2
    cash = cash_override if cash_override is not None else 0.0
    sr["_cash"] = round(cash, 6)
    new_nav = round(hold_val + cash, 6)
    sr["nav"] = new_nav
    print(f"보유평가액 {hold_val:.4f} + 현금 {cash:.4f} → NAV {new_nav}")
    print(f"  (마지막 정상 NAV {last_good_nav} 대비 {(new_nav/last_good_nav-1)*100:+.1f}%)")

    # ── 4) 전체 NaN 제거 후 저장 ────────────────────────────────────
    sr = sanitize(sr)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sr, f, ensure_ascii=False, indent=2, allow_nan=False)

    # 검증
    with open(path, encoding="utf-8") as f:
        json.load(f)
    txt = open(path, encoding="utf-8").read()
    assert "NaN" not in txt, "NaN 이 남아있습니다"
    print(f"✅ 저장 완료 — 표준 JSON 유효 (NaN 0개)")


if __name__ == "__main__":
    main()
