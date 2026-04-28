"""
phase_history.py — Phase 전환 추적 공통 모듈
====================================================
미국/한국 양 시장 공용. screener.py와 kr_screener.py에서 사용.

기능:
  - calc_phase(stock): dashboard.html의 calcPhase()를 1:1 Python 포팅
  - load_phase_history(path): JSON 로드 (7일 stale 자동 무효화)
  - save_phase_history(path, stocks, today_str): 오늘자 phase 저장
  - annotate_phase_changes(stocks, history): phase / phase_yesterday / phase_changed_up 주입
  - annotate_and_persist(stocks, path, today_str): 위 셋을 한 번에 처리

JSON 구조:
{
  "date": "2026-04-28",
  "phases": {
    "AAPL": "4plus",
    "MSFT": "4",
    ...
  }
}

Phase 전환 정의 (IBD pivot buy point):
  - phase_yesterday=3 → phase=4: 🚀 pivot buy point 당일 (가장 강한 신호)
  - phase_yesterday=2 → phase=3: 바닥 탈출 진행
  - phase_yesterday=67 → phase=5/4: 회복 시작
  - 첫날(history 없음): phase_changed_up=None (UI에서 '—' 표시)
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict

log = logging.getLogger(__name__)

# Phase 우선순위 (높을수록 강세). NEW↑ 판정에 사용.
# dashboard의 calcPhase() 반환값과 정확히 일치해야 함.
PHASE_RANK = {
    "01":    0,
    "67":    1,
    "2":     2,
    "3":     3,
    "5":     4,
    "4":     5,
    "4plus": 6,
}

# stale 처리 임계값 — 마지막 저장으로부터 N일 초과면 폐기
STALE_DAYS = 7


# ─────────────────────────────────────────────────────────────────────
# 1) calc_phase: dashboard.html의 calcPhase() 1:1 포팅
# ─────────────────────────────────────────────────────────────────────
def calc_phase(s: dict) -> str:
    """
    종목 dict를 받아 phase 문자열 반환.
    dashboard.html `function calcPhase(s)` 와 100% 동일한 로직.

    JS 원본:
        const dots = (s.pass_dots || []).filter(Boolean).length;
        const rs = s.rs || 0, acc = s.acc2 || s.acc, h52 = s.h52_new;
        if (s.is_stage2 && acc && h52 && rs >= 80) return '4plus';
        if (s.is_stage2 && acc && rs >= 70)        return '4';
        if (s.is_stage2 && rs >= 60)               return '5';
        if (dots >= 4 && rs >= 50)                 return '3';
        if (dots >= 2 && rs >= 35)                 return '2';
        if (rs < 35)                               return '01';
        return '67';
    """
    pass_dots = s.get("pass_dots") or []
    dots = sum(1 for d in pass_dots if d)
    rs = s.get("rs") or 0
    acc = s.get("acc2") or s.get("acc")
    h52 = s.get("h52_new")
    is_s2 = s.get("is_stage2")

    if is_s2 and acc and h52 and rs >= 80:
        return "4plus"
    if is_s2 and acc and rs >= 70:
        return "4"
    if is_s2 and rs >= 60:
        return "5"
    if dots >= 4 and rs >= 50:
        return "3"
    if dots >= 2 and rs >= 35:
        return "2"
    if rs < 35:
        return "01"
    return "67"


# ─────────────────────────────────────────────────────────────────────
# 2) load / save
# ─────────────────────────────────────────────────────────────────────
def load_phase_history(path: str) -> dict:
    """
    phase history JSON 로드. 파일 없거나 7일 초과 stale면 빈 dict 반환.

    Returns:
        dict: { "date": "...", "phases": {ticker: phase, ...} } or {} if stale/missing
    """
    if not os.path.exists(path):
        log.info(f"  📜 phase history 없음 (첫 실행): {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"  📜 phase history 로드 실패 — 빈 상태로 시작: {e}")
        return {}

    last_date_str = data.get("date")
    if not last_date_str:
        log.warning("  📜 phase history에 date 필드 없음 — 빈 상태로 시작")
        return {}

    try:
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
    except ValueError:
        log.warning(f"  📜 phase history date 파싱 실패: {last_date_str}")
        return {}

    age_days = (datetime.now().date() - last_date).days
    if age_days > STALE_DAYS:
        log.warning(f"  📜 phase history stale ({age_days}일 경과 > {STALE_DAYS}일) — 폐기")
        return {}

    phases = data.get("phases", {})
    log.info(f"  📜 phase history 로드: {last_date_str} ({len(phases)}종목, {age_days}일 전)")
    return data


def save_phase_history(path: str, stocks: List[dict], today_str: str) -> None:
    """
    오늘자 phase 저장. today_str는 YYYY-MM-DD 또는 YYYYMMDD.
    """
    # 날짜 정규화
    if len(today_str) == 8 and today_str.isdigit():
        # YYYYMMDD → YYYY-MM-DD
        date_iso = f"{today_str[:4]}-{today_str[4:6]}-{today_str[6:]}"
    else:
        date_iso = today_str

    phases = {}
    for s in stocks:
        ticker = s.get("ticker")
        phase = s.get("phase")  # annotate_phase_changes에서 이미 주입됨
        if ticker and phase:
            phases[ticker] = phase

    out = {
        "date": date_iso,
        "phases": phases,
    }

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        log.info(f"  📜 phase history 저장: {path} ({len(phases)}종목)")
    except OSError as e:
        log.warning(f"  📜 phase history 저장 실패: {e}")


# ─────────────────────────────────────────────────────────────────────
# 3) annotate
# ─────────────────────────────────────────────────────────────────────
def annotate_phase_changes(stocks: List[dict], history: dict) -> Tuple[int, bool]:
    """
    각 종목에 phase, phase_yesterday, phase_changed_up 필드 주입.

    Returns:
        (phase_up_count, phase_first_day)
        - phase_up_count: phase_changed_up=True인 종목 수
        - phase_first_day: history가 비어있어서 비교 불가능한 첫날인지 여부
    """
    yesterday_phases = history.get("phases", {}) if history else {}
    is_first_day = not bool(yesterday_phases)

    up_count = 0
    for s in stocks:
        # 1) 오늘 phase 계산
        today_phase = calc_phase(s)
        s["phase"] = today_phase

        # 2) 어제 phase
        ticker = s.get("ticker")
        yesterday_phase = yesterday_phases.get(ticker) if ticker else None
        s["phase_yesterday"] = yesterday_phase

        # 3) 전환 ↑ 판정
        if is_first_day or yesterday_phase is None:
            # 첫날이거나 어제 데이터에 없는 종목 → 비교 불가
            s["phase_changed_up"] = None
        else:
            today_rank = PHASE_RANK.get(today_phase, -1)
            yesterday_rank = PHASE_RANK.get(yesterday_phase, -1)
            changed_up = (today_rank > yesterday_rank)
            s["phase_changed_up"] = changed_up
            if changed_up:
                up_count += 1

    return up_count, is_first_day


# ─────────────────────────────────────────────────────────────────────
# 4) 편의 함수: load + annotate + save 한 방에
# ─────────────────────────────────────────────────────────────────────
def annotate_and_persist(stocks: List[dict], history_path: str, today_str: str) -> Tuple[int, bool]:
    """
    원샷 처리:
      1. history_path에서 어제 phase 로드 (없거나 stale이면 빈 상태)
      2. 모든 종목에 phase / phase_yesterday / phase_changed_up 주입
      3. 오늘자 phase를 history_path에 저장

    Returns:
        (phase_up_count, phase_first_day)
    """
    history = load_phase_history(history_path)
    up_count, first_day = annotate_phase_changes(stocks, history)
    save_phase_history(history_path, stocks, today_str)
    return up_count, first_day


# ─────────────────────────────────────────────────────────────────────
# 모듈 단위 셀프 테스트
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # 테스트용 가짜 종목
    fake_stocks_today = [
        {"ticker": "AAA", "is_stage2": True,  "acc2": True, "h52_new": True,  "rs": 95, "pass_dots": [1,1,1,1,1,1,1]},  # 4plus
        {"ticker": "BBB", "is_stage2": True,  "acc2": True, "h52_new": False, "rs": 75, "pass_dots": [1,1,1,1,1,0,0]},  # 4
        {"ticker": "CCC", "is_stage2": True,  "acc": False,"h52_new": False, "rs": 65, "pass_dots": [1,1,1,1,0,0,0]},  # 5
        {"ticker": "DDD", "is_stage2": False, "acc": False,"h52_new": False, "rs": 55, "pass_dots": [1,1,1,1,0,0,0]},  # 3
        {"ticker": "EEE", "is_stage2": False, "acc": False,"h52_new": False, "rs": 40, "pass_dots": [1,1,0,0,0,0,0]},  # 2
        {"ticker": "FFF", "is_stage2": False, "acc": False,"h52_new": False, "rs": 25, "pass_dots": []},                # 01
        {"ticker": "GGG", "is_stage2": False, "acc": False,"h52_new": False, "rs": 50, "pass_dots": [1]},               # 67
    ]

    print("\n=== Test 1: calc_phase() ===")
    for s in fake_stocks_today:
        print(f"  {s['ticker']}: {calc_phase(s)}")

    print("\n=== Test 2: 첫날 (history 없음) ===")
    test_path = "/tmp/test_phase_history.json"
    if os.path.exists(test_path):
        os.remove(test_path)
    up, first = annotate_and_persist(fake_stocks_today, test_path, "2026-04-28")
    print(f"  up_count={up}, first_day={first}")
    print(f"  AAA: phase={fake_stocks_today[0]['phase']} yesterday={fake_stocks_today[0]['phase_yesterday']} up={fake_stocks_today[0]['phase_changed_up']}")
    assert first is True
    assert up == 0
    assert all(s["phase_changed_up"] is None for s in fake_stocks_today)
    print("  ✅ PASS")

    print("\n=== Test 3: 둘째날 (어제 데이터 있음) ===")
    # 어제는 모두 한 단계 낮은 phase였다고 가정 → 모두 NEW↑
    fake_stocks_today_2 = [dict(s) for s in fake_stocks_today]  # deep enough copy
    # AAA가 어제는 4였는데 오늘은 4plus → up
    # BBB가 어제는 5였는데 오늘은 4 → up
    # CCC가 어제는 4plus였는데 오늘은 5 → down (NOT up)
    fake_history = {
        "date": "2026-04-27",
        "phases": {
            "AAA": "4",
            "BBB": "5",
            "CCC": "4plus",
            "DDD": "2",
            "EEE": "01",
            "FFF": "01",  # 같음 → not up
            "GGG": "67",  # 같음 → not up
        }
    }
    with open(test_path, "w") as f:
        json.dump(fake_history, f)
    up, first = annotate_and_persist(fake_stocks_today_2, test_path, "2026-04-28")
    print(f"  up_count={up}, first_day={first}")
    for s in fake_stocks_today_2:
        print(f"  {s['ticker']}: {s['phase_yesterday']} → {s['phase']} (up={s['phase_changed_up']})")
    assert first is False
    assert up == 4  # AAA, BBB, DDD, EEE up
    assert fake_stocks_today_2[0]["phase_changed_up"] is True   # AAA 4 → 4plus
    assert fake_stocks_today_2[1]["phase_changed_up"] is True   # BBB 5 → 4
    assert fake_stocks_today_2[2]["phase_changed_up"] is False  # CCC 4plus → 5 (down)
    assert fake_stocks_today_2[3]["phase_changed_up"] is True   # DDD 2 → 3
    assert fake_stocks_today_2[4]["phase_changed_up"] is True   # EEE 01 → 2
    assert fake_stocks_today_2[5]["phase_changed_up"] is False  # FFF 01 → 01 (same)
    print("  ✅ PASS")

    print("\n=== Test 4: stale 처리 ===")
    old_history = {
        "date": "2026-04-01",  # 27일 전
        "phases": {"AAA": "01"}
    }
    with open(test_path, "w") as f:
        json.dump(old_history, f)
    loaded = load_phase_history(test_path)
    assert loaded == {}, f"stale은 빈 dict 반환해야 하는데 {loaded}"
    print("  ✅ PASS")

    # 정리
    if os.path.exists(test_path):
        os.remove(test_path)

    print("\n🎉 모든 셀프 테스트 통과")
