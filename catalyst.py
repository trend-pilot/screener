# -*- coding: utf-8 -*-
"""
catalyst.py — TrendPilot 전략실(Strategy Room)용 테마 카탈리스트 빌더
================================================================================

목적
----
screener.py 가 이미 만들어내는 stocks 리스트(= screener_data.json 의 "stocks")를
입력으로 받아, 샘플 대시보드의 `window.__CATALYST` 스키마와 동일한 dict 를 만든다.

이 dict 를 screener_data.json 에 "catalyst" 키로 끼워 넣으면, dashboard.html 의
전략실 탭(Phase 6)이 그대로 읽어서 렌더할 수 있다.

핵심 개념 (샘플 note 그대로)
----------------------------
- cluster 발화 = 그날 업종(industry)별 avg_pct 상위 top_n (floor min_fire_avg%
  또는 lead ≥ strong_lead_pct%).
- sustain = 연속 발화 일수 (D+N). ★★★ = D+3(연속 3일) 이상 메인 테마.
- cascade = 업종 내 상승 종목 비율이 cascade_ratio 이상 (소수 lead 가 아닌 광범위 상승).
- faded = 어제 발화했으나 오늘 발화하지 못한 업종.
- catalyst(텍스트) = 웹 검색 기반 해설 (선택, 기본 OFF — ANTHROPIC_API_KEY 필요).

연동 방법 (screener.py 에 2줄)
-----------------------------
    from catalyst import build_catalyst
    ...
    data = {"stocks": stocks, "market": market, "meta": meta}
    data["catalyst"] = build_catalyst(
        stocks,
        data_date=meta["updated_at"][:10],     # 'YYYY-MM-DD'
        state_path="catalyst_state.json",       # 지속도 누적용 (repo 에 커밋되어 매일 갱신)
        holdings=my_positions_tickers,          # 선택: 보유 종목 set/list (없으면 None)
    )
    json.dump(data, f, ensure_ascii=False)

state_path("catalyst_state.json")
---------------------------------
어제까지의 연속 발화 이력을 저장하는 작은 JSON. phase_history.py 와 같은 철학.
GitHub Actions 가 매일 이 파일을 읽고 → 갱신 → 다시 커밋하면 D+N 이 누적된다.
처음 실행 시 파일이 없으면 모든 클러스터가 D+0(첫 발화)로 시작한다.

종목 필드 자동 감지
-------------------
스크리너마다 필드명이 달라서, 아래 후보 목록에서 자동으로 찾는다.
daily % 변화(FIELD_PCT)만큼은 필수 — 없으면 명확한 에러를 던진다.
필드명이 다르면 FIELDS 의 후보 목록에 추가만 하면 된다.
"""

import json
import os
from datetime import datetime, date

# ──────────────────────────────────────────────────────────────────────────────
# 종목 dict 필드명 후보 (앞에서부터 먼저 발견되는 것을 사용)
# ──────────────────────────────────────────────────────────────────────────────
FIELDS = {
    "ticker":   ["ticker", "symbol", "code"],
    "name":     ["name", "company", "company_name", "longName"],
    "industry": ["industry", "sub_industry", "industry_name", "gics_industry"],
    "sector":   ["sector", "gics_sector"],
    "rs":       ["rs", "rs_rating", "ibd_rs"],
    "pct":      ["pct", "pct_change", "change_pct", "chg", "chg_pct",
                 "day_change", "daily_change", "pct_chg", "change", "chg1d"],
    # 선택 필드 (없으면 null 처리)
    "earnings_surprise": ["earnings_surprise", "eps_surprise", "surprise_pct"],
    "days_to_earnings":  ["days_to_earnings", "days_to_er", "next_earnings_days"],
    "earnings_flag":     ["earnings_flag", "er_flag"],
}

DEFAULT_THRESHOLDS = {
    "min_members": 2,        # 클러스터 최소 종목 수
    "min_fire_avg": 1.0,     # 발화 floor: avg_pct ≥ 1.0%
    "strong_lead_pct": 8.0,  # lead 단독 발화: lead_pct ≥ 8.0%
    "top_n": 8,              # 발화 클러스터 상위 N개만 채택
    "cascade_ratio": 0.5,    # 광범위 상승 판정 비율
}

SCHEMA_VERSION = 1


# ──────────────────────────────────────────────────────────────────────────────
# 필드 접근 헬퍼
# ──────────────────────────────────────────────────────────────────────────────
def _pick(stock, key, default=None):
    """FIELDS[key] 후보 중 stock 에 존재하는 첫 값을 반환."""
    for cand in FIELDS.get(key, []):
        if cand in stock and stock[cand] not in (None, ""):
            return stock[cand]
    return default


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _detect_pct_field(stocks):
    """stocks 전체에서 일간 % 변화 필드가 실제로 존재하는지 확인."""
    for s in stocks:
        for cand in FIELDS["pct"]:
            if cand in s and s[cand] not in (None, ""):
                return cand
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 지속도(sustain) 상태 파일 입출력
# ──────────────────────────────────────────────────────────────────────────────
def _load_state(state_path):
    if state_path and os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_state(state_path, state):
    if not state_path:
        return
    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=0)
    except Exception as e:
        print(f"[catalyst] state 저장 실패: {e}")


def _sustain_label(consecutive_days):
    """연속 발화 일수 → ★ 라벨. D+N = consecutive_days - 1."""
    d = max(0, consecutive_days - 1)
    if consecutive_days >= 3:
        stars = "★★★"
    elif consecutive_days == 2:
        stars = "★★"
    else:
        stars = "★"
    return f"{stars} D+{d}"


def _sustain_status(consecutive_days):
    if consecutive_days >= 3:
        return "elevation"      # 메인 테마(연속 3일+)
    if consecutive_days == 2:
        return "building"
    return "new"


# ──────────────────────────────────────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────────────────────────────────────
def build_catalyst(stocks, *, data_date=None, prev_date=None,
                   state_path="catalyst_state.json", holdings=None,
                   thresholds=None, catalyst_texts=None):
    """
    stocks          : screener_data.json 의 "stocks" 리스트 (dict 들)
    data_date       : 'YYYY-MM-DD' (없으면 오늘)
    prev_date       : 직전 거래일 (표시용, 없으면 None)
    state_path      : 지속도 누적 JSON 경로 (None 이면 지속도 D+0 고정)
    holdings        : 보유 종목 티커 set/list (없으면 None)
    thresholds      : DEFAULT_THRESHOLDS 오버라이드 dict
    catalyst_texts  : {cluster_name: "해설 텍스트"} — 외부에서 채워 넣으면 그대로 사용
    """
    th = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        th.update(thresholds)

    if not data_date:
        data_date = date.today().isoformat()
    holdings_set = set(holdings or [])

    # ── 0) 일간 % 변화 필드 확인 (필수) ───────────────────────────────────────
    pct_field = _detect_pct_field(stocks)
    if pct_field is None:
        raise ValueError(
            "[catalyst] 일간 % 변화 필드를 찾지 못했습니다. "
            "screener.py 의 각 종목 dict 에 일간 등락률(예: 'pct')을 추가하거나 "
            "catalyst.FIELDS['pct'] 후보 목록에 실제 필드명을 넣어주세요."
        )

    # ── 1) 업종(industry)별 클러스터링 ────────────────────────────────────────
    # industry 가 거의 없으면 sector 로 폴백 (enrich 가 sector 만 채운 경우)
    has_industry = sum(1 for s in stocks if _pick(s, "industry")) >= max(3, len(stocks) * 0.1)
    cluster_key = "industry" if has_industry else "sector"

    clusters_raw = {}
    for s in stocks:
        ind = _pick(s, cluster_key)
        if not ind:
            continue
        clusters_raw.setdefault(ind, []).append(s)

    # ── 2) 클러스터별 지표 계산 ───────────────────────────────────────────────
    candidates = []
    for ind, members in clusters_raw.items():
        if len(members) < th["min_members"]:
            continue
        rows = []
        for s in members:
            rows.append({
                "symbol": _pick(s, "ticker", "?"),
                "pct": round(_num(s.get(pct_field)), 2),
                "rs": int(_num(_pick(s, "rs", 0))),
                "holding": _pick(s, "ticker") in holdings_set,
                "_name": _pick(s, "name", ""),
                "_sector": _pick(s, "sector", ""),
                "_es": _pick(s, "earnings_surprise"),
                "_dte": _pick(s, "days_to_earnings"),
                "_eflag": _pick(s, "earnings_flag"),
            })
        rows.sort(key=lambda r: r["pct"], reverse=True)

        n = len(rows)
        avg_pct = round(sum(r["pct"] for r in rows) / n, 2)
        up_count = sum(1 for r in rows if r["pct"] > 0)
        lead = rows[0]
        lead_pct = lead["pct"]
        cascade = (up_count / n) >= th["cascade_ratio"]
        single_stock = up_count <= 1

        fired = (avg_pct >= th["min_fire_avg"]) or (lead_pct >= th["strong_lead_pct"])
        if not fired:
            continue

        sector = next((r["_sector"] for r in rows if r["_sector"]), "")
        holdings_in = [r["symbol"] for r in rows if r["holding"]]

        candidates.append({
            "cluster": ind,
            "n": n,
            "avg_pct": avg_pct,
            "lead": lead["symbol"],
            "lead_pct": lead_pct,
            "lead_company": lead["_name"],
            "lead_rs": lead["rs"],
            "up_count": up_count,
            "cascade": cascade,
            "single_stock": single_stock,
            "members": [
                {"symbol": r["symbol"], "pct": r["pct"], "rs": r["rs"], "holding": r["holding"]}
                for r in rows[:5]
            ],
            "holdings": holdings_in,
            "lead_earnings": {
                "earnings_flag": lead["_eflag"],
                "earnings_surprise": lead["_es"],
                "days_to_earnings": lead["_dte"],
            },
            "sector": sector,
        })

    # ── 3) 발화 상위 top_n 채택 (avg_pct 내림차순) ────────────────────────────
    candidates.sort(key=lambda c: c["avg_pct"], reverse=True)
    fired_clusters = candidates[: th["top_n"]]
    fired_names = {c["cluster"] for c in fired_clusters}

    # ── 4) 지속도(sustain) 계산 — 상태 파일 기반 ─────────────────────────────
    state = _load_state(state_path)
    prev_fired = set(state.get("_fired_names", []))
    new_state = {"_date": data_date, "_fired_names": sorted(fired_names)}

    for c in fired_clusters:
        name = c["cluster"]
        prev = state.get(name, {})
        prev_consec = int(prev.get("consecutive_days", 0))
        ever = bool(prev.get("ever_fired", False))
        prev_avg = prev.get("last_avg_pct", None)

        if name in prev_fired:
            consecutive_days = prev_consec + 1
            first_fired = prev.get("first_fired", data_date)
            is_refire = False
        else:
            consecutive_days = 1
            first_fired = data_date
            is_refire = ever            # 과거에 발화한 적 있는데 끊겼다 재발화
        is_first_ever = not ever
        decelerating = (prev_avg is not None) and (c["avg_pct"] < float(prev_avg))

        c["consecutive_days"] = consecutive_days
        c["sustain_label"] = _sustain_label(consecutive_days)
        c["sustain_status"] = _sustain_status(consecutive_days)
        c["first_fired"] = first_fired
        c["is_first_ever"] = is_first_ever
        c["is_refire"] = is_refire
        c["decelerating"] = decelerating

        new_state[name] = {
            "consecutive_days": consecutive_days,
            "first_fired": first_fired,
            "last_avg_pct": c["avg_pct"],
            "ever_fired": True,
        }

    # 과거 ever_fired 이력 보존 (오늘 발화 안 했어도 ever 플래그 유지)
    for name, prev in state.items():
        if name.startswith("_"):
            continue
        if name not in new_state and prev.get("ever_fired"):
            new_state[name] = {
                "consecutive_days": 0,
                "first_fired": prev.get("first_fired"),
                "last_avg_pct": prev.get("last_avg_pct"),
                "ever_fired": True,
            }

    _save_state(state_path, new_state)

    # ── 5) faded: 어제 발화했으나 오늘 미발화 ────────────────────────────────
    faded = sorted(prev_fired - fired_names)

    # ── 6) 보유 종목이 포함된 발화 클러스터 ──────────────────────────────────
    holdings_fired = [c["cluster"] for c in fired_clusters if c["holdings"]]

    # ── 7) catalyst 텍스트 (선택) ─────────────────────────────────────────────
    catalysts = {}
    n_searches = 0
    for c in fired_clusters:
        name = c["cluster"]
        if catalyst_texts and name in catalyst_texts:
            catalysts[name] = {"ok": True, "text": catalyst_texts[name]}
            n_searches += 1
        else:
            catalysts[name] = {"ok": False, "error": "ANTHROPIC_API_KEY 미설정"}

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_date": data_date,
        "prev_date": prev_date,
        "thresholds": th,
        "cluster_by": cluster_key,
        "clusters": fired_clusters,
        "all_fired_count": len(fired_clusters),
        "faded_clusters": faded,
        "holdings_fired": holdings_fired,
        "catalysts": catalysts,
        "note": ("cluster 발화 = 그날 avg_pct 상위 {top_n} (floor {floor}% 또는 lead ≥ {lead}%). "
                 "sustain = 연속 발화 일수 (D+N, ★★★=D+3 이상 메인 테마). "
                 "catalyst = web 검색 (선택)."
                 ).format(top_n=th["top_n"], floor=th["min_fire_avg"], lead=th["strong_lead_pct"]),
        "catalyst_usage": {"date": data_date, "searches": n_searches},
    }


# ──────────────────────────────────────────────────────────────────────────────
# 단독 실행 테스트: python catalyst.py screener_data.json
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "screener_data.json"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    stocks = data.get("stocks", [])
    meta = data.get("meta", {})
    dd = (meta.get("updated_at") or "")[:10] or None
    cat = build_catalyst(stocks, data_date=dd, state_path="catalyst_state.json")
    print(f"발화 클러스터: {cat['all_fired_count']}개")
    for c in cat["clusters"]:
        print(f"  {c['sustain_label']:>9}  {c['cluster']:<30} "
              f"avg {c['avg_pct']:>5.2f}%  lead {c['lead']} {c['lead_pct']:>5.2f}%  "
              f"(n={c['n']}, up={c['up_count']}, cascade={c['cascade']})")
    print(f"faded: {cat['faded_clusters']}")
