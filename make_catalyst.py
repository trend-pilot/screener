# -*- coding: utf-8 -*-
"""
make_catalyst.py — 전략실(Phase 6) 카탈리스트 주입 단계
================================================================================

파이프라인 순서:
    1) python screener.py          → output/screener_data.json (stocks + 일간 pct)
    2) python enrich_sectors.py     → 위 JSON 에 sector / industry 추가
    3) python make_catalyst.py      → 위 JSON 에 "catalyst" 키 주입  ← 이 파일
    4) (GitHub 업로드 / 커밋)

catalyst 는 업종(industry)별 일간 발화를 보므로, 반드시 enrich_sectors.py 가
sector/industry 를 채운 "다음에" 실행해야 한다 (그래야 클러스터가 잡힌다).
industry 가 없으면 catalyst.py 가 자동으로 sector 로 폴백한다.

catalyst_state.json (연속 발화 D+N 누적):
    phase_history.json 과 동일하게 매일 repo 에 커밋되어야 D+N 이 쌓인다.
    GitHub Actions 커밋 대상에 catalyst_state.json 을 추가할 것.
"""

import json
import os
import sys

from catalyst import build_catalyst

DEFAULT_PATH = os.path.join("output", "screener_data.json")
STATE_PATH = "catalyst_state.json"


def main(path=DEFAULT_PATH):
    if not os.path.exists(path):
        print(f"[make_catalyst] 파일 없음: {path} — screener.py 를 먼저 실행하세요.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stocks = data.get("stocks", [])
    if not stocks:
        print("[make_catalyst] stocks 비어있음. 중단.")
        sys.exit(1)

    meta = data.get("meta", {})
    data_date = (meta.get("updated_at") or "")[:10] or None

    catalyst_data = build_catalyst(
        stocks,
        data_date=data_date,
        state_path=STATE_PATH,
    )
    data["catalyst"] = catalyst_data

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    cb = catalyst_data.get("cluster_by")
    print(f"[make_catalyst] 발화 클러스터 {catalyst_data['all_fired_count']}개 "
          f"(기준: {cb}, faded {len(catalyst_data['faded_clusters'])}개) → {path}")
    for c in catalyst_data["clusters"]:
        print(f"   {c['sustain_label']:>9}  {c['cluster']:<28} "
              f"avg {c['avg_pct']:>5.2f}%  lead {c['lead']} {c['lead_pct']:>5.2f}%")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH)
