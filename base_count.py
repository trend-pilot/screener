# -*- coding: utf-8 -*-
"""
base_count.py — MarketSurge 스타일 베이스 카운트 분석
=====================================================================
샘플 차트덱(차트덱_15종목_2026-07-05)의 산출물 61개 베이스를 역산해
동일한 스키마·규칙으로 베이스를 탐지한다.

[샘플에서 실측 확정된 규칙]
  · pivot  = left_high + $0.10      ← 61/61 전부 일치 (오닐 "피벗 10센트 위")
  · depth% = (left_high - low) / left_high × 100   ← 최대 오차 0.079%p
  · Flat Base 는 depth 8.8~19.9% 구간에 분포 (20% 미만)
  · Cup Without Handle 은 depth ≥ 20% 만 존재
  · 베이스 최소 길이 5주 (오닐 표준)

[출력 스키마] — 샘플과 동일
  {stage, stage_n, type, status, left_date, left_high,
   low_date, low, brk_date, pivot, depth, len_w, prior_up}
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional, Sequence

# ── 탐지 파라미터 ─────────────────────────────────────────────────────
MIN_BASE_WEEKS = 5        # 오닐 최소 베이스 길이
MAX_BASE_WEEKS = 40       # 이보다 길면 베이스가 아니라 장기 하락/횡보로 본다.
#   (65주였을 때 DK 의 2024년 하락 구간 전체가 '52주 베이스' 하나로 잡혀
#    앞쪽 실제 베이스 2개를 삼켰다. 그리드 서치 결과 40주가 최적)
PIVOT_OFFSET = 0.10       # 실측: pivot = left_high + $0.10
FLAT_MAX_DEPTH = 20.0     # Flat Base 상한 (실측 19.9%)
MAX_BASE_DEPTH = 55.0     # 이보다 깊으면 베이스가 아니라 추세 붕괴 (그리드 서치 최적)
RESET_DECLINE = 20.0      # 직전 고점 대비 이 이상 하락 → 베이스 카운트 리셋
ADVANCE_MIN = 20.0        # 직전 돌파 대비 이 이상 상승 → 카운트 전진


def _d(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _weeks(a: str, b: str) -> int:
    return max(0, round((_d(b) - _d(a)).days / 7.0) - 1)


@dataclass
class Base:
    stage: str
    stage_n: int
    type: str
    status: str
    left_date: str
    left_high: float
    low_date: str
    low: float
    brk_date: Optional[str]
    pivot: float
    depth: float
    len_w: int
    prior_up: Optional[float]


# ── 1) 주봉 변환 ──────────────────────────────────────────────────────
def to_weekly(dates: Sequence[str], h: Sequence[float],
              l: Sequence[float], c: Sequence[float]):
    """일봉 → 주봉 (금요일 기준 마지막 거래일로 집계)."""
    wk: dict = {}
    order: List[str] = []
    for i, ds in enumerate(dates):
        dt = _d(ds)
        key = (dt.isocalendar().year, dt.isocalendar().week)
        if key not in wk:
            wk[key] = {"date": ds, "h": h[i], "l": l[i], "c": c[i]}
            order.append(key)
        else:
            w = wk[key]
            w["date"] = ds
            w["h"] = max(w["h"], h[i])
            w["l"] = min(w["l"], l[i])
            w["c"] = c[i]
    return [wk[k] for k in order]


# ── 2) 베이스 타입 분류 ───────────────────────────────────────────────
def classify(depth: float, seg_c: Sequence[float], low_idx: int) -> str:
    """
    실측 분포에 맞춘 분류:
      depth < 20%            → Flat Base
      깊고 저점이 중앙부·U자  → Cup (오른쪽 끝 눌림 있으면 With Handle)
      그 외                  → Consolidation
    """
    n = len(seg_c)
    if depth < FLAT_MAX_DEPTH:
        return "Flat Base"
    if n < 5:
        return "Consolidation"

    pos = low_idx / max(1, n - 1)
    # U자 판정: 저점이 중앙부(0.25~0.75) + 좌우가 모두 저점보다 충분히 높음
    lo = seg_c[low_idx]
    left_pk = max(seg_c[: low_idx + 1]) if low_idx > 0 else lo
    right_pk = max(seg_c[low_idx:]) if low_idx < n - 1 else lo
    u_shape = (0.25 <= pos <= 0.78
               and left_pk > lo * 1.12 and right_pk > lo * 1.12)
    if not u_shape:
        return "Consolidation"

    # 핸들: 우측 고점 이후 마지막 구간에서 소폭(3~15%) 눌림
    r_idx = low_idx + max(range(len(seg_c[low_idx:])),
                          key=lambda k: seg_c[low_idx + k])
    tail = seg_c[r_idx:]
    if len(tail) >= 2:
        pull = (max(tail) - min(tail)) / max(tail) * 100
        if 3.0 <= pull <= 15.0 and len(tail) >= 2:
            return "Cup With Handle"
    return "Cup Without Handle"


# ── 3) 베이스 탐지 ────────────────────────────────────────────────────
def detect_bases(dates, h, l, c) -> List[Base]:
    """
    주봉 기준으로 '고점(left) → 조정 저점(low) → 피벗 돌파(brk)' 구조를 순차 탐지.
    """
    W = to_weekly(dates, h, l, c)
    n = len(W)
    if n < MIN_BASE_WEEKS + 2:
        return []

    highs = [w["h"] for w in W]

    def is_swing_high(k: int, back: int = 10, fwd: int = 4) -> bool:
        """좌측 고점 자격: 앞 10주·뒤 4주에서 가장 높은 봉이어야 한다 (그리드 서치로 최적화).
        (이 조건이 없으면 사소한 고점마다 베이스가 생겨 과탐지된다)"""
        lo = max(0, k - back)
        hi = min(n, k + fwd + 1)
        return highs[k] >= max(highs[lo:hi]) - 1e-9

    raw: List[dict] = []
    i = 0
    while i < n - MIN_BASE_WEEKS:
        left_i = i
        left_high = W[left_i]["h"]
        if not is_swing_high(left_i):
            i += 1
            continue

        pivot = round(left_high + PIVOT_OFFSET, 2)

        # (b) 저점 탐색 + 피벗 돌파 지점
        low_v, low_i = left_high, left_i
        brk_i = None
        j = left_i + 1
        while j < n:
            if W[j]["l"] < low_v:
                low_v, low_i = W[j]["l"], j
            weeks = j - left_i
            if weeks > MAX_BASE_WEEKS:
                break
            # 돌파: 종가가 피벗 위 + 최소 길이 충족
            if weeks >= MIN_BASE_WEEKS and W[j]["c"] > pivot:
                brk_i = j
                break
            j += 1

        depth = (left_high - low_v) / left_high * 100 if left_high else 0.0
        # 실측 최소 depth 8.8% → 8% 미만 흔들림은 베이스로 보지 않음
        if depth < 8.0 or depth > MAX_BASE_DEPTH or (low_i == left_i):
            i += 1
            continue

        end_i = brk_i if brk_i is not None else n - 1
        if end_i - left_i < MIN_BASE_WEEKS:
            i += 1
            continue
        # [버그 수정] 돌파 없이 최대 길이를 넘겨 루프를 빠져나온 구간은
        #   베이스가 아니다. 이를 '형성 중'으로 기록하면 len_w 가 오늘까지
        #   늘어나 NBTX 에서 "102주 베이스" 같은 값이 나왔다.
        if brk_i is None and (end_i - left_i) > MAX_BASE_WEEKS:
            i += 1
            continue

        seg_c = [w["c"] for w in W[left_i:end_i + 1]]
        btype = classify(depth, seg_c, low_i - left_i)

        raw.append({
            "left_date": W[left_i]["date"], "left_high": round(left_high, 2),
            "low_date": W[low_i]["date"], "low": round(low_v, 2),
            "brk_date": W[brk_i]["date"] if brk_i is not None else None,
            "pivot": pivot, "depth": round(depth, 1),
            "len_w": _weeks(W[left_i]["date"],
                            W[end_i]["date"]) if brk_i is not None
                     else _weeks(W[left_i]["date"], W[n - 1]["date"]),
            "type": btype,
            "status": "completed" if brk_i is not None else "forming",
            "_left_i": left_i, "_end_i": end_i,
        })
        i = end_i + 1 if brk_i is not None else i + 1

    return _assign_stages(raw)


# ── 4) 스테이지(베이스 카운트) 부여 ───────────────────────────────────
def _assign_stages(raw: List[dict]) -> List[Base]:
    """
    [차트덱 방법론 문단에 명시된 규칙 그대로]
      · 직전 피벗 대비 +20% 초과 상승 후 새 베이스 → 스테이지 N+1
      · 그 이하                                  → base-on-base = 같은 N + (a)(b)(c)
      · 직전 저점 붕괴(undercut)                  → 스테이지 1 로 리셋
    표기: "2(2)" = 카운트2·체인2 / "1(a)(2)" = 카운트1·서브a·체인2
    """
    out: List[Base] = []
    count, chain, sub = 0, 0, 0
    prev_pivot: Optional[float] = None
    prev_low: Optional[float] = None

    for b in raw:
        prior_up = None
        if prev_pivot:
            prior_up = round((b["left_high"] / prev_pivot - 1) * 100, 1)

        undercut = (prev_low is not None and b["low"] < prev_low)

        if count == 0 or undercut:
            count, chain, sub = 1, 0, 0
            label = "1"
        elif prior_up is not None and prior_up > ADVANCE_MIN:
            count += 1; chain += 1; sub = 0
            label = f"{count}({chain + 1})"
        else:
            sub += 1; chain += 1
            label = f"{count}({chr(96 + sub)})({chain + 1})"

        out.append(Base(
            stage=label, stage_n=count, type=b["type"], status=b["status"],
            left_date=b["left_date"], left_high=b["left_high"],
            low_date=b["low_date"], low=b["low"], brk_date=b["brk_date"],
            pivot=b["pivot"], depth=b["depth"], len_w=b["len_w"],
            prior_up=prior_up,
        ))
        prev_pivot = b["pivot"]
        prev_low = b["low"]
    return out


# ── 5) RS 점수 (차트덱 방법론: 3/6/9/12개월 가중 40/20/20/20 → 1~99 로지스틱) ──
def compute_rs_score(closes: Sequence[float],
                     bench: Sequence[float]) -> Optional[int]:
    """
    RS점수 = 3/6/9/12개월 가중수익률(40/20/20/20)의 벤치마크 상대비를
             1~99 로지스틱으로 매핑한 자체 점수 (IBD 백분위 아님).
    캘리브레이션 기준(방법론 문단): 시장 대비 +40% ≈ 87, +100% ≈ 98, 이상 99 고정.
    """
    n = min(len(closes), len(bench))
    if n < 260:
        return None
    c, bm = closes[-n:], bench[-n:]

    def ret(series, days):
        if len(series) <= days or series[-days - 1] <= 0:
            return None
        return series[-1] / series[-days - 1] - 1.0

    W = [(63, 0.40), (126, 0.20), (189, 0.20), (252, 0.20)]
    sw = bw = 0.0
    for days, wgt in W:
        rs_, rb_ = ret(c, days), ret(bm, days)
        if rs_ is None or rb_ is None:
            return None
        sw += wgt * rs_
        bw += wgt * rb_
    rel = (1 + sw) / (1 + bw) - 1.0            # 벤치마크 상대비

    # 로지스틱 상수는 +40%→87, +100%→98 두 점으로 역산한 값
    k, x0 = 4.334, -0.0571
    score = 99.0 / (1.0 + math.exp(-k * (rel - x0)))
    return int(max(1, min(99, round(score))))


# ── 6) Ants / Blue Dot (차트 범례에 정의가 명시됨) ────────────────────
def sma(vals: Sequence[float], n: int) -> List[Optional[float]]:
    """단순이동평균 (ANTS 맥락 판정용 50일선 등)."""
    out, run = [], 0.0
    for i, x in enumerate(vals):
        run += x
        if i >= n:
            run -= vals[i - n]
        out.append(round(run / n, 4) if i >= n - 1 else None)
    return out


def compute_ants(dates, c, v, win: int = 15, need_up: int = 12,
                 need_ret: float = 20.0, vol_mult: float = 1.20,
                 ma50=None, ext_max: float = 25.0,
                 strict: bool = True) -> List[dict]:
    """
    ANTS (David Ryan) — MVP 3조건을 모두 충족해야 발화.
      M (Momentum) : 최근 15거래일 중 12일 이상 상승 마감
      V (Volume)   : 15일 평균 거래량 ≥ 50일 평균 × 1.20
      P (Price)    : 15일간 주가 +20% 이상
    원전(오닐 사내 구현)도 3조건 동시 충족 시에만 마킹했다.

    [맥락 판정] ANTS 는 '매집 확인'이지 진입 신호가 아니다.
      베이스 초기에 나오면 강세 확인, 이미 수개월 오른 뒤 나오면
      클라이맥스 런 경고로 뒤집힌다 → 50일선 이격도로 구분해 flag 를 붙인다.
      ma50 을 넘기면 이격 ext_max% 초과 시 kind="climax" 로 표시한다.

    [strict 옵션]
      strict=True  (기본) — 원전 MVP 3조건 모두. 정밀도 우선.
      strict=False        — M + 거래량 증가만. 참고 차트덱 범례
                            "15봉중 12↑·거래량↑" 와 동일한 느슨한 기준.
      실측: strict=True 는 정밀도 100%/재현율 34%,
            strict=False 는 참고본 정답을 100% 재현(대신 신호 수 증가).

    return: [{"d": 날짜, "kind": "accum"|"climax"|"weak",
              "ret": 15일수익률, "ext": 이격%, "mvp": bool}]
    """
    out: List[dict] = []
    n = len(c)
    if n < max(win + 1, 50):
        return out
    for i in range(max(win, 50), n):
        # M
        up = sum(1 for k in range(i - win + 1, i + 1) if c[k] > c[k - 1])
        if up < need_up:
            continue
        # P
        base_px = c[i - win]
        if base_px <= 0:
            continue
        ret = (c[i] / base_px - 1) * 100
        # V
        v15 = sum(v[i - win + 1:i + 1]) / win
        v50 = sum(v[i - 49:i + 1]) / 50
        if v50 <= 0:
            continue
        mvp = (ret >= need_ret) and (v15 >= v50 * vol_mult)
        if strict and not mvp:
            continue
        if not strict and v15 <= v50:      # 느슨 기준: 거래량 증가만 요구
            continue
        # 맥락: 50일선 이격
        ext = None
        kind = "accum"
        if ma50 is not None and i < len(ma50) and ma50[i]:
            ext = (c[i] / ma50[i] - 1) * 100
            if ext > ext_max:
                kind = "climax"
        if not mvp:
            kind = "weak"          # M+V 만 충족 (참고본 호환 신호)
        out.append({"d": dates[i], "kind": kind, "mvp": mvp,
                    "ret": round(ret, 1),
                    "ext": (round(ext, 1) if ext is not None else None)})
    return out


def compute_blue_dots(dates, rs_line, win: int = 252,
                      min_gap: int = 0) -> List[str]:
    """
    Blue Dot = RS 라인 52주 신고가.
    min_gap=0 이 기본 (정답셋에 연속일이 포함되어 있어 중복 제거하지 않음).
    과탐지가 신경 쓰이면 min_gap 을 올려 군집을 압축할 수 있다.
    """
    out, last = [], -10 ** 9
    for i in range(win, len(rs_line)):
        if rs_line[i] >= max(rs_line[i - win:i + 1]) - 1e-12:
            if i - last >= min_gap:
                out.append(dates[i])
                last = i
    return out


def analyze(symbol: str, dates, o, h, l, c, v) -> dict:
    """단일 종목 분석 → 샘플 DECK 과 동일한 형태의 dict."""
    bases = detect_bases(dates, h, l, c)
    last = bases[-1] if bases else None
    return {
        "symbol": symbol,
        "asof": dates[-1] if dates else None,
        "last_close": c[-1] if c else None,
        "stage_weekly": ({
            "stage": last.stage, "status": last.status, "pivot": last.pivot,
            "pct_from_pivot": round((c[-1] / last.pivot - 1) * 100, 1) if last.pivot else None,
            "base_len_weeks": last.len_w, "base_depth_pct": last.depth,
            "prior_uptrend_pct": last.prior_up, "base_type": last.type,
            "reliability": "high" if last.len_w >= MIN_BASE_WEEKS else "medium",
        } if last else None),
        "bases": [asdict(b) for b in bases],
    }


# ═════════════════════════════════════════════════════════════════════
# 7) 추가 신호 — HTF / 타이트 밴드 / VCP 축소
#    (참고 차트덱 범례에 정의가 명시된 항목들)
# ═════════════════════════════════════════════════════════════════════
def compute_htf(dates, h, l, c,
                pole_min_pct: float = 90.0, pole_max_weeks: int = 8,
                flag_min_weeks: int = 3, flag_max_weeks: int = 5,
                flag_max_depth: float = 25.0,
                gap_weeks: int = 8, max_signals: int = 3) -> List[dict]:
    """
    High Tight Flag — 짧은 기간 급등(폴) 후 얕은 횡보(깃발).
      폴  : pole_max_weeks 이내 +pole_min_pct% 이상 상승
      깃발: 이어지는 flag_min~max_weeks 동안 조정폭 flag_max_depth% 이내
    사용자 백테스트: HTF 돌파 n=267 · 40일 +14.7% (Δ +7.7%p) — 최상위 신호.
    """
    W = to_weekly(dates, h, l, c)
    n, out = len(W), []
    for i in range(pole_max_weeks, n - flag_min_weeks):
        lo = min(W[k]["l"] for k in range(i - pole_max_weeks, i + 1))
        hi = W[i]["h"]
        if lo <= 0 or (hi / lo - 1) * 100 < pole_min_pct:
            continue
        for fw in range(flag_min_weeks, flag_max_weeks + 1):
            j = i + fw
            if j >= n:
                break
            seg = W[i + 1:j + 1]
            if not seg:
                continue
            f_lo = min(x["l"] for x in seg)
            depth = (hi - f_lo) / hi * 100
            if depth <= flag_max_depth:
                out.append({"pole_start": W[i - pole_max_weeks]["date"],
                            "pole_top_date": W[i]["date"], "pole_top": round(hi, 2),
                            "pole_gain": round((hi / lo - 1) * 100, 1),
                            "flag_end": W[j]["date"], "flag_depth": round(depth, 1),
                            "pivot": round(hi + PIVOT_OFFSET, 2)})
                break
    # ── 군집 압축 ────────────────────────────────────────────────
    #   급등주는 인접 주봉마다 HTF 조건이 연달아 성립해 신호가 폭증한다.
    #   (실측: NBTX 12건 → 라벨이 서로 겹쳐 판독 불가)
    #   같은 상승 국면은 하나의 사건이므로, 깃발 종료일이 gap_weeks 이내로
    #   이어지면 한 묶음으로 보고 '폴 상승률이 가장 큰' 것만 남긴다.
    if not out:
        return []
    out.sort(key=lambda x: x["pole_top_date"])
    groups, cur = [], [out[0]]
    for x in out[1:]:
        gap = (_d(x["pole_top_date"]) - _d(cur[-1]["pole_top_date"])).days
        if gap <= gap_weeks * 7:
            cur.append(x)
        else:
            groups.append(cur); cur = [x]
    groups.append(cur)
    best = [max(g, key=lambda z: z["pole_gain"]) for g in groups]
    # 그래도 많으면 상승률 상위 max_signals 개만
    if len(best) > max_signals:
        best = sorted(best, key=lambda z: -z["pole_gain"])[:max_signals]
        best.sort(key=lambda z: z["pole_top_date"])
    return best


def compute_tight_bands(dates, h, l, c,
                        min_weeks: int = 3, max_weeks: int = 4,
                        max_range: float = 2.5) -> List[dict]:
    """
    3~4주 타이트 밴드 — 연속 주봉 종가가 max_range% 이내에 모임.
    ※ 백테스트상 '일반 tight' 단독은 Δ−3.2%p 로 무의미하고,
      HTF 맥락 위의 tight 만 유효(Δ+6.8%p)하므로 htf 여부를 함께 표시한다.
    """
    W = to_weekly(dates, h, l, c)
    n, out, i = len(W), [], 0
    while i < n - min_weeks:
        best = None
        for wlen in range(max_weeks, min_weeks - 1, -1):
            seg = W[i:i + wlen]
            if len(seg) < wlen:
                continue
            cs = [x["c"] for x in seg]
            if max(cs) <= 0:
                continue
            rng = (max(cs) - min(cs)) / max(cs) * 100
            if rng <= max_range:
                best = {"start": seg[0]["date"], "end": seg[-1]["date"],
                        "weeks": wlen, "range_pct": round(rng, 1),
                        "top": round(max(x["h"] for x in seg), 2)}
                break
        if best:
            out.append(best); i += best["weeks"]
        else:
            i += 1
    return out


def compute_vcp(dates, h, l, c, base: Optional[dict] = None,
                min_contractions: int = 2) -> Optional[dict]:
    """
    VCP(Volatility Contraction Pattern) — 베이스 안에서 조정폭이 순차 축소.
    반환 예: {"n":2, "from":17.0, "to":9.0, "legs":[17.0, 9.0]}
    """
    W = to_weekly(dates, h, l, c)
    if base:
        ds = [x["date"] for x in W]
        try:
            s = ds.index(base["left_date"])
            e = ds.index(base["brk_date"]) if base.get("brk_date") else len(W) - 1
        except ValueError:
            s, e = 0, len(W) - 1
        W = W[s:e + 1]
    if len(W) < 6:
        return None

    legs, peak, trough = [], None, None
    for x in W:
        if peak is None or x["h"] >= peak:
            if peak is not None and trough is not None and trough < peak:
                legs.append((peak - trough) / peak * 100)
            peak, trough = x["h"], None
        else:
            trough = x["l"] if trough is None else min(trough, x["l"])
    if peak is not None and trough is not None and trough < peak:
        legs.append((peak - trough) / peak * 100)

    legs = [round(v, 1) for v in legs if v > 1.0]
    if len(legs) < min_contractions:
        return None
    # 마지막 구간들이 축소 추세인지 확인
    tail = legs[-min(len(legs), 4):]
    if tail[0] <= tail[-1]:
        return None
    return {"n": len(tail), "from": tail[0], "to": tail[-1], "legs": tail}
