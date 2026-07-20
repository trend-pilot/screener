# -*- coding: utf-8 -*-
"""
ad_early_strength.py — screener.py 추가 필드 계산기
======================================================================
참고 대시보드 ⑧(진입 후보)의 셋업 카드에 필요한 두 필드를 계산한다.

  1) AD (Accumulation/Distribution 레이팅)   → 종목 dict에 'ad', 'ad_grade'
  2) 추세초기강세 (Early Trend Strength)      → 종목 dict에 'early_strength'

────────────────────────────────────────────────────────────────────
[ AD — 계산 원리 ]
IBD A/D 레이팅의 정량 근사. 최근 LOOKBACK(기본 50)거래일에서:
  • 상승일/하락일 거래량 비율 (up/down volume) — 기관 매집/분산의 고전 지표
  • 종가의 일중 위치 × 거래량 (Chaikin Money Flow) — 종가가 고가 근처면 매집
두 신호를 결합해 0~100 점수 + 등급(A+/A/B/C/D/E). 최근일 가중.
OHLCV 히스토리(High/Low/Close/Volume)가 필요 — screener 가 이미 받는 데이터.

[ 추세초기강세 — 계산 원리 ]
참고본 정의: "Phase 전환 신선도 - RS Line 60~85 early zone - 가속 종합"
  • zone   : RS Line 이 60~85 초기 구간(피벗 직후, 미연장)일 때 최고점.
             <60 은 미형성, >85 는 연장(추격 위험)으로 감점.
  • fresh  : 오늘 Phase 상향(phase_changed_up) + 초기 Phase(4/4+)일수록 신선.
  • accel  : RS now-IBD 갭(모멘텀 가속) + RS Line 1주 변화 + 거래량 가속(acc/acc2).
종목 dict 의 기존 필드만으로 계산(히스토리 불필요).
======================================================================
"""

import numpy as np


# ─────────────────────────────────────────────────────────────────────
# 1) AD 레이팅
# ─────────────────────────────────────────────────────────────────────
def compute_ad_rating(high, low, close, volume, lookback=50):
    """
    high/low/close/volume: 시간순(과거→최신) 1D array-like (list 또는 np.array).
      pandas 를 쓴다면 df['High'].values 등으로 넘기면 된다.
    return: (ad_score: float 1~99, ad_grade: 'A+'|'A'|'B'|'C'|'D'|'E') 또는 (None, None)
    """
    high = np.asarray(high, dtype=float)
    low = np.asarray(low, dtype=float)
    close = np.asarray(close, dtype=float)
    volume = np.asarray(volume, dtype=float)

    n = len(close)
    if n < 20 or not (len(high) == len(low) == len(volume) == n):
        return None, None

    # 최근 lookback+1 (전일 대비 비교 위해 +1)
    hi = high[-(lookback + 1):]
    lo = low[-(lookback + 1):]
    cl = close[-(lookback + 1):]
    vo = volume[-(lookback + 1):]

    # --- (a) Chaikin Money Flow Multiplier: 종가의 일중 위치 (-1..+1) ---
    rng = hi - lo
    with np.errstate(divide='ignore', invalid='ignore'):
        mfm = np.where(rng > 0, ((cl - lo) - (hi - cl)) / rng, 0.0)
    mfv = mfm * vo  # money flow volume

    # 최근일 가중(선형 1→2)
    w = np.linspace(1.0, 2.0, len(mfv))
    tot_w_vol = np.sum(vo * w)
    cmf = float(np.sum(mfv * w) / tot_w_vol) if tot_w_vol > 0 else 0.0  # -1..+1

    # --- (b) 상승일/하락일 거래량 비율 (IBD 고전) ---
    diff = np.diff(cl)                 # 전일 대비
    vol_days = vo[1:]                  # diff 와 정렬
    up_vol = float(vol_days[diff > 0].sum())
    dn_vol = float(vol_days[diff < 0].sum())
    udv = up_vol / (up_vol + dn_vol) if (up_vol + dn_vol) > 0 else 0.5  # 0..1

    # --- 결합 → 0~100 ---
    #   cmf(-1..1)→0..100 과 udv(0..1)→0..100 을 6:4 로 블렌드
    cmf_score = (0.5 + 0.5 * cmf) * 100.0
    udv_score = udv * 100.0
    score = 0.6 * cmf_score + 0.4 * udv_score
    score = float(max(1.0, min(99.0, round(score, 1))))

    grade = ('A+' if score >= 85 else 'A' if score >= 72 else 'B' if score >= 58
             else 'C' if score >= 45 else 'D' if score >= 30 else 'E')
    return score, grade


# ─────────────────────────────────────────────────────────────────────
# 1-b) 거래량 비율 (50일 평균 대비 당일)
# ─────────────────────────────────────────────────────────────────────
def compute_vol_ratio(volume, lookback=50):
    """
    당일 거래량 / 최근 lookback일 평균 거래량.
    참고 대시보드 ⑧ 진입 근거의 '거래량 폭증 (50d 3.9x)' / '거래량 미달 (50d 0.4x)' 용.
    return: float (소수 2자리) 또는 None
    """
    v = np.asarray(volume, dtype=float)
    v = v[np.isfinite(v)]
    if len(v) < 10:
        return None
    today = float(v[-1])
    base = v[-(lookback + 1):-1] if len(v) > lookback else v[:-1]
    if len(base) == 0:
        return None
    avg = float(np.mean(base))
    if avg <= 0:
        return None
    return round(today / avg, 2)


# ─────────────────────────────────────────────────────────────────────
# 1-c) 분산일 (Distribution Day) — IBD 정통 정의
# ─────────────────────────────────────────────────────────────────────
def compute_distribution_days(close, volume, lookback=25,
                              drop_pct=0.2, rally_reset_pct=5.0):
    """
    지수의 최근 lookback 거래일 중 '분산일' 개수.

    [IBD 정의]
      분산일 = 지수가 전일 대비 drop_pct(기본 0.2%) 이상 하락 +
               거래량이 전일보다 증가한 날 (기관 매도 흔적).
      만료   = ① lookback(25) 거래일 경과, 또는
               ② 해당 분산일 종가 대비 지수가 rally_reset_pct(5%) 이상 상승.

    close/volume: 시간순(과거→최신) array-like
    return: int (없거나 데이터 부족이면 None)
    """
    c = np.asarray(close, dtype=float)
    v = np.asarray(volume, dtype=float)
    n = len(c)
    if n < lookback + 2 or len(v) != n:
        return None

    latest = float(c[-1])
    count = 0
    # 최근 lookback 거래일 검사 (인덱스는 전일 비교가 가능해야 하므로 1부터)
    start = max(1, n - lookback)
    for i in range(start, n):
        prev_c, cur_c = float(c[i - 1]), float(c[i])
        prev_v, cur_v = float(v[i - 1]), float(v[i])
        if not (np.isfinite(prev_c) and np.isfinite(cur_c)
                and np.isfinite(prev_v) and np.isfinite(cur_v)):
            continue
        if prev_c <= 0:
            continue
        chg = (cur_c / prev_c - 1.0) * 100.0
        if chg <= -drop_pct and cur_v > prev_v:
            # 5% 랠리 리셋: 이후 지수가 해당 종가 대비 5%+ 상승했으면 만료
            if (latest / cur_c - 1.0) * 100.0 >= rally_reset_pct:
                continue
            count += 1
    return count


# ─────────────────────────────────────────────────────────────────────
# 2) 추세초기강세
# ─────────────────────────────────────────────────────────────────────
def compute_early_strength(s):
    """
    s: 종목 dict — rs_line_score / phase / phase_changed_up / rs_now /
       ibd_rs(또는 rs) / rs_line_1w / acc / acc2 를 사용.
    return: early_strength (float 0~100)
    """
    z = _num(s.get('rs_line_score'))

    # (1) early zone: 60~85 초기 구간에서 최고, 바깥은 감쇠
    if z <= 0:
        zone = 0.0
    elif z < 60:
        zone = z * 0.9                      # 아직 형성 전 — 60 향해 상승
    elif z <= 85:
        zone = 100.0 - abs(72.0 - z) * 1.6  # 72 근처 피크
    else:
        zone = max(0.0, 100.0 - (z - 85.0) * 4.0)  # 연장(추격 위험) 감점

    # (2) freshness: 오늘 상향 + 초기 Phase 일수록 신선
    fresh = 0.0
    if s.get('phase_changed_up'):
        fresh += 55.0
    fresh += {'4': 30, '4plus': 25, '3': 20, '2': 18, '5': 12}.get(s.get('phase'), 0)
    fresh = min(100.0, fresh)

    # (3) acceleration: RS now-IBD 갭 + RS Line 1주 + 거래량 가속
    gap = _num(s.get('rs_now')) - _num(s.get('ibd_rs') if s.get('ibd_rs') is not None else s.get('rs'))
    rl1w = _num(s.get('rs_line_1w'))
    accel = 50.0 + gap * 1.5 + rl1w * 0.5 + (10 if s.get('acc') else 0) + (8 if s.get('acc2') else 0)
    accel = max(0.0, min(100.0, accel))

    es = 0.45 * zone + 0.25 * fresh + 0.30 * accel
    return round(max(0.0, min(100.0, es)), 1)


def _num(v):
    return float(v) if isinstance(v, (int, float)) else 0.0


# ─────────────────────────────────────────────────────────────────────
# 3) screener.py 통합 예시
# ─────────────────────────────────────────────────────────────────────
"""
[ screener.py 안에서 이렇게 쓰면 된다 ]

from ad_early_strength import compute_ad_rating, compute_early_strength

# ── 종목별 히스토리(hist)가 있는 지점 (ma50/rs_line 계산하는 곳과 동일) ──
#    hist 는 yfinance download 결과 (DataFrame, columns High/Low/Close/Volume)
ad_score, ad_grade = compute_ad_rating(
    hist['High'].values, hist['Low'].values,
    hist['Close'].values, hist['Volume'].values
)
stock['ad'] = ad_score          # 0~100 (없으면 None)
stock['ad_grade'] = ad_grade    # 'A+'~'E'

# ── 추세초기강세는 종목 dict 가 rs_line_score/phase 등 채워진 '뒤'에 호출 ──
stock['early_strength'] = compute_early_strength(stock)

# → screener_data.json 의 각 stock 에 ad / ad_grade / early_strength 추가됨.
#   대시보드 ⑧-b 셋업 카드가 이 필드를 읽어 참고본처럼 렌더한다.
"""


# ─────────────────────────────────────────────────────────────────────
# 4) 자체 테스트
# ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import json, sys

    # (a) AD — 합성 OHLCV 로 로직 검증 (매집 vs 분산)
    rng = np.random.default_rng(1)
    def synth(trend, close_bias):
        # trend: 일일 드리프트, close_bias: 종가가 고가(+)/저가(-) 근처
        n = 60
        base = 100 * np.cumprod(1 + trend/100 + rng.normal(0, 0.01, n))
        hi = base * (1 + rng.uniform(0.005, 0.02, n))
        lo = base * (1 - rng.uniform(0.005, 0.02, n))
        cl = lo + (hi - lo) * np.clip(0.5 + close_bias + rng.normal(0, 0.1, n), 0.05, 0.95)
        vol = rng.uniform(1e6, 3e6, n) * (1 + 0.5*(trend>0))
        return hi, lo, cl, vol
    for label, tr, cb in [('강한 매집', 0.3, 0.35), ('중립', 0.0, 0.0), ('분산', -0.25, -0.35)]:
        hi, lo, cl, vol = synth(tr, cb)
        sc, gr = compute_ad_rating(hi, lo, cl, vol)
        print(f"AD [{label:<8}] → {sc:>5} ({gr})")

    # (b) 추세초기강세 — 실제 screener_data.json 이 있으면 상위 분포 확인
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if path:
        d = json.load(open(path, encoding='utf-8'))
        stocks = [s for s in d['stocks'] if s.get('asset_type') == 'STOCK']
        for s in stocks:
            s['_es'] = compute_early_strength(s)
        top = sorted(stocks, key=lambda s: s['_es'], reverse=True)[:12]
        print("\n추세초기강세 상위 12 (진입 후보 정렬 키):")
        for s in top:
            print(f"  {s['ticker']:<6} ES {s['_es']:>5}  RSLine {s.get('rs_line_score')}"
                  f"  phase {s.get('phase')}  now-IBD {(_num(s.get('rs_now'))-_num(s.get('ibd_rs'))):+.0f}")
