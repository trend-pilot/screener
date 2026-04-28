"""
추세추종 스크리너 v9 - 패턴 감지 + RS Line 계산
========================================
- 나스닥 전종목 (nasdaq_tickers.csv)
- Stage 2 트렌드 템플릿
- $NDFI/$S5FI 시장 신호등
- 패턴 감지: VCP / 컵앤핸들 / 이중바닥 / 플랫베이스 /
             어센딩베이스 / 하이타이트플래그 / 소서앤핸들
- v9 추가: RS Line 4시점 (현재/1주전/3주전/6주전) + 52주 백분위 점수

실행: python screener.py
"""

import os, json, time, logging
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from phase_history import annotate_and_persist  # v11: Phase 전환 추적

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────────────────────
OUTPUT_DIR   = "output"
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "screener_data.json")
TICKER_FILE  = "nasdaq_tickers.csv"
ETF_FILE     = "etf_tickers.csv"   # ETF 티커 + 자산군 분류
os.makedirs(OUTPUT_DIR, exist_ok=True)

TEST_MODE        = False
TEST_LIMIT       = 50
RS_RANK_MIN      = 70
FROM_52W_LOW     = 30
FROM_52W_HIGH    = -25
HISTORY_DAYS     = 320
MIN_MARKET_CAP_M = 300
SIGNAL_GREEN     = 50
SIGNAL_RED       = 80
SIGNAL_BLUE      = 20

# v9 추가: RS Line 계산용 벤치마크 (SPY = S&P 500 ETF)
RS_LINE_BENCHMARK = "SPY"

# ── 패턴 감지 모듈 import ─────────────────────────────────────────────────────
try:
    from pattern_detector import detect_all_patterns, get_pattern_label
    PATTERN_ENABLED = True
    log.info("패턴 감지 모듈 로드 완료")
except ImportError:
    PATTERN_ENABLED = False
    log.warning("pattern_detector.py 없음 — 패턴 감지 비활성화")

# ── $NDFI / $S5FI ─────────────────────────────────────────────────────────────
NDX_100 = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
    "NFLX","AMD","CSCO","ADBE","PEP","INTU","QCOM","TXN","AMAT","HON",
    "SBUX","ISRG","BKNG","AMGN","VRTX","GILD","ADI","LRCX","PANW","MDLZ",
    "REGN","MU","KLAC","INTC","MELI","SNPS","CDNS","PYPL","MAR","CTAS",
    "ABNB","ORLY","MNST","FTNT","PCAR","KDP","AEP","CHTR","CPRT","MRVL",
    "WDAY","ODFL","DXCM","PAYX","ROST","CEG","EA","FAST","GEHC","IDXX",
    "KHC","MRNA","TTWO","VRSK","XEL","ZS","CRWD","DDOG","TEAM","ANSS",
    "BIIB","DLTR","EXC","NXPI","ON","PDD","SPLK","TMUS","ARM","PLTR",
    "COIN","TTD","NET","SNOW","MDB","HUBS","VEEV","NOW","SHOP","SMCI",
]

SPX_SAMPLE = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","JPM","LLY",
    "V","UNH","XOM","MA","JNJ","PG","HD","MRK","COST","ABBV",
    "CRM","BAC","CVX","NFLX","AMD","WMT","KO","PEP","TMO","CSCO",
    "ACN","MCD","ABT","ORCL","CAT","GS","NOW","ISRG","T","INTU",
    "DE","SPGI","BKNG","BLK","AXP","GILD","SYK","PLD","ADI","MDLZ",
    "REGN","VRTX","CB","ETN","AMT","PANW","AMAT","KLAC","LRCX","MRVL",
    "PLTR","COIN","MELI","SHOP","ZS","CRWD","DDOG","NET","SNOW","MDB",
    "RTX","LMT","HON","UPS","FDX","NEE","DUK","AMT","PLD","EQIX",
]


def calc_breadth(tickers, label):
    log.info(f"{label} 계산 중...")
    above, valid = 0, 0
    end   = datetime.now()
    start = end - timedelta(days=120)
    for ticker in tickers:
        try:
            df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
            if df is None or len(df) < 52: continue
            close = df["Close"].astype(float)
            ma50  = close.rolling(50).mean()
            if float(close.iloc[-1]) > float(ma50.iloc[-1]):
                above += 1
            valid += 1
            time.sleep(0.1)
        except Exception:
            pass
    value = round(above / valid * 100, 1) if valid > 0 else 0
    if value >= SIGNAL_RED:
        signal, action = "red", "과열 — 신규 매수 자제"
    elif value >= SIGNAL_GREEN:
        signal, action = "green", "강세장 — 적극적 매수 및 보유"
    elif value >= SIGNAL_BLUE:
        signal, action = "yellow", "약세장 — 현금 비중 확대"
    else:
        signal, action = "blue", "과매도 — 역발상 매수 기회"
    return {"value": value, "above": above, "total": valid, "signal": signal, "action": action}


def calc_market_signals():
    log.info("=== 시장 신호등 계산 시작 ===")
    ndfi = calc_breadth(NDX_100,    "$NDFI (나스닥 100)")
    s5fi = calc_breadth(SPX_SAMPLE, "$S5FI (S&P 500 샘플)")
    avg  = (ndfi["value"] + s5fi["value"]) / 2
    if avg >= SIGNAL_RED:
        overall, overall_label, invest_pct = "red",    "🔴 과열 (Euphoria)",      "0~20%"
    elif avg >= SIGNAL_GREEN:
        overall, overall_label, invest_pct = "green",  "🟢 강세장 (Bullish)",     "80~100%"
    elif avg >= SIGNAL_BLUE:
        overall, overall_label, invest_pct = "yellow", "🟡 약세장 (Bearish)",     "0~30%"
    else:
        overall, overall_label, invest_pct = "blue",   "🔵 과매도 (Capitulation)", "20~30%"
    log.info(f"$NDFI: {ndfi['value']}% | $S5FI: {s5fi['value']}% | {overall_label}")
    return {"ndfi": ndfi, "s5fi": s5fi, "overall": overall,
            "overall_label": overall_label, "invest_pct": invest_pct,
            "avg": round(avg,1), "updated_at": datetime.now().isoformat()}


# ── 종목 목록 ─────────────────────────────────────────────────────────────────

def get_symbols():
    symbols = []
    if os.path.exists(TICKER_FILE):
        symbols = _load_file(TICKER_FILE)
        if symbols: log.info(f"nasdaq_tickers.csv 로드: {len(symbols)}개")
    nyse_file = "nyse_tickers.csv"
    if os.path.exists(nyse_file):
        nyse = _load_file(nyse_file)
        seen = {s["ticker"] for s in symbols}
        added = [s for s in nyse if s["ticker"] not in seen]
        symbols += added
        log.info(f"nyse 추가: {len(added)}개")
    if not symbols:
        log.warning("CSV 없음 — 기본 목록 사용")
        symbols = _fallback()

    # ─── ETF 추가 (시가총액 필터 우회 위해 별도 로드) ───
    seen_tickers = {s["ticker"] for s in symbols}
    if os.path.exists(ETF_FILE):
        etfs = _load_etf_file(ETF_FILE)
        added_etfs = [e for e in etfs if e["ticker"] not in seen_tickers]
        symbols += added_etfs
        log.info(f"ETF 추가: {len(added_etfs)}개 (전체 ETF 파일: {len(etfs)}개)")
    else:
        log.info(f"ETF 파일 없음 ({ETF_FILE}) - ETF 스크리닝 건너뜀")

    log.info(f"처리 대상: {len(symbols)}개")
    return symbols


def _load_etf_file(filepath):
    """ETF 전용 로더 - asset_type='ETF', asset_class(자산군) 메타데이터 부여"""
    try:
        import io
        with open(filepath, "r", encoding="utf-8") as f:
            df = pd.read_csv(io.StringIO(f.read()))
        df.columns = [c.strip().lower() for c in df.columns]
        result = []
        for _, row in df.iterrows():
            t = None
            for col in ["ticker", "symbol"]:
                if col in row and pd.notna(row[col]):
                    t = str(row[col]).strip().upper().replace("/", "-")
                    break
            if not t:
                continue
            name = t
            for col in ["name", "company"]:
                if col in row and pd.notna(row[col]):
                    name = str(row[col]).strip()
                    break
            asset_class = ""
            for col in ["asset_class", "category", "group"]:
                if col in row and pd.notna(row[col]):
                    asset_class = str(row[col]).strip()
                    break
            result.append({
                "ticker": t,
                "name": name,
                "sector": "",
                "asset_type": "ETF",
                "asset_class": asset_class,
            })
        return result
    except Exception as e:
        log.warning(f"ETF 파일 로드 실패: {e}")
        return []


def _load_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if raw.startswith("[") or raw.startswith("{"):
            return _parse_json(raw)
        return _parse_csv(raw)
    except Exception as e:
        log.warning(f"파일 로드 실패: {e}")
        return []


def _parse_json(raw):
    try:
        data = json.loads(raw)
        rows = data.get("data",{}).get("rows",[]) if isinstance(data,dict) else data
        result = []
        for row in rows:
            t = str(row.get("symbol","")).strip().replace("/","-")
            if not t or not t.replace("-","").replace(".","").isalpha() or len(t)>6: continue
            mc = 0
            try: mc = float(str(row.get("marketCap","0")).replace(",","").replace("$",""))
            except: pass
            if 0 < mc < MIN_MARKET_CAP_M*1_000_000: continue
            result.append({"ticker":t,"name":str(row.get("name",t)).strip(),"sector":str(row.get("sector","")).strip()})
        log.info(f"JSON 파싱: {len(result)}개")
        return result
    except Exception as e:
        log.warning(f"JSON 파싱 실패: {e}")
        return []


def _parse_csv(raw):
    try:
        import io
        df = pd.read_csv(io.StringIO(raw))
        df.columns = [c.strip().lower() for c in df.columns]
        result = []
        for _, row in df.iterrows():
            t = None
            for col in ["symbol","ticker"]:
                if col in row and pd.notna(row[col]):
                    t = str(row[col]).strip().replace("/","-"); break
            if not t or not t.replace("-","").replace(".","").isalpha() or len(t)>6: continue
            name = t
            for col in ["name","company"]:
                if col in row and pd.notna(row[col]): name=str(row[col]).strip(); break
            result.append({"ticker":t,"name":name,"sector":""})
        return result
    except Exception as e:
        log.warning(f"CSV 파싱 실패: {e}")
        return []


def _fallback():
    tickers = ["AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","ORCL","CRM",
               "ADBE","AMD","INTC","QCOM","TXN","ADI","MRVL","MU","AMAT","LRCX",
               "KLAC","PANW","CRWD","ZS","NET","DDOG","SNOW","MDB","TTD","PLTR",
               "LLY","UNH","JNJ","MRK","ABBV","TMO","ISRG","REGN","VRTX","GILD",
               "JPM","BAC","GS","V","MA","AXP","BLK","HD","MCD","COST"]
    seen, result = set(), []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            result.append({"ticker":t,"name":t,"sector":""})
    return result


# ── 벤치마크 (SPY) 가격 데이터 사전 로드 [v9 NEW] ───────────────────────────

_BENCHMARK_CLOSE = None  # 모듈 전역 캐시 (한 번만 로드)


def load_benchmark():
    """SPY 가격 데이터를 한 번만 다운로드. 모든 종목 RS Line 계산 시 재사용."""
    global _BENCHMARK_CLOSE
    if _BENCHMARK_CLOSE is not None:
        return _BENCHMARK_CLOSE
    log.info(f"벤치마크({RS_LINE_BENCHMARK}) 가격 데이터 로드 중...")
    end   = datetime.now()
    start = end - timedelta(days=HISTORY_DAYS)
    try:
        bm_df = yf.Ticker(RS_LINE_BENCHMARK).history(start=start, end=end, auto_adjust=True)
        if bm_df is None or len(bm_df) < 200:
            log.warning(f"{RS_LINE_BENCHMARK} 데이터 부족 — RS Line 계산 비활성화")
            _BENCHMARK_CLOSE = pd.Series(dtype=float)
            return _BENCHMARK_CLOSE
        _BENCHMARK_CLOSE = bm_df["Close"].astype(float)
        log.info(f"  {RS_LINE_BENCHMARK} {len(_BENCHMARK_CLOSE)} 거래일 로드됨")
    except Exception as e:
        log.warning(f"벤치마크 로드 실패 ({e}) — RS Line 계산 비활성화")
        _BENCHMARK_CLOSE = pd.Series(dtype=float)
    return _BENCHMARK_CLOSE


def calc_rs_line(stock_close):
    """
    RS Line = 종목 가격 / 벤치마크(SPY) 가격
    반환: dict {
        rs_line_score: float (현재값),
        rs_line_1w:    float (5거래일 전),
        rs_line_3w:    float (15거래일 전),
        rs_line_6w:    float (30거래일 전),
        rs_line_pct:   int   (52주 백분위, 0~100),
        rs_line_high:  bool  (52주 신고가 근접 여부, 상위 5%)
    }
    데이터 부족 시 None 값들 반환.
    """
    bm = load_benchmark()
    result = {
        "rs_line_score": None,
        "rs_line_1w":    None,
        "rs_line_3w":    None,
        "rs_line_6w":    None,
        "rs_line_pct":   None,
        "rs_line_high":  False,
    }
    if bm is None or len(bm) < 60 or stock_close is None or len(stock_close) < 60:
        return result

    try:
        # tz 제거 + 날짜 normalize
        sc = stock_close.copy()
        bc = bm.copy()
        if sc.index.tz is not None:
            sc.index = sc.index.tz_localize(None)
        if bc.index.tz is not None:
            bc.index = bc.index.tz_localize(None)
        sc.index = pd.to_datetime(sc.index).normalize()
        bc.index = pd.to_datetime(bc.index).normalize()
        df = pd.DataFrame({"s": sc, "b": bc}).dropna()
        if len(df) < 60:
            return result
        rs_line = (df["s"] / df["b"]).astype(float)
    except Exception:
        return result

    def _get(n):
        if len(rs_line) <= n: return None
        v = rs_line.iloc[-n-1]
        return float(v) if pd.notna(v) else None

    result["rs_line_score"] = _get(0)
    result["rs_line_1w"]    = _get(5)
    result["rs_line_3w"]    = _get(15)
    result["rs_line_6w"]    = _get(30)

    # 52주 백분위 (현재 RS Line 이 52주 분포에서 어느 위치인지, 0~100)
    w52 = min(252, len(rs_line))
    recent = rs_line.iloc[-w52:]
    cur = result["rs_line_score"]
    if cur is not None and len(recent) > 0:
        try:
            pct = float((recent < cur).sum()) / float(len(recent)) * 100
            result["rs_line_pct"] = int(round(pct))
            result["rs_line_high"] = pct >= 95
        except Exception:
            pass

    return result


def momentum_score_v2(rs_now, rs_line_score, rs_line_1w, rs_line_3w):
    """
    RS Line 점수 (momentum_score_v2) — 0~100 종합 모멘텀 점수
    가이드 정의에 따라 4개 컴포넌트 합산:
      1) RS 절대 강도 (최대 40점) - rs_now 기준
      2) 최근 1주 RS Line 변화율 (최대 25점) - rs_chg_0_1w
      3) 1~3주 RS Line 변화율 (최대 20점) - rs_chg_1w_3w
      4) 가속 보너스 (최대 20점) - 1주 변화 > 3주 변화
    """
    if rs_line_score is None or rs_line_1w is None or rs_line_3w is None:
        return None
    if rs_line_1w == 0 or rs_line_3w == 0:
        return None

    score = 0

    # 1️⃣ RS 절대 강도 (최대 40점)
    if rs_now is not None:
        if   rs_now >= 90: score += 40
        elif rs_now >= 85: score += 32
        elif rs_now >= 80: score += 24
        else:              score += 12
    else:
        score += 12  # 안전 기본값

    # 2️⃣ 최근 1주 RS 변화율 (최대 25점)
    rs_chg_0_1w = (rs_line_score - rs_line_1w) / abs(rs_line_1w) * 100
    if   rs_chg_0_1w >= 2.0:  score += 25
    elif rs_chg_0_1w >= 1.0:  score += 18
    elif rs_chg_0_1w >= 0.3:  score += 10
    elif rs_chg_0_1w >= -0.3: score += 5
    else:                      score += 0

    # 3️⃣ 1~3주 RS 변화율 (최대 20점)
    rs_chg_1w_3w = (rs_line_1w - rs_line_3w) / abs(rs_line_3w) * 100
    if   rs_chg_1w_3w >= 1.5:  score += 20
    elif rs_chg_1w_3w >= 0.8:  score += 14
    elif rs_chg_1w_3w >= 0.2:  score += 8
    elif rs_chg_1w_3w >= -0.2: score += 3
    else:                       score += 0

    # 4️⃣ 가속 보너스 (최대 20점)
    if rs_chg_0_1w > rs_chg_1w_3w:
        score += 15  # 가속
        # 가속추세: 3구간 단조 증가 (rs_3w < rs_1w < rs_now) + 1주 ≥ 0.5%
        if rs_line_3w < rs_line_1w < rs_line_score and rs_chg_0_1w >= 0.5:
            score += 5

    return min(100, max(0, int(round(score))))


# ── 종목 지표 계산 ────────────────────────────────────────────────────────────

def process_symbol(ticker, name, asset_type="STOCK", asset_class=""):
    """
    asset_type: "STOCK" (기본) 또는 "ETF"
    asset_class: ETF의 자산군 라벨 (예: "반도체", "한국", "리튬/배터리")
    ETF는 시가총액 필터를 적용하지 않음 (AUM 개념이라 다름).
    """
    is_etf = (asset_type == "ETF")
    try:
        end   = datetime.now()
        start = end - timedelta(days=HISTORY_DAYS)
        tk    = yf.Ticker(ticker)
        df    = tk.history(start=start, end=end, auto_adjust=True)
        if df is None or len(df) < 200: return None

        # 시가총액 (fast_info 사용 — 추가 API 호출 없음)
        mktcap = None
        try:
            mktcap = tk.fast_info.market_cap
            if mktcap: mktcap = int(mktcap)
        except Exception:
            pass

        close  = df["Close"].astype(float)
        high   = df["High"].astype(float)
        low    = df["Low"].astype(float)
        volume = df["Volume"].astype(float)

        ma50  = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()

        latest      = float(close.iloc[-1])
        ma50_now    = float(ma50.iloc[-1])
        ma150_now   = float(ma150.iloc[-1])
        ma200_now   = float(ma200.iloc[-1])
        ma200_clean = ma200.dropna()
        ma200_21d   = float(ma200_clean.iloc[-22] if len(ma200_clean)>=22 else ma200_clean.iloc[0])

        w52      = min(252, len(close))
        high_52w = float(high.iloc[-w52:].max())
        low_52w  = float(low.iloc[-w52:].min())

        def pct(n):
            if len(close)<=n: return 0.0
            return round((latest/float(close.iloc[-n-1])-1)*100,1)

        w1,w3,w6 = pct(5),pct(15),pct(30)

        c1 = latest > ma150_now
        c2 = latest > ma200_now
        c3 = ma200_now > ma200_21d
        c4 = ma50_now > ma150_now > ma200_now
        c5 = low_52w>0  and (latest-low_52w)/low_52w*100   >= FROM_52W_LOW
        c6 = high_52w>0 and (latest-high_52w)/high_52w*100 >= FROM_52W_HIGH

        acc  = w1>0 and w3>0 and w1>abs(w3)/3
        acc2 = acc and w3>0 and w6>0
        h52_pct = round((latest-high_52w)/high_52w*100,1) if high_52w>0 else 0

        # 패턴 감지
        patterns = {}
        if PATTERN_ENABLED:
            try:
                patterns = detect_all_patterns(df)
            except Exception as e:
                log.debug(f"{ticker} 패턴 감지 오류: {e}")

        # ─── v9 NEW: RS Line 4시점 + 52주 백분위 ───
        rs_line_data = calc_rs_line(close)

        # ─── v10 NEW: IBD 12개월 가중 RS raw 점수 (한국 버전과 동일) ───
        # 0~3개월(40%) + 3~6개월(20%) + 6~9개월(20%) + 9~12개월(20%)
        n_close = len(close)
        def ibd_ret(i_start, i_end):
            if n_close < i_start: return 0.0
            ps = float(close.iloc[max(0, n_close - i_start)])
            pe = float(close.iloc[n_close - i_end - 1]) if i_end > 0 else float(close.iloc[-1])
            return (pe / ps - 1) * 100 if ps > 0 else 0.0
        ibd_raw = (ibd_ret(63, 0) * 0.4 +
                   ibd_ret(126, 63) * 0.2 +
                   ibd_ret(189, 126) * 0.2 +
                   ibd_ret(252, 189) * 0.2)

        return {
            "ticker":    ticker, "market":"US", "name":name,
            "asset_type":  asset_type,    # "STOCK" or "ETF"
            "asset_class": asset_class,   # ETF 자산군 (반도체/한국/리튬 등)
            "price":     round(latest,2),
            "mktcap":    mktcap,
            "ma50":      round(ma50_now,2),
            "ma150":     round(ma150_now,2),
            "ma200":     round(ma200_now,2),
            "high_52w":  round(high_52w,2),
            "low_52w":   round(low_52w,2),
            "h52_pct":   h52_pct,
            "w1":w1,"w3":w3,"w6":w6,
            "rs":0,"rs_now":0,"ibd_rs":0,
            "ibd_raw":   round(ibd_raw, 2),
            "is_stage2": False,
            "_base": all([c1,c2,c3,c4,c5,c6]),
            "acc":bool(acc),"acc2":bool(acc2),
            "h52_new": h52_pct >= -3,
            "stage2":{
                "above_ma150":bool(c1),"above_ma200":bool(c2),
                "ma200_uptrend":bool(c3),"ma_aligned":bool(c4),
                "from_52w_low":bool(c5),"from_52w_high":bool(c6),
                "rs_rank":False,
            },
            "pass_dots":[],
            # ─── v9: RS Line raw 값 (가격 비율) ───
            # 주의: rs_line_score 는 v10부터 momentum_score_v2 (0~100 점수)로 변경됨
            #       기존 raw 값은 rs_line_value 로 보존
            "rs_line_value": round(rs_line_data["rs_line_score"], 4) if rs_line_data["rs_line_score"] is not None else None,
            "rs_line_1w":    round(rs_line_data["rs_line_1w"],    4) if rs_line_data["rs_line_1w"]    is not None else None,
            "rs_line_3w":    round(rs_line_data["rs_line_3w"],    4) if rs_line_data["rs_line_3w"]    is not None else None,
            "rs_line_6w":    round(rs_line_data["rs_line_6w"],    4) if rs_line_data["rs_line_6w"]    is not None else None,
            "rs_line_pct":   rs_line_data["rs_line_pct"],
            "rs_line_high":  rs_line_data["rs_line_high"],
            # ─── v10 NEW: rs_line_score = momentum_score_v2 (0~100 종합 점수) ───
            # rank_rs() 단계에서 rs_now 가 채워진 후 계산됨 (지금은 None)
            "rs_line_score": None,
            # 패턴 감지 결과
            "pattern_count":   patterns.get("pattern_count", 0),
            "patterns":        patterns.get("patterns", []),
            "best_pattern":    patterns.get("best_pattern"),
            "pattern_detail":  {k: patterns[k] for k in [
                                "vcp","cup_handle","double_bottom","flat_base",
                                "ascending_base","htf","saucer_handle",
                                "rs_line_lead","earnings_gap","power_play",
                                "vol_breakout","s1_to_s2"]
                                if k in patterns},
        }
    except Exception as e:
        log.debug(f"{ticker} 오류: {e}")
        return None


def rank_rs(stocks):
    if not stocks: return stocks
    w3_arr = np.array([s["w3"] for s in stocks])
    w1_arr = np.array([s["w1"] for s in stocks])
    ibd_arr = np.array([s.get("ibd_raw", 0.0) for s in stocks])
    for s in stocks:
        s["rs"]     = int(np.sum(w3_arr < s["w3"]) / len(w3_arr) * 100)
        s["rs_now"] = int(np.sum(w1_arr < s["w1"]) / len(w1_arr) * 100)
        s["ibd_rs"] = int(np.sum(ibd_arr < s.get("ibd_raw", 0.0)) / len(ibd_arr) * 100)

        # ─── v10: RS Line 점수 (momentum_score_v2) - 0~100 종합 모멘텀 ───
        s["rs_line_score"] = momentum_score_v2(
            s["rs_now"],
            s.get("rs_line_value"),
            s.get("rs_line_1w"),
            s.get("rs_line_3w"),
        )
        s["stage2"]["rs_rank"] = s["rs"] >= RS_RANK_MIN
        s["is_stage2"] = s.pop("_base",False) and s["stage2"]["rs_rank"]
        s["pass_dots"] = [
            int(s["stage2"]["above_ma150"]),  int(s["stage2"]["above_ma200"]),
            int(s["stage2"]["ma200_uptrend"]),int(s["stage2"]["ma_aligned"]),
            int(s["stage2"]["from_52w_low"]), int(s["stage2"]["from_52w_high"]),
            int(s["stage2"]["rs_rank"]),
        ]
    return stocks


# ── 메인 ──────────────────────────────────────────────────────────────────────

def _json_default(obj):
    """numpy/pandas 타입을 JSON 직렬화 가능하게 변환"""
    import numpy as np
    if isinstance(obj, (np.bool_)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    log.info("="*60)
    log.info("추세추종 스크리너 v9 (패턴 감지 + RS Line)")
    log.info("="*60)

    # 1. 시장 신호등
    market_signals = calc_market_signals()

    # 2. 벤치마크(SPY) 가격 데이터 사전 로드 [v9]
    load_benchmark()

    # 3. 종목 스크리닝
    all_syms = get_symbols()
    symbols  = all_syms[:TEST_LIMIT] if TEST_MODE else all_syms
    log.info(f"처리 시작: {len(symbols)}개 | 예상 {round(len(symbols)*0.3/60,1)}분")

    results, failed = [], 0
    t0 = time.time()

    for i, sym in enumerate(symbols):
        if (i+1) % 50 == 0:
            elapsed = round((time.time()-t0)/60,1)
            remain  = round((len(symbols)-i-1)*0.3/60,1)
            log.info(f"  [{i+1}/{len(symbols)}] 경과 {elapsed}분 / 남은 {remain}분 (성공 {len(results)})")

        result = process_symbol(
            sym["ticker"],
            sym["name"],
            asset_type=sym.get("asset_type", "STOCK"),
            asset_class=sym.get("asset_class", ""),
        )
        if result: results.append(result)
        else: failed += 1
        time.sleep(0.25)

    log.info(f"수집 완료: 성공 {len(results)} / 실패 {failed}")
    if not results:
        log.error("수집 실패.")
        return

    results = rank_rs(results)

    # ─── Phase 전환 추적 (v11) ──────────────────────────────────────────
    log.info("📜 Phase 전환 추적 (어제 vs 오늘)...")
    today_str = datetime.now().strftime("%Y-%m-%d")
    phase_up_count, phase_first_day = annotate_and_persist(
        results, "phase_history.json", today_str
    )
    log.info(f"   NEW↑ {phase_up_count}개 종목 (첫날={phase_first_day})")

    stage2_count   = sum(1 for s in results if s["is_stage2"])
    avg_rs         = round(float(np.mean([s["rs"] for s in results])),1)
    acc_count      = sum(1 for s in results if s["acc2"])
    pattern_count  = sum(1 for s in results if s.get("pattern_count",0) > 0)
    largecap_count = sum(1 for s in results if (s.get("mktcap") or 0) >= 10_000_000_000)   # 시총 $10B+
    megacap_count  = sum(1 for s in results if (s.get("mktcap") or 0) >= 100_000_000_000)  # 시총 $100B+
    # v9 추가
    rs_line_count  = sum(1 for s in results if s.get("rs_line_score") is not None)
    rs_line_high   = sum(1 for s in results if s.get("rs_line_high"))
    # v10 추가: ETF 카운트
    etf_count      = sum(1 for s in results if s.get("asset_type") == "ETF")
    stock_count    = sum(1 for s in results if s.get("asset_type") != "ETF")

    output = {
        "meta": {
            "updated_at":    datetime.now().isoformat(),
            "total":         len(results),
            "stock_count":   stock_count,
            "etf_count":     etf_count,
            "stage2_count":  stage2_count,
            "avg_rs":        avg_rs,
            "acc_count":     acc_count,
            "pattern_count": pattern_count,
            "largecap_count": largecap_count,
            "megacap_count":  megacap_count,
            "rs_line_count":  rs_line_count,
            "rs_line_high":   rs_line_high,
            "rs_line_benchmark": RS_LINE_BENCHMARK,
            "phase_up_count":  phase_up_count,
            "phase_first_day": phase_first_day,
            "test_mode":     TEST_MODE,
        },
        "market": market_signals,
        "stocks": sorted(results, key=lambda x: x["rs"], reverse=True)
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=_json_default)

    # GitHub 자동 업로드
    log.info("GitHub 업로드 중...")
    try:
        from github_upload import upload_to_github
        upload_to_github()
    except Exception as e:
        log.warning(f"GitHub 업로드 실패: {e}")

    elapsed_total = round((time.time()-t0)/60,1)
    log.info("="*60)
    log.info(f"완료! ({elapsed_total}분) → {OUTPUT_FILE}")
    log.info(f"  신호등: {market_signals['overall_label']}")
    log.info(f"  $NDFI: {market_signals['ndfi']['value']}% | $S5FI: {market_signals['s5fi']['value']}%")
    log.info(f"  종목: {len(results)} (주식 {stock_count} + ETF {etf_count}) | Stage2: {stage2_count} | RS평균: {avg_rs}")
    log.info(f"  패턴 감지: {pattern_count}개 종목에서 패턴 발견")
    log.info(f"  RS Line: {rs_line_count}개 계산됨 ({rs_line_high}개 52주 신고가)")
    log.info(f"  Phase 전환↑: {phase_up_count}개 (첫날={phase_first_day})")
    log.info("="*60)


if __name__ == "__main__":
    main()
