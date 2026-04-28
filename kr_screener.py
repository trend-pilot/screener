"""
kr_screener.py v2 — 한국 주식 추세추종 스크리너 (실전 버전)
=============================================================
- KOSPI + KOSDAQ 전종목 (pykrx 메인 / FDR 보조)
- Stage 2 트렌드 템플릿 (미국 screener.py와 동일 기준)
- RS Score / IBD RS 스타일 퍼센타일 계산
- w1 / w3 / w6 주간 수익률
- 시가총액 (억원)
- MA50 / MA150 / MA200
- 52주 고저 대비
- 가속 (acc / acc2)
- KRX 공식 업종 섹터 분류
- KOSPI 신호등 (시장 강세/약세 판단)

실행:
  python kr_screener.py           # 전체 실행
  python kr_screener.py 200       # 200종목만
  python kr_screener.py --demo    # 데모 모드
"""

import os, json, time, sys, logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── 라이브러리 ──────────────────────────────────────────────
try:
    from pykrx import stock as krx
    PYKRX_OK = True
except ImportError:
    PYKRX_OK = False
    log.warning("pykrx 미설치 → pip install pykrx")

try:
    import FinanceDataReader as fdr
    FDR_OK = True
except ImportError:
    FDR_OK = False
    log.warning("FinanceDataReader 미설치 → pip install finance-datareader")

# ── 설정 ────────────────────────────────────────────────────
OUTPUT_FILE     = "kr_screener_output.json"
HISTORY_DAYS    = 400          # 약 1.5년치 (MA200 + 여유)
MIN_PRICE       = 1000         # 최소 주가 (원) — 동전주 제외
MIN_MKTCAP_100M = 300          # 최소 시가총액 (억원) — 소형주 필터
MIN_AVG_VOLUME  = 30000        # 최소 평균 거래량
RS_RANK_MIN     = 70           # Stage2 RS 조건
FROM_52W_LOW    = 25           # 52주 저점 대비 최소 상승률
FROM_52W_HIGH   = -30          # 52주 고점 대비 최대 하락률
SIGNAL_GREEN    = 50
SIGNAL_RED      = 80
SIGNAL_BLUE     = 20

# FDR Dept 컬럼 실제 값 확인된 매핑
FDR_DEPT_MAP = {
    # KOSPI
    "음식료품":    "소비재",
    "섬유의복":    "소비재",
    "종이목재":    "소재",
    "화학":        "소재",
    "의약품":      "헬스케어",
    "비금속광물":  "소재",
    "철강금속":    "소재",
    "기계":        "산업재",
    "전기전자":    "IT·전자",
    "의료정밀":    "헬스케어",
    "운수장비":    "산업재",
    "유통업":      "소비재",
    "전기가스업":  "유틸리티",
    "건설업":      "산업재",
    "운수창고업":  "산업재",
    "통신업":      "통신",
    "금융업":      "금융",
    "증권":        "금융",
    "보험":        "금융",
    "서비스업":    "서비스",
    # KOSDAQ
    "IT서비스":    "IT·소프트웨어",
    "소프트웨어":  "IT·소프트웨어",
    "반도체":      "IT·전자",
    "디스플레이":  "IT·전자",
    "통신장비":    "IT·전자",
    "디지털컨텐츠":"IT·소프트웨어",
    "컴퓨터서비스":"IT·소프트웨어",
    "인터넷":      "IT·소프트웨어",
    "오락문화":    "서비스",
    "방송서비스":  "통신",
    "통신서비스":  "통신",
    "제약":        "헬스케어",
    "바이오":      "헬스케어",
    "의료기기":    "헬스케어",
    "기계장비":    "산업재",
    "금속":        "소재",
    "제조":        "산업재",
    "건설":        "산업재",
    "유통":        "소비재",
    "운송":        "산업재",
    "금융":        "금융",
    "음식료·담배": "소비재",
    "섬유·의류":   "소비재",
    "기타서비스":  "서비스",
    "기타금융":    "금융",
    "기타제조":    "산업재",
    "기타":        "기타",
}

KRX_SECTOR_MAP = {
    # KOSPI 업종
    "음식료품": "Food & Beverage",
    "섬유의복": "Textile & Apparel",
    "종이목재": "Paper & Wood",
    "화학": "Chemicals",
    "의약품": "Pharmaceuticals",
    "비금속광물": "Non-metallic Minerals",
    "철강금속": "Steel & Metals",
    "기계": "Machinery",
    "전기전자": "Electronics",
    "의료정밀": "Medical & Precision",
    "운수장비": "Transportation Equipment",
    "유통업": "Distribution",
    "전기가스업": "Utilities",
    "건설업": "Construction",
    "운수창고업": "Transportation & Warehousing",
    "통신업": "Telecommunications",
    "금융업": "Financials",
    "증권": "Securities",
    "보험": "Insurance",
    "서비스업": "Services",
    # KOSDAQ 업종
    "IT서비스": "IT Services",
    "소프트웨어": "Software",
    "반도체": "Semiconductors",
    "디스플레이": "Display",
    "오락문화": "Entertainment & Culture",
    "인터넷": "Internet",
    "통신장비": "Telecom Equipment",
    "컴퓨터서비스": "Computer Services",
    "디지털컨텐츠": "Digital Content",
    "소프트웨어": "Software",
    "제조": "Manufacturing",
    "기계장비": "Machinery & Equipment",
    "금속": "Metals",
    "화학": "Chemicals",
    "제약": "Pharmaceuticals",
    "바이오": "Biotech",
    "의료기기": "Medical Devices",
    "건설": "Construction",
    "유통": "Distribution",
    "음식료·담배": "Food & Beverage",
    "섬유·의류": "Textile & Apparel",
    "방송서비스": "Broadcasting",
    "통신서비스": "Telecom Services",
    "운송": "Transportation",
    "금융": "Financials",
    "기타": "Others",
    "기타서비스": "Other Services",
    "기타금융": "Other Financials",
    "기타제조": "Other Manufacturing",
}

# 섹터 그룹핑 (세부 업종 → 대분류)
SECTOR_GROUP_MAP = {
    "Food & Beverage": "소비재",
    "Textile & Apparel": "소비재",
    "Paper & Wood": "소재",
    "Chemicals": "소재",
    "Pharmaceuticals": "헬스케어",
    "Biotech": "헬스케어",
    "Medical Devices": "헬스케어",
    "Medical & Precision": "헬스케어",
    "Non-metallic Minerals": "소재",
    "Steel & Metals": "소재",
    "Metals": "소재",
    "Machinery": "산업재",
    "Machinery & Equipment": "산업재",
    "Manufacturing": "산업재",
    "Electronics": "IT·전자",
    "Semiconductors": "IT·전자",
    "Display": "IT·전자",
    "Telecom Equipment": "IT·전자",
    "IT Services": "IT·소프트웨어",
    "Software": "IT·소프트웨어",
    "Internet": "IT·소프트웨어",
    "Computer Services": "IT·소프트웨어",
    "Digital Content": "IT·소프트웨어",
    "Transportation Equipment": "산업재",
    "Distribution": "소비재",
    "Utilities": "유틸리티",
    "Construction": "산업재",
    "Transportation & Warehousing": "산업재",
    "Transportation": "산업재",
    "Telecommunications": "통신",
    "Telecom Services": "통신",
    "Broadcasting": "통신",
    "Financials": "금융",
    "Securities": "금융",
    "Insurance": "금융",
    "Other Financials": "금융",
    "Services": "서비스",
    "Other Services": "서비스",
    "Entertainment & Culture": "서비스",
    "Other Manufacturing": "산업재",
    "Others": "기타",
}

# ════════════════════════════════════════════════════════════
# 날짜 유틸
# ════════════════════════════════════════════════════════════
def latest_trading_day():
    """가장 최근 거래일 반환 (주말 자동 처리)"""
    d = datetime.today()
    # 주말이면 금요일로
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def date_range(days_back=HISTORY_DAYS):
    end = datetime.today()
    start = end - timedelta(days=days_back)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


# ════════════════════════════════════════════════════════════
# 종목 목록 + 기본 정보 (시총 포함)
# ════════════════════════════════════════════════════════════
def get_stock_universe(today_str):
    """KOSPI + KOSDAQ 전종목 — FDR 전용 (pykrx 미사용)"""
    stocks = []
    for mkt in ["KOSPI", "KOSDAQ"]:
        if FDR_OK:
            try:
                df = fdr.StockListing(mkt)
                cnt = 0
                for _, row in df.iterrows():
                    t = str(row.get("Code","")).zfill(6)
                    if not t or t == "000000":
                        continue
                    stocks.append({
                        "ticker": t,
                        "name":   str(row.get("Name", t)),
                        "market": mkt,
                    })
                    cnt += 1
                log.info(f"  {mkt}: {cnt}개 (FDR)")
            except Exception as e:
                log.warning(f"  FDR {mkt} 목록 실패: {e}")
    log.info(f"  전체 유니버스: {len(stocks)}개")
    return stocks


def get_sector_map(today_str):
    """
    KRX 업종 분류 수집
    1) KRX 정보데이터시스템 CSV 다운로드 (가장 정확)
    2) 종목코드 기반 하드코딩 (폴백)
    """
    import urllib.request, io, csv

    sector_dict = {}

    # ── 방법 1: KRX 업종분류 현황 CSV ──────────────────────
    # KRX 정보데이터시스템 공개 API (로그인 불필요)
    krx_url = (
        "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    )
    headers = {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020102",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    for mkt_code, mkt_name in [("STK","KOSPI"), ("KSQ","KOSDAQ")]:
        try:
            import urllib.parse
            params = urllib.parse.urlencode({
                "bld": "dbms/MDC/STAT/standard/MDCSTAT03901",
                "mktId": mkt_code,
                "trdDd": today_str,
                "money": "1",
                "csvxls_isNo": "false",
            }).encode("utf-8")
            req = urllib.request.Request(krx_url, data=params, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                rows = data.get("OutBlock_1", [])
                for row in rows:
                    code = str(row.get("ISU_SRT_CD","")).zfill(6)
                    sector = str(row.get("IDX_NM","")).replace("KOSPI ","").replace("KOSDAQ ","").strip()
                    if code and sector and sector not in ("nan",""):
                        sector_dict[code] = sector
                log.info(f"  KRX {mkt_name} 업종: {len([v for k,v in sector_dict.items() if v])}개")
        except Exception as e:
            log.debug(f"  KRX {mkt_name} API 실패: {e}")

    # ── 방법 2: FinanceDataReader 업종 코드별 종목 조회 ────
    if len(sector_dict) < 100 and FDR_OK:
        try:
            # FDR의 KRX 업종 인덱스 종목 조회
            krx_sector_codes = {
                "음식료품":   "1001",  # KOSPI 업종 인덱스 코드
                "섬유의복":   "1002",
                "종이목재":   "1003",
                "화학":       "1004",
                "의약품":     "1005",
                "비금속광물": "1006",
                "철강금속":   "1007",
                "기계":       "1008",
                "전기전자":   "1009",
                "의료정밀":   "1010",
                "운수장비":   "1011",
                "유통업":     "1012",
                "전기가스업": "1013",
                "건설업":     "1014",
                "운수창고업": "1015",
                "통신업":     "1016",
                "금융업":     "1017",
                "증권":       "1018",
                "보험":       "1019",
                "서비스업":   "1020",
            }
            for sector_name, idx_code in krx_sector_codes.items():
                try:
                    df_idx = fdr.DataReader(f"KRX/{idx_code}")
                    # 인덱스에 속한 종목들에 업종 할당
                    if df_idx is not None and "Code" in df_idx.columns:
                        for _, row in df_idx.iterrows():
                            t = str(row["Code"]).zfill(6)
                            sector_dict[t] = sector_name
                except Exception:
                    pass
        except Exception as e:
            log.debug(f"  FDR 업종 인덱스 실패: {e}")

    # ── 방법 3: 주요 종목 하드코딩 (최후 수단) ────────────
    KNOWN = {
        "005930":"전기전자","000660":"반도체","009150":"전기전자",
        "066570":"전기전자","051910":"화학","006400":"전기전자",
        "005380":"운수장비","000270":"운수장비","012330":"운수장비",
        "011170":"화학","096770":"화학","003670":"화학","010950":"화학",
        "068270":"의약품","207940":"의약품","000100":"의약품",
        "128940":"의약품","185750":"의약품","326030":"의약품",
        "105560":"금융업","055550":"금융업","086790":"금융업",
        "316140":"금융업","032830":"보험","000810":"보험",
        "006800":"증권","003550":"금융업",
        "017670":"통신업","030200":"통신업","032640":"통신업",
        "000720":"건설업","047040":"건설업","006360":"건설업",
        "005490":"철강금속","004020":"철강금속",
        "035420":"서비스업","035720":"서비스업",
        "042700":"반도체","102120":"반도체","064760":"반도체",
        "259960":"서비스업","263750":"서비스업","036570":"서비스업",
        "069960":"유통업","004170":"유통업","282330":"유통업",
        "028260":"서비스업","018880":"운수장비",
    }
    for t, s in KNOWN.items():
        if sector_dict.get(t, "기타") in ("기타",""):
            sector_dict[t] = s

    total = len([v for v in sector_dict.values() if v not in ("기타","nan","")])
    log.info(f"  섹터 매핑 완료: {len(sector_dict)}개, 유효 {total}개")
    return sector_dict



# yfinance 영문 섹터 → 한국어 매핑
YF_SECTOR_KR = {
    "Technology":             "전기전자",
    "Consumer Cyclical":      "소비재",
    "Consumer Defensive":     "소비재",
    "Healthcare":             "의약품",
    "Financial Services":     "금융업",
    "Industrials":            "산업재",
    "Basic Materials":        "화학",
    "Energy":                 "에너지",
    "Utilities":              "전기가스업",
    "Communication Services": "통신업",
    "Real Estate":            "건설업",
    "Semiconductor":          "반도체",
    "Software":               "서비스업",
    "Biotechnology":          "의약품",
    "Pharmaceuticals":        "의약품",
    "Chemicals":              "화학",
    "Steel":                  "철강금속",
    "Auto Parts":             "운수장비",
    "Banks":                  "금융업",
    "Insurance":              "보험",
    "Telecom":                "통신업",
    "Construction":           "건설업",
    "Food & Beverage":        "음식료품",
    "Retail":                 "유통업",
    "Machinery":              "기계",
}



def get_mktcap_map(today_str):
    """시가총액 {ticker: 억원} — FDR Marcap 컬럼"""
    mktcap_dict = {}
    for mkt in ["KOSPI", "KOSDAQ"]:
        if FDR_OK:
            try:
                df = fdr.StockListing(mkt)
                for _, row in df.iterrows():
                    t = str(row.get("Code","")).zfill(6)
                    cap = row.get("Marcap", 0)
                    try:
                        v = float(cap) if cap and str(cap) not in ("nan","None","") else 0
                        if v > 0:
                            mktcap_dict[t] = round(v / 1e8, 1)
                    except Exception:
                        pass
                log.info(f"  {mkt} 시가총액: {len(mktcap_dict)}개 (FDR)")
            except Exception as e:
                log.warning(f"  FDR {mkt} 시가총액 실패: {e}")
    return mktcap_dict


# ════════════════════════════════════════════════════════════
# KOSPI 신호등
# ════════════════════════════════════════════════════════════
def calc_kospi_signal(start_date, end_date):
    """KOSPI 시장 강세 판단 — FDR/yfinance 기반 (pykrx 미사용)"""
    log.info("  KOSPI 신호등 계산 (FDR/yfinance)...")
    above, total = 0, 0

    try:
        import yfinance as yf
        # KOSPI 대형주 샘플 50개
        sample = [
            "005930.KS","000660.KS","051910.KS","005380.KS","000270.KS",
            "068270.KS","207940.KS","005490.KS","105560.KS","055550.KS",
            "086790.KS","035420.KS","035720.KS","017670.KS","030200.KS",
            "006400.KS","003670.KS","066570.KS","009150.KS","012330.KS",
            "011170.KS","096770.KS","028260.KS","032830.KS","000810.KS",
            "034730.KS","018260.KS","003550.KS","004020.KS","011070.KS",
            "042700.KS","064760.KS","102120.KS","247540.KS","042660.KS",
            "000720.KS","047040.KS","006360.KS","000210.KS","026890.KS",
            "032640.KS","251270.KS","263750.KS","259960.KS","036570.KS",
            "069960.KS","004170.KS","282330.KS","139480.KS","161390.KS",
        ]
        data = yf.download(
            " ".join(sample),
            start=start_date[:4]+"-"+start_date[4:6]+"-"+start_date[6:],
            end=end_date[:4]+"-"+end_date[4:6]+"-"+end_date[6:],
            progress=False, auto_adjust=True
        )
        close_df = data["Close"] if "Close" in data else data
        for sym in sample:
            try:
                prices = close_df[sym].dropna()
                if len(prices) < 50:
                    continue
                ma50 = prices.rolling(50).mean()
                if float(prices.iloc[-1]) > float(ma50.iloc[-1]):
                    above += 1
                total += 1
            except Exception:
                pass
    except Exception as e:
        log.warning(f"  yfinance 신호등 실패: {e}")

    # FDR fallback
    if total < 10 and FDR_OK:
        try:
            df_ks = fdr.DataReader("KS11",
                start_date[:4]+"-"+start_date[4:6]+"-"+start_date[6:],
                end_date[:4]+"-"+end_date[4:6]+"-"+end_date[6:])
            if df_ks is not None and len(df_ks) > 50:
                close = df_ks["Close"].astype(float)
                ma50 = close.rolling(50).mean()
                val = round(float(close.iloc[-1]) / float(ma50.iloc[-1]) * 50, 1)
                val = min(90, max(10, val))
                above, total = int(val), 100
        except Exception as e:
            log.warning(f"  FDR 신호등 fallback 실패: {e}")

    value = round(above / total * 100, 1) if total > 0 else 50
    if value >= SIGNAL_RED:
        signal, label, action = "red",    "🔴 과열",   "신규 매수 자제"
    elif value >= SIGNAL_GREEN:
        signal, label, action = "green",  "🟢 강세장", "적극적 매수 및 보유"
    elif value >= SIGNAL_BLUE:
        signal, label, action = "yellow", "🟡 약세장", "현금 비중 확대"
    else:
        signal, label, action = "blue",   "🔵 과매도", "역발상 매수 기회"

    log.info(f"  KOSPI 신호: {label} ({value}%)")
    return {
        "value": value, "above": above, "total": total,
        "signal": signal, "label": label, "action": action,
        "overall": signal, "overall_label": label,
        "invest_pct": f"{value:.0f}%",
        "ndfi": {"value": value, "action": action},
        "s5fi": {"value": value, "action": action},
    }


# ════════════════════════════════════════════════════════════
# 벤치마크 로드
# ════════════════════════════════════════════════════════════
def load_benchmark(start_date, end_date):
    """KOSPI 벤치마크 — FDR → yfinance"""
    def fmt(d): return f"{d[:4]}-{d[4:6]}-{d[6:]}"

    # 1) FDR
    if FDR_OK:
        try:
            df = fdr.DataReader("KS11", start_date, end_date)
            if df is not None and len(df) > 20:
                log.info(f"  FDR 벤치마크 로드 성공: {len(df)}일")
                return df["Close"].astype(float)
        except Exception as e:
            log.warning(f"  FDR 벤치마크 실패: {e}")

    # 2) yfinance
    try:
        import yfinance as yf
        df = yf.Ticker("^KS11").history(start=fmt(start_date), end=fmt(end_date))
        if df is not None and len(df) > 20:
            log.info(f"  yfinance 벤치마크 로드 성공: {len(df)}일")
            return df["Close"].astype(float)
    except Exception as e:
        log.warning(f"  yfinance 벤치마크 실패: {e}")

    log.error("벤치마크 로드 전체 실패")
    return None


# ════════════════════════════════════════════════════════════
# 종목별 지표 계산 (핵심)
# ════════════════════════════════════════════════════════════
def process_stock(ticker, name, market, benchmark):
    """
    단일 종목 지표 계산
    Returns: dict or None
    """
    start_date, end_date = date_range(HISTORY_DAYS)
    try:
        # 가격 데이터 (pykrx → FDR fallback)
        df = None
        if PYKRX_OK:
            try:
                df = krx.get_market_ohlcv(start_date, end_date, ticker)
                if df is not None and len(df) >= 60:
                    close  = df["종가"].astype(float)
                    high   = df["고가"].astype(float)
                    low    = df["저가"].astype(float)
                    volume = df["거래량"].astype(float)
                else:
                    df = None
            except Exception:
                df = None

        if df is None and FDR_OK:
            try:
                df = fdr.DataReader(ticker, start_date, end_date)
                if df is not None and len(df) >= 60:
                    close  = df["Close"].astype(float)
                    high   = df["High"].astype(float)
                    low    = df["Low"].astype(float)
                    volume = df["Volume"].astype(float)
                else:
                    df = None
            except Exception:
                df = None

        if df is None:
            return None

        n = len(close)
        latest = float(close.iloc[-1])

        # 최소 주가 필터
        if latest < MIN_PRICE:
            return None

        # 평균 거래량 필터
        avg_vol = float(volume.tail(20).mean())
        if avg_vol < MIN_AVG_VOLUME:
            return None

        # ── 이동평균 ──
        ma50  = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()

        has_ma200 = n >= 200
        ma50_now  = float(ma50.iloc[-1])  if n >= 50  else float(close.mean())
        ma150_now = float(ma150.iloc[-1]) if n >= 150 else float(close.mean())
        ma200_now = float(ma200.iloc[-1]) if has_ma200 else float(close.mean())

        # MA200 기울기 (21일 전 대비)
        ma200_clean = ma200.dropna()
        ma200_21d = float(ma200_clean.iloc[-22]) if len(ma200_clean) >= 22 else float(ma200_clean.iloc[0]) if len(ma200_clean) > 0 else ma200_now

        # ── 52주 고저 ──
        w52 = min(252, n)
        high_52w = float(high.iloc[-w52:].max())
        low_52w  = float(low.iloc[-w52:].min())

        # ── 주간 수익률 ──
        def pct(days):
            if n <= days: return 0.0
            base = float(close.iloc[-days-1])
            return round((latest / base - 1) * 100, 1) if base > 0 else 0.0

        w1 = pct(5)   # 1주
        w3 = pct(15)  # 3주
        w6 = pct(30)  # 6주

        # ── Stage 2 조건 ──
        c1 = latest > ma150_now                          # MA150 위
        c2 = latest > ma200_now                          # MA200 위
        c3 = ma200_now > ma200_21d                       # MA200 우상향
        c4 = ma50_now > ma150_now > ma200_now            # 정배열
        c5 = low_52w > 0 and (latest - low_52w) / low_52w * 100 >= FROM_52W_LOW   # 52주 저점 대비
        c6 = high_52w > 0 and (latest - high_52w) / high_52w * 100 >= FROM_52W_HIGH  # 52주 고점 근접

        # ── 가속 ──
        acc  = w1 > 0 and w3 > 0 and w1 > abs(w3) / 3
        acc2 = acc and w3 > 0 and w6 > 0

        # ── RS Line ──
        rs_pct_change = 0.0
        h52_new = False
        rs_line_lead = False
        try:
            common_idx = close.index.intersection(benchmark.index)
            if len(common_idx) >= 20:
                sp = close.reindex(common_idx)
                bp = benchmark.reindex(common_idx)
                rs_line = (sp / bp) * 100
                rs_now_val  = float(rs_line.iloc[-1])
                rs_start_val = float(rs_line.iloc[0])
                rs_pct_change = round(((rs_now_val / rs_start_val) - 1) * 100, 2) if rs_start_val != 0 else 0

                # RS Line 신고가 선행
                rs_52w_high = float(rs_line.iloc[-min(252, len(rs_line)):].max())
                h52_pct = (latest - high_52w) / high_52w * 100 if high_52w > 0 else 0
                rs_line_lead = (rs_now_val >= rs_52w_high * 0.98) and (h52_pct < -5)
        except Exception:
            pass

        h52_pct = round((latest - high_52w) / high_52w * 100, 1) if high_52w > 0 else 0
        h52_new = h52_pct >= -3

        # ── IBD RS 스타일 점수 (원시값, 나중에 퍼센타일로 변환) ──
        def ibd_ret(i_start, i_end):
            if n < i_start: return 0.0
            ps = float(close.iloc[max(0, n - i_start)])
            pe = float(close.iloc[n - i_end]) if i_end > 0 else float(close.iloc[-1])
            return (pe / ps - 1) * 100 if ps > 0 else 0.0

        ibd_raw = (ibd_ret(63, 0) * 0.4 +
                   ibd_ret(126, 63) * 0.2 +
                   ibd_ret(189, 126) * 0.2 +
                   ibd_ret(252, 189) * 0.2)

        return {
            "ticker":    ticker,
            "name":      name,
            "market":    market,
            "price":     round(latest, 0),
            "ma50":      round(ma50_now, 0),
            "ma150":     round(ma150_now, 0),
            "ma200":     round(ma200_now, 0),
            "high_52w":  round(high_52w, 0),
            "low_52w":   round(low_52w, 0),
            "h52_pct":   h52_pct,
            "h52_new":   h52_new,
            "w1": w1, "w3": w3, "w6": w6,
            "acc":  bool(acc),
            "acc2": bool(acc2),
            "rs_pct_change": rs_pct_change,
            "ibd_raw":   round(ibd_raw, 2),
            # RS, rs_score, ibd_rs → 나중에 퍼센타일로
            "rs": 0, "rs_score": 0, "rs_now": 0, "ibd_rs": 0,
            "rs_line_lead": bool(rs_line_lead),
            "is_stage2": False,
            "_base_stage2": bool(c1 and c2 and c3 and c4 and c5 and c6),
            "stage2": {
                "above_ma150":    bool(c1),
                "above_ma200":    bool(c2),
                "ma200_uptrend":  bool(c3),
                "ma_aligned":     bool(c4),
                "from_52w_low":   bool(c5),
                "from_52w_high":  bool(c6),
                "rs_rank":        False,
            },
            "pass_dots": [],
            "patterns":  [],
            "pattern_count": 0,
            "best_pattern": None,
            "pattern_detail": {},
            # 섹터 / 시총은 나중에 추가
            "sector_kr": "기타",
            "sector":    "Others",
            "mktcap":    0,
        }
    except Exception as e:
        log.debug(f"{ticker} 오류: {e}")
        return None


# ════════════════════════════════════════════════════════════
# RS 퍼센타일 계산 (전체 종목 기준)
# ════════════════════════════════════════════════════════════
def rank_all(stocks):
    if not stocks:
        return stocks

    rs_arr   = np.array([s["rs_pct_change"] for s in stocks])
    ibd_arr  = np.array([s["ibd_raw"] for s in stocks])
    w1_arr   = np.array([s["w1"] for s in stocks])

    for s in stocks:
        s["rs_score"] = int(np.sum(rs_arr  < s["rs_pct_change"]) / len(rs_arr)  * 100)
        s["ibd_rs"]   = int(np.sum(ibd_arr < s["ibd_raw"])       / len(ibd_arr) * 100)
        s["rs"]       = s["rs_score"]   # 통일
        s["rs_now"]   = int(np.sum(w1_arr < s["w1"]) / len(w1_arr) * 100)

        # Stage2 RS 조건 확정
        s["stage2"]["rs_rank"] = s["rs_score"] >= RS_RANK_MIN
        s["is_stage2"] = s.pop("_base_stage2", False) and s["stage2"]["rs_rank"]

        # pass_dots (7개 조건)
        s["pass_dots"] = [
            int(s["stage2"]["above_ma150"]),
            int(s["stage2"]["above_ma200"]),
            int(s["stage2"]["ma200_uptrend"]),
            int(s["stage2"]["ma_aligned"]),
            int(s["stage2"]["from_52w_low"]),
            int(s["stage2"]["from_52w_high"]),
            int(s["stage2"]["rs_rank"]),
        ]

    return stocks


# ════════════════════════════════════════════════════════════
# 섹터별 집계
# ════════════════════════════════════════════════════════════
def build_sector_analysis(stocks):
    from collections import defaultdict
    sec_map = defaultdict(list)
    for s in stocks:
        # 섹터가 "기타"인 경우 시장별로 분리
        sec_key = s["sector"]
        if sec_key in ("기타", "Others", "Other Manufacturing", "Other Services"):
            sec_key = s["market"] + " 기타"
        sec_map[sec_key].append(s)

    sectors = []
    for sector_en, stks in sec_map.items():
        if not stks:
            continue
        rs_scores = [s["rs_score"] for s in stks]
        ibd_scores = [s["ibd_rs"]  for s in stks]
        avg_rs  = round(np.mean(rs_scores), 1)
        avg_ibd = round(np.mean(ibd_scores), 1)
        strength = "strong" if avg_rs >= 75 else "neutral" if avg_rs >= 50 else "weak"
        top5 = sorted(stks, key=lambda x: x["rs_score"], reverse=True)[:5]
        sectors.append({
            "sector":       sector_en,
            "sector_kr":    sector_en,
            "avg_rs_score": avg_rs,
            "avg_ibd_rs":   avg_ibd,
            "max_rs_score": max(rs_scores),
            "stock_count":  len(stks),
            "strength":     strength,
            "strength_label": "강세" if strength=="strong" else "중립" if strength=="neutral" else "약세",
            "top_stocks": [{
                "ticker":   s["ticker"],
                "name":     s["name"],
                "market":   s["market"],
                "rs_score": s["rs_score"],
                "ibd_rs":   s["ibd_rs"],
                "price":    s["price"],
                "mktcap":   s["mktcap"],
            } for s in top5],
        })

    sectors.sort(key=lambda x: x["avg_rs_score"], reverse=True)
    return sectors


# ════════════════════════════════════════════════════════════
# 메인 스크리닝
# ════════════════════════════════════════════════════════════
def run_screening(max_stocks=9999, verbose=True):
    log.info("=" * 60)
    log.info("🇰🇷 한국 주식 추세추종 스크리너 v2 (실전)")
    log.info("=" * 60)

    today_str = latest_trading_day()
    start_date, end_date = date_range(HISTORY_DAYS)
    log.info(f"📅 분석 기준일: {today_str}  |  데이터 기간: {start_date}~{end_date}")

    # 1. 벤치마크
    log.info("\n📊 KOSPI 벤치마크 로드...")
    benchmark = load_benchmark(start_date, end_date)
    if benchmark is None:
        log.error("벤치마크 로드 실패. 종료.")
        return None

    # 2. 종목 유니버스
    log.info("\n📋 종목 유니버스 로드...")
    universe = get_stock_universe(today_str)

    # 3. 섹터 / 시총 일괄 조회
    log.info("\n🏭 섹터 정보 로드...")
    # pykrx 섹터 API 진단
    if PYKRX_OK:
        try:
            _test = krx.get_market_sector_classifications(today_str, market="KOSPI")
            if _test is not None and len(_test) > 0:
                log.info(f"  pykrx 섹터 컬럼 진단: {list(_test.columns)}")
                log.info(f"  첫 행 샘플: {_test.iloc[0].to_dict()}")
            else:
                log.warning("  pykrx 섹터 반환값 없음")
        except Exception as _e:
            log.warning(f"  pykrx 섹터 진단 실패: {_e}")
    sector_map = get_sector_map(today_str)

    log.info("\n💰 시가총액 일괄 로드...")
    mktcap_map = get_mktcap_map(today_str)

    # 시총 필터 적용
    universe = [s for s in universe
                if mktcap_map.get(s["ticker"], 0) >= MIN_MKTCAP_100M]
    log.info(f"  시총 {MIN_MKTCAP_100M}억 이상 필터 후: {len(universe)}개")

    # 4. 종목별 지표 계산
    targets = universe[:max_stocks]
    log.info(f"\n🔍 지표 계산 시작: {len(targets)}개 (예상 {round(len(targets)*0.05/60,1)}분)")

    results = []
    t0 = time.time()
    for i, sym in enumerate(targets):
        if verbose and (i+1) % 100 == 0:
            elapsed = round((time.time()-t0)/60, 1)
            remain  = round((len(targets)-i-1)*0.05/60, 1)
            pct_done = round((i+1)/len(targets)*100, 1)
            log.info(f"  [{i+1}/{len(targets)}] {pct_done}% | 경과 {elapsed}분 | 남은 {remain}분 | 성공 {len(results)}")

        r = process_stock(sym["ticker"], sym["name"], sym["market"], benchmark)
        if r:
            # 섹터 / 시총 추가
            sector_kr = sector_map.get(sym["ticker"], "기타")
            # 업종명 → 대분류 그룹핑
            sector_group = FDR_DEPT_MAP.get(sector_kr,
                           SECTOR_GROUP_MAP.get(
                               KRX_SECTOR_MAP.get(sector_kr, sector_kr),
                               sector_kr if sector_kr not in ("기타","") else sym["market"]+" 기타"
                           ))
            r["sector_kr"]     = sector_kr
            r["sector"]        = sector_group
            r["sector_detail"] = KRX_SECTOR_MAP.get(sector_kr, sector_kr)
            r["mktcap"]        = mktcap_map.get(sym["ticker"], 0)  # 억원
            results.append(r)

        time.sleep(0.05)  # API 부하 방지

    log.info(f"\n✅ 수집 완료: {len(results)}개 (실패: {len(targets)-len(results)}개)")

    if not results:
        log.error("수집된 종목 없음.")
        return None

    # 5. RS 퍼센타일 계산
    log.info("📈 RS 퍼센타일 계산...")
    results = rank_all(results)

    # 6. KOSPI 신호등
    log.info("\n📡 KOSPI 신호등 계산...")
    market_signal = calc_kospi_signal(start_date, end_date)

    # 7. 섹터 분석
    log.info("🏭 섹터 집계...")
    sectors = build_sector_analysis(results)

    # 8. 요약 통계
    stage2_count   = sum(1 for s in results if s["is_stage2"])
    acc_count      = sum(1 for s in results if s["acc2"])
    h52_count      = sum(1 for s in results if s["h52_new"])
    rsl_count      = sum(1 for s in results if s["rs_line_lead"])
    avg_rs         = round(float(np.mean([s["rs_score"] for s in results])), 1)
    strong_sectors = len([s for s in sectors if s["strength"] == "strong"])
    neutral_sectors= len([s for s in sectors if s["strength"] == "neutral"])
    weak_sectors   = len([s for s in sectors if s["strength"] == "weak"])
    largecap_count = sum(1 for s in results if s["mktcap"] >= 10000)   # 1조 이상
    megacap_count  = sum(1 for s in results if s["mktcap"] >= 100000)  # 10조 이상

    top_stocks = sorted(results, key=lambda x: x["rs_score"], reverse=True)[:100]

    output = {
        "meta": {
            "generated_at":   datetime.now().isoformat(),
            "analysis_date":  today_str,
            "market":         "KR",
            "total_stocks":   len(results),
            "stage2_count":   stage2_count,
            "acc_count":      acc_count,
            "h52_count":      h52_count,
            "rsl_count":      rsl_count,
            "largecap_count": largecap_count,
            "megacap_count":  megacap_count,
        },
        "summary": {
            "total_stocks":          len(results),
            "avg_rs_score":          avg_rs,
            "stage2_count":          stage2_count,
            "strong_sectors_count":  strong_sectors,
            "neutral_sectors_count": neutral_sectors,
            "weak_sectors_count":    weak_sectors,
        },
        "market": market_signal,
        "sectors":    sectors,
        "top_stocks": top_stocks,
        "all_stocks": sorted(results, key=lambda x: x["rs_score"], reverse=True),
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=_json_default)

    # 결과 요약 출력
    log.info("\n" + "=" * 60)
    log.info(f"✅ 완료! → {OUTPUT_FILE}")
    log.info(f"   분석 종목:  {len(results)}개")
    log.info(f"   Stage2:    {stage2_count}개")
    log.info(f"   RS Line선행:{rsl_count}개")
    log.info(f"   52주신고가: {h52_count}개")
    log.info(f"   강세 섹터:  {strong_sectors}개")
    log.info(f"   평균 RS:   {avg_rs}")
    log.info(f"   시장 신호:  {market_signal['label']}")
    log.info("=" * 60)
    _print_sector_summary(sectors)

    return output


def _print_sector_summary(sectors):
    log.info("\n📊 섹터 요약 (RS Score 기준)")
    log.info("-" * 50)
    for i, s in enumerate(sectors[:15], 1):
        bar = "█" * int(s["avg_rs_score"] / 5)
        log.info(f"#{i:2d} {s['sector_kr']:<12} RS:{s['avg_rs_score']:5.1f} {bar} [{s['strength_label']}]")


def _json_default(obj):
    if isinstance(obj, (np.bool_,)):     return bool(obj)
    if isinstance(obj, (np.integer,)):   return int(obj)
    if isinstance(obj, (np.floating,)):  return float(obj)
    if isinstance(obj, np.ndarray):      return obj.tolist()
    if isinstance(obj, pd.Timestamp):    return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


# ════════════════════════════════════════════════════════════
# 데모 모드
# ════════════════════════════════════════════════════════════
def generate_demo():
    import random
    random.seed(42)
    log.info("🎭 데모 모드 (실제 API 없음)")

    demo_stocks_raw = {
        "전기전자": [("005930","삼성전자"),("000660","SK하이닉스"),("009150","삼성전기"),("066570","LG전자"),("028260","삼성물산")],
        "반도체":   [("042700","한미반도체"),("102120","어보브반도체"),("064760","티씨케이"),("240810","원익IPS"),("336370","솔브레인홀딩스")],
        "의약품":   [("068270","셀트리온"),("207940","삼성바이오로직스"),("000100","유한양행"),("128940","한미약품"),("185750","종근당")],
        "화학":     [("051910","LG화학"),("011170","롯데케미칼"),("006400","삼성SDI"),("096770","SK이노베이션"),("010950","S-Oil")],
        "금융업":   [("105560","KB금융"),("055550","신한지주"),("086790","하나금융지주"),("316140","우리금융지주"),("003550","LG")],
        "운수장비": [("005380","현대차"),("000270","기아"),("012330","현대모비스"),("018880","한온시스템"),("011210","현대위아")],
        "건설업":   [("000720","현대건설"),("028260","삼성물산"),("047040","대우건설"),("006360","GS건설"),("000210","DL이앤씨")],
        "통신업":   [("017670","SK텔레콤"),("030200","KT"),("032640","LG유플러스")],
        "소프트웨어":[("035420","NAVER"),("035720","카카오"),("259960","크래프톤"),("263750","펄어비스"),("036570","엔씨소프트")],
        "유통업":   [("069960","현대백화점"),("004170","신세계"),("023530","롯데쇼핑"),("282330","BGF리테일"),("139480","이마트")],
        "철강금속": [("005490","POSCO홀딩스"),("004020","현대제철"),("001440","대한제강"),("012800","위니아홀딩스")],
        "기계":     [("042670","HD현대인프라코어"),("011210","현대위아"),("047080","하나마이크론")],
    }

    results = []
    for sector_kr, stocks in demo_stocks_raw.items():
        sector_en = KRX_SECTOR_MAP.get(sector_kr, sector_kr)
        base_rs = random.uniform(35, 90)
        for ticker, name in stocks:
            rs_score = min(99, max(1, base_rs + random.uniform(-20, 20)))
            ibd_rs   = min(99, max(1, rs_score + random.uniform(-10, 10)))
            w3 = random.uniform(-15, 30)
            w1 = random.uniform(-8, 15)
            w6 = random.uniform(-20, 40)
            mktcap = random.randint(500, 500000)
            dots_count = int(rs_score / 14.3)
            results.append({
                "ticker": ticker, "name": name,
                "market": "KOSPI" if random.random() > 0.3 else "KOSDAQ",
                "price":  random.randint(10000, 200000),
                "ma50":   random.randint(8000, 180000),
                "ma150":  random.randint(7000, 170000),
                "ma200":  random.randint(6000, 160000),
                "high_52w": random.randint(50000, 250000),
                "low_52w":  random.randint(5000, 50000),
                "h52_pct":  round(random.uniform(-30, 5), 1),
                "h52_new":  random.random() > 0.8,
                "w1": round(w1,1), "w3": round(w3,1), "w6": round(w6,1),
                "acc":  w1 > 0 and w3 > 0,
                "acc2": w1 > 0 and w3 > 0 and w6 > 0,
                "rs_score": round(rs_score), "rs": round(rs_score),
                "ibd_rs":   round(ibd_rs),   "rs_now": round(ibd_rs),
                "ibd_raw":  round(w3*0.4 + w6*0.2, 2),
                "rs_pct_change": round(w3, 2),
                "rs_line_lead": random.random() > 0.85,
                "is_stage2": rs_score >= 70 and dots_count >= 6,
                "sector_kr": sector_kr, "sector": sector_en,
                "mktcap": mktcap,
                "stage2": {
                    "above_ma150": True, "above_ma200": rs_score > 50,
                    "ma200_uptrend": rs_score > 60, "ma_aligned": rs_score > 65,
                    "from_52w_low": True, "from_52w_high": rs_score > 70,
                    "rs_rank": rs_score >= RS_RANK_MIN,
                },
                "pass_dots": [1,1,int(rs_score>50),int(rs_score>60),1,int(rs_score>70),int(rs_score>=RS_RANK_MIN)],
                "patterns": [], "pattern_count": 0,
                "best_pattern": None, "pattern_detail": {},
            })

    results.sort(key=lambda x: x["rs_score"], reverse=True)
    sectors = build_sector_analysis(results)

    avg_rs = round(float(np.mean([r["rs_score"] for r in results])), 1)
    output = {
        "meta": {
            "generated_at":  datetime.now().isoformat(),
            "analysis_date": datetime.now().strftime("%Y%m%d"),
            "market": "KR", "demo": True,
            "total_stocks": len(results),
            "stage2_count": sum(1 for s in results if s["is_stage2"]),
        },
        "summary": {
            "total_stocks":         len(results),
            "avg_rs_score":         avg_rs,
            "stage2_count":         sum(1 for s in results if s["is_stage2"]),
            "strong_sectors_count": len([s for s in sectors if s["strength"]=="strong"]),
            "neutral_sectors_count":len([s for s in sectors if s["strength"]=="neutral"]),
            "weak_sectors_count":   len([s for s in sectors if s["strength"]=="weak"]),
        },
        "market": {"signal":"green","label":"🟢 강세장","overall":"green","overall_label":"🟢 강세장","value":62.5},
        "sectors":    sectors,
        "top_stocks": results[:50],
        "all_stocks": results,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"✅ 데모 데이터 생성 → {OUTPUT_FILE} ({len(results)}종목)")
    _print_sector_summary(sectors)
    return output


# ════════════════════════════════════════════════════════════
# 엔트리포인트
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    args = sys.argv[1:]

    if "--demo" in args or (not PYKRX_OK and not FDR_OK):
        generate_demo()
    else:
        max_s = 9999
        for a in args:
            if a.isdigit():
                max_s = int(a)
                break
        run_screening(max_stocks=max_s)
