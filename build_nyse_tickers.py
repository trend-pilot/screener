# -*- coding: utf-8 -*-
"""
build_nyse_tickers.py — NYSE/AMEX 티커 목록 생성기
=====================================================================
[목적]
  screener.py 는 nasdaq_tickers.csv 외에 nyse_tickers.csv 가 있으면
  자동으로 합쳐서 스크리닝한다 (get_symbols() 의 'nyse 추가' 로직).
  이 스크립트는 그 nyse_tickers.csv 를 만든다.

[데이터 출처]
  NASDAQ Trader 공식 심볼 디렉터리 (무료·공개):
    https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt
  otherlisted.txt = NASDAQ 이외 거래소(NYSE, NYSE American/AMEX, ARCA 등) 상장 종목.
  파이프(|) 구분 텍스트이며 마지막 줄은 파일 생성 시각 안내라 제외한다.

[제외 규칙] — screener.py 의 유니버스 정책과 동일 방향
  · ETF (별도 etf_tickers.csv 로 관리)
  · Test Issue (테스트용 심볼)
  · 워런트/유닛/권리/우선주 (티커 접미 W/U/R/P, 5자 이상)
  · 심볼에 '.' 또는 '$' 가 포함된 종목 (우선주 시리즈 BAC.PB, 유닛 AACT.U 등)

[사용법]
    python build_nyse_tickers.py                 # nyse_tickers.csv 생성
    python build_nyse_tickers.py --out other.csv # 파일명 지정
    python build_nyse_tickers.py --dry-run       # 저장 없이 개수만 확인

  생성된 nyse_tickers.csv 를 저장소 루트(screener.py 옆)에 두면 끝.
  screener.py 는 수정할 필요가 없다.
"""

import argparse
import csv
import re
import sys
import urllib.request

SOURCE_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# 워런트(W)/유닛(U)/권리(R)/우선주(P) 추정 — screener.py 의 is_non_common 과 동일 규칙
NON_COMMON = re.compile(r"^[A-Z]{4,}(W|U|R)$|^[A-Z]{4,}P[A-Z]?$")


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace")


def parse(text: str):
    """
    otherlisted.txt 컬럼:
      ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
    Exchange: A=NYSE American(AMEX), N=NYSE, P=ARCA, Z=BATS, V=IEX
    """
    rows, skipped = [], {"etf": 0, "test": 0, "noncommon": 0, "symbol": 0, "exchange": 0}
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return rows, skipped
    header = [h.strip() for h in lines[0].split("|")]
    idx = {name: i for i, name in enumerate(header)}

    def col(parts, name):
        i = idx.get(name)
        return parts[i].strip() if (i is not None and i < len(parts)) else ""

    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        sym = col(parts, "ACT Symbol") or col(parts, "NASDAQ Symbol")
        name = col(parts, "Security Name")
        exch = col(parts, "Exchange")
        etf = col(parts, "ETF")
        test = col(parts, "Test Issue")

        if test == "Y":
            skipped["test"] += 1;  continue
        if etf == "Y":
            skipped["etf"] += 1;   continue
        # NYSE(N) + NYSE American(A) 만. ARCA/BATS/IEX 는 대부분 ETF·중복 상장.
        if exch not in ("N", "A"):
            skipped["exchange"] += 1;  continue
        # 심볼 정제: 알파벳(+하이픈)만, 6자 이하
        s = sym.replace("/", "-").strip()
        # NYSE 관례상 '.' 또는 '$' 가 붙은 심볼은 우선주 시리즈(BAC.PB)·유닛(AACT.U) 이므로 제외
        if not s or len(s) > 6 or ("." in s) or ("$" in s) or not s.replace("-", "").isalpha():
            skipped["symbol"] += 1;  continue
        if NON_COMMON.match(s):
            skipped["noncommon"] += 1; continue

        rows.append({"symbol": s, "name": name or s})

    # 중복 제거 (심볼 기준)
    seen, uniq = set(), []
    for r in rows:
        if r["symbol"] in seen:
            continue
        seen.add(r["symbol"]); uniq.append(r)
    return uniq, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="nyse_tickers.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"다운로드: {SOURCE_URL}")
    try:
        text = fetch(SOURCE_URL)
    except Exception as e:
        print(f"❌ 다운로드 실패: {e}", file=sys.stderr)
        sys.exit(1)

    rows, skipped = parse(text)
    print(f"제외 — ETF {skipped['etf']} · 테스트 {skipped['test']} · "
          f"타거래소 {skipped['exchange']} · 비보통주 {skipped['noncommon']} · 심볼형식 {skipped['symbol']}")
    print(f"✅ NYSE/AMEX 보통주 {len(rows)}개")
    if rows[:5]:
        print("   예시:", ", ".join(r["symbol"] for r in rows[:8]))

    if args.dry_run:
        print("(--dry-run: 저장하지 않음)")
        return

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "name"])
        w.writeheader()
        w.writerows(rows)
    print(f"저장 완료 → {args.out}")


if __name__ == "__main__":
    main()
