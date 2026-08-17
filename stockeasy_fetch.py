# -*- coding: utf-8 -*-
"""
stockeasy_fetch.py — 스탁이지 전략실 스냅샷 수집 · diff · 텔레그램 알림 (Phase 0)
================================================================================
전략실 페이지는 로그인 없이 SSR HTML 로 보유/이탈 테이블을 내려준다.
이 스크립트는 그 테이블을 파싱해 스냅샷 JSON 으로 남기고, 전일 스냅샷과
비교해 신규 편입 / 이탈을 텔레그램으로 알린다.

종목코드는 kr_screener_output.json 의 name 필드로 조인해 붙인다.
(별도 KRX 마스터 불필요 — 매칭 실패분은 unmapped 로 분리해 알림에 경고)

출력:  stockeasy_kr.json   ← Phase 1 enrich_kr.py 의 입력이 된다

실행:
    python stockeasy_fetch.py                       # 1호 모멘텀, 알림 O
    python stockeasy_fetch.py --strategy peak       # 2호 피크
    python stockeasy_fetch.py --no-notify           # 수집만
    python stockeasy_fetch.py --html saved.html     # 오프라인 테스트

환경변수:
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

주의: 개인 알림 용도. 하루 1~2회만 호출한다 (이용약관 제6조 3호 — 부하 유발 금지).
      수집 결과를 재배포·상업적 이용하지 않는다 (제7조 2항).

변경 이력:
    v1.0  최초
    v1.1  rowspan 병합 셀 파싱 (섹터 열이 빈 td 가 아니라 rowspan 이었음)
    v1.2  보유 0 = 전량 이탈 처리. 파싱 실패와 구분한다 — 전량 이탈은
          가장 중요한 신호인데 기존 코드는 알림 없이 종료했다.
"""

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import date, datetime

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("bs4 가 필요합니다:  pip install beautifulsoup4")

BASE = "https://stockeasy.intellio.kr/strategy-room"
STRATEGIES = {"momentum": "1호 모멘텀", "peak": "2호 피크", "value": "3호 밸류"}

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

STOP_PCT = -7.0   # 전략실 하드스톱 (알림 참고 표시용)


# ─── 수집 ────────────────────────────────────────────────────────────
def fetch_html(strategy, timeout=20):
    url = f"{BASE}/{strategy}"
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept-Language": "ko-KR,ko;q=0.9"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def _num(txt):
    """'1,551,000' / '-3.70%' / '+8.01%' → float. 실패 시 None."""
    if txt is None:
        return None
    t = re.sub(r"[,%\s원]", "", str(txt)).replace("−", "-")
    try:
        return float(t)
    except ValueError:
        return None


def _mmdd(txt, ref_year):
    """'08/10' → '2026-08-10'. 연말연시 롤오버 보정 포함."""
    m = re.match(r"(\d{1,2})/(\d{1,2})", (txt or "").strip())
    if not m:
        return None
    mm, dd = int(m.group(1)), int(m.group(2))
    y = ref_year
    if mm >= 11 and date.today().month <= 2:
        y -= 1
    try:
        return date(y, mm, dd).isoformat()
    except ValueError:
        return None


def _rows_from_table(table):
    """thead 헤더 + tbody 행. rowspan 병합 셀을 그리드로 펼친다.

    섹터 열은 같은 섹터 종목들에 rowspan 으로 병합돼 있어서, 2번째 행부터는
    td 가 아예 없다(빈 td 가 아니다). 단순 zip 으로는 열이 밀린다.
    """
    heads = [th.get_text(strip=True) for th in table.select("thead th")]
    if not heads:
        first = table.find("tr")
        heads = [c.get_text(strip=True) for c in first.find_all(["th", "td"])] if first else []
    ncol = len(heads)
    if not ncol:
        return []

    body = table.find("tbody") or table
    carry = {}          # col -> [text, 남은 행수]
    out = []
    for tr in body.find_all("tr"):
        raw = tr.find_all(["td", "th"])
        if not raw:
            continue
        row = [None] * ncol
        # 위 행에서 rowspan 으로 내려온 셀 먼저 채운다
        for col in sorted(carry):
            txt, left = carry[col]
            if col < ncol:
                row[col] = txt
            carry[col][1] = left - 1
        carry = {c: v for c, v in carry.items() if v[1] > 0}
        # 남은 빈 칸에 이 행의 셀을 순서대로 배치
        for cell in raw:
            try:
                col = row.index(None)
            except ValueError:
                break
            txt = cell.get_text(strip=True)
            row[col] = txt
            try:
                rs = int(cell.get("rowspan") or 1)
            except ValueError:
                rs = 1
            if rs > 1:
                carry[col] = [txt, rs - 1]
        if all(v is None for v in row):
            continue
        row = ["" if v is None else v for v in row]
        if row == heads:
            continue
        out.append(dict(zip(heads, row)))
    return out


def _pick(row, *names):
    for n in names:
        if n in row and row[n] != "":
            return row[n]
    return None


def parse_page(html, ref_year=None):
    """보유/이탈 테이블 + updated 날짜를 뽑는다."""
    ref_year = ref_year or date.today().year
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    m = re.search(r"updated\s*(\d{1,2}/\d{1,2})", text)
    data_date = _mmdd(m.group(1), ref_year) if m else date.today().isoformat()

    holdings, exits = [], []
    for tbl in soup.find_all("table"):
        rows = _rows_from_table(tbl)
        if not rows:
            continue
        keys = set().union(*(set(r.keys()) for r in rows))
        is_exit = "매도가" in keys
        for r in rows:
            name = _pick(r, "종목명")
            if not name:
                continue
            sec_raw = _pick(r, "섹터") or ""
            sec_m = re.search(r"(\d+)$", sec_raw)
            rec = {
                "name": name,
                "sector": re.sub(r"\d+$", "", sec_raw),
                "sector_score": _num(sec_m.group(1)) if sec_m else None,
                "src_entry_px": _num(_pick(r, "매수가")),
                "entry_date": _mmdd(_pick(r, "편입일"), ref_year),
                "ret_pct": _num(_pick(r, "수익률")),
            }
            if is_exit:
                rec["exit_px"] = _num(_pick(r, "매도가"))
                exits.append(rec)
            else:
                rec["cur"] = _num(_pick(r, "현재가"))
                holdings.append(rec)

    return {"data_date": data_date, "holdings": holdings, "exits": exits}


# ─── 종목코드 매핑 ───────────────────────────────────────────────────
def _norm(s):
    return re.sub(r"[\s\(\)·\.]", "", (s or "")).upper()


def load_code_map(path):
    """kr_screener_output.json → {정규화이름: {ticker, market, rs_score, ...}}"""
    if not path or not os.path.exists(path):
        print(f"[map] {path} 없음 — 종목코드 매핑 생략")
        return {}
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    m = {}
    for s in (raw.get("all_stocks") or raw.get("top_stocks") or []):
        n = _norm(s.get("name"))
        if not n:
            continue
        m[n] = {"ticker": s.get("ticker"), "market": s.get("market"),
                "rs_score": s.get("rs_score"), "ibd_rs": s.get("ibd_rs"),
                "is_stage2": s.get("is_stage2"),
                "pass_dots": sum(s.get("pass_dots") or []),
                "h52_new": s.get("h52_new"), "acc2": s.get("acc2"),
                "sector_kr": s.get("sector_kr"), "mktcap": s.get("mktcap")}
    print(f"[map] {len(m)}종목 로드")
    return m


def enrich(snap, code_map):
    unmapped = []
    for bucket in ("holdings", "exits"):
        for r in snap[bucket]:
            hit = code_map.get(_norm(r["name"]))
            if hit:
                r.update({k: v for k, v in hit.items() if v is not None})
            else:
                r["ticker"] = None
                unmapped.append(r["name"])
    snap["unmapped"] = sorted(set(unmapped))
    return snap


# ─── diff ────────────────────────────────────────────────────────────
def diff(prev, cur):
    """전일 스냅샷 대비 신규/이탈. 이름 기준(코드 없는 종목도 추적)."""
    if not prev:
        return {"new": [], "dropped": [], "first_run": True}
    p = {r["name"] for r in prev.get("holdings", [])}
    c = {r["name"] for r in cur.get("holdings", [])}
    return {
        "new": [r for r in cur["holdings"] if r["name"] not in p],
        "dropped": [r for r in prev["holdings"] if r["name"] not in c],
        "first_run": False,
    }


# ─── 알림 ────────────────────────────────────────────────────────────
def build_message(snap, d, label):
    L = []
    L.append(f"📋 <b>[국장] 스탁이지 {label}</b> · {snap['data_date']}")
    L.append(f"보유 {len(snap['holdings'])} · 신규 {len(d['new'])} · 이탈 {len(d['dropped'])}")

    # 전량 이탈 — 소스가 전부 손을 털었다는 뜻. 가장 눈에 띄어야 하는 상태다.
    if not snap["holdings"]:
        L.append("\n🚨 <b>보유 0 — 전량 이탈</b>")

    if d["first_run"]:
        L.append("\n<i>첫 실행 — 기준 스냅샷을 저장했습니다.</i>")

    if d["new"]:
        L.append("\n🟢 <b>신규 편입</b>")
        for r in d["new"]:
            px = r.get("src_entry_px")
            code = f" <code>{r['ticker']}</code>" if r.get("ticker") else " ⚠️코드미매칭"
            L.append(f"• {r['name']}{code} — {px:,.0f}원" if px else f"• {r['name']}{code}")
            if px:
                L.append(f"  └ 전략실 스탑(-7%) {px * (1 + STOP_PCT / 100):,.0f}원")
            bits = []
            if r.get("rs_score") is not None:
                bits.append(f"RS {r['rs_score']}")
            if r.get("pass_dots") is not None:
                bits.append(f"dots {r['pass_dots']}/7")
            if r.get("h52_new"):
                bits.append("🚀신고가")
            if bits:
                L.append(f"  └ {' · '.join(bits)}")

    if d["dropped"]:
        L.append("\n🔴 <b>이탈</b>")
        for r in d["dropped"]:
            rp = r.get("ret_pct")
            L.append(f"• {r['name']} {rp:+.2f}%" if rp is not None else f"• {r['name']}")

    if snap.get("unmapped"):
        L.append(f"\n⚠️ 코드 미매칭 {len(snap['unmapped'])}건: {', '.join(snap['unmapped'][:5])}")

    L.append("\n<i>※ 관찰용 · 자동 발주 없음</i>")
    return "\n".join(L)


def notify(text):
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not (tok and chat):
        print("[tg] 토큰/챗ID 없음 — 콘솔 출력만\n" + "─" * 50 + f"\n{text}\n" + "─" * 50)
        return False
    payload = json.dumps({"chat_id": chat, "text": text,
                          "parse_mode": "HTML",
                          "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{tok}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            ok = json.load(r).get("ok")
        print(f"[tg] 발송 {'성공' if ok else '실패'}")
        return bool(ok)
    except Exception as e:
        print(f"[tg] 발송 실패: {e}")
        return False


# ─── main ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="momentum", choices=list(STRATEGIES))
    ap.add_argument("--out", default="stockeasy_kr.json")
    ap.add_argument("--screener", default="kr_screener_output.json")
    ap.add_argument("--html", help="오프라인 테스트용 저장 HTML")
    ap.add_argument("--no-notify", action="store_true")
    a = ap.parse_args()

    html = open(a.html, encoding="utf-8").read() if a.html else fetch_html(a.strategy)
    snap = parse_page(html)

    # 보유 0 과 파싱 실패는 정반대 상황이다.
    #   전량 이탈이면 이탈 테이블에는 종목이 남아 있다.
    #   둘 다 비어 있을 때만 파싱 실패로 판정한다.
    if not snap["holdings"] and not snap["exits"]:
        sys.exit("[!] 보유·이탈 테이블 모두 비어 있음 — 파싱 실패로 판단합니다.")
    if not snap["holdings"]:
        print("[!] 보유 0종목 — 전량 이탈 상태입니다.")

    print(f"[parse] {snap['data_date']} · 보유 {len(snap['holdings'])} · 이탈 {len(snap['exits'])}")

    snap = enrich(snap, load_code_map(a.screener))
    snap.update({"source": f"stockeasy:{a.strategy}",
                 "strategy_label": STRATEGIES[a.strategy],
                 "fetched_at": datetime.now().isoformat(timespec="seconds")})

    prev = None
    if os.path.exists(a.out):
        try:
            prev = json.load(open(a.out, encoding="utf-8"))
        except Exception as e:
            print(f"[state] 이전 스냅샷 로드 실패({e}) — 첫 실행으로 처리")

    # 같은 data_date 재실행이면 diff 를 재계산하지 않는다 (중복 알림 방지)
    if prev and prev.get("data_date") == snap["data_date"]:
        print(f"[skip] {snap['data_date']} 스냅샷 이미 처리됨 — 알림 생략")
        snap["diff"] = prev.get("diff", {"new": [], "dropped": [], "first_run": False})
        json.dump(snap, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        return

    d = diff(prev, snap)
    snap["diff"] = d
    json.dump(snap, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"[out] {a.out} · 신규 {len(d['new'])} · 이탈 {len(d['dropped'])}")

    if not a.no_notify:
        notify(build_message(snap, d, STRATEGIES[a.strategy]))


if __name__ == "__main__":
    main()
