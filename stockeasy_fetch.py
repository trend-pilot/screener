# -*- coding: utf-8 -*-
"""
stockeasy_fetch.py — 스탁이지 전략실 스냅샷 수집 · diff · 텔레그램 알림
================================================================================
전략실 페이지는 로그인 없이 SSR HTML 로 보유/이탈 테이블을 내려준다.
이 스크립트는 그 테이블을 파싱해 스냅샷으로 남기고, 전일과 비교해 신규 편입 /
이탈을 알린다. 몇 달치 이력 축적이 목적이므로 수집 신뢰성을 최우선으로 둔다.

출력:
    stockeasy_kr.json        최신 스냅샷 (덮어쓰기) — enrich_kr.py 의 입력
    stockeasy_history.jsonl  일별 이력 (append) — 백테스트/분석용
    kr_ticker_master.json    종목코드 폴백 캐시 (주 1회 갱신)

실행:
    python stockeasy_fetch.py
    python stockeasy_fetch.py --strategy peak
    python stockeasy_fetch.py --no-notify
    python stockeasy_fetch.py --html saved.html
    python stockeasy_fetch.py --no-notify --out test_kr.json   (로컬 테스트)

환경변수:
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

주의: 개인 알림 용도. 하루 1~2회만 호출 (이용약관 제6조 3호 — 부하 유발 금지).
      수집 결과를 재배포·상업적 이용하지 않는다 (제7조 2항).

변경 이력:
    v1.0  최초
    v1.1  rowspan 병합 셀 파싱
    v1.2  보유 0 = 전량 이탈 처리 (파싱 실패와 구분)
    v1.3  신뢰성 보강 — 이력 누적 · stale 감지 · 종목수 급변 경고 · 주간 하트비트
    v1.4  종목코드 폴백 — 스크리너는 자체 필터로 매일 구성이 바뀌므로
          (1635→1628) 단독 매핑 소스로 쓰면 신규 편입 종목이 누락된다.
"""

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("bs4 가 필요합니다:  pip install beautifulsoup4")

BASE = "https://stockeasy.intellio.kr/strategy-room"
STRATEGIES = {"momentum": "1호 모멘텀", "peak": "2호 피크", "value": "3호 밸류"}

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

STOP_PCT = -7.0        # 전략실 하드스톱 (종가 판정 · 알림 참고 표시용)
STALE_MAX_BD = 3       # 사이트 미갱신 경고 임계 (영업일)
SHOCK_RATIO = 0.5      # 종목수 급감 경고 비율
SHOCK_MIN_BASE = 10    # 이 미만이면 급변 판정 안 함


# ─── 신뢰성 가드 ─────────────────────────────────────────────────────
def bdays_between(d1, d2):
    """두 ISO 날짜 사이 영업일 수 (주말만 제외 · 공휴일 무시)."""
    try:
        a = datetime.strptime(d1, "%Y-%m-%d").date()
        b = datetime.strptime(d2, "%Y-%m-%d").date()
    except Exception:
        return 0
    if a > b:
        a, b = b, a
    n, cur = 0, a
    while cur < b:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def check_stale(data_date, today, max_bd=STALE_MAX_BD):
    """사이트 data_date 가 N영업일 넘게 안 바뀌면 경고."""
    bd = bdays_between(data_date, today)
    return (bd >= max_bd), bd


def check_count_shock(prev_n, cur_n, ratio=SHOCK_RATIO, min_base=SHOCK_MIN_BASE):
    """종목 수 급감 감지 — DOM 변경/부분 파싱 실패의 대리 신호.

    전량 이탈(cur_n==0)은 정상 상태라 여기서 잡지 않는다.
    별도 '보유 0' 알림이 이미 처리한다.
    """
    if prev_n is None or prev_n < min_base or cur_n == 0:
        return False, None
    if cur_n < prev_n * ratio:
        return True, f"{prev_n}→{cur_n}"
    return False, None


def is_heartbeat_day(today, weekday=4):
    """주간 하트비트 — 기본 금요일(4). 변화가 없어도 살아있음을 알린다."""
    try:
        return datetime.strptime(today, "%Y-%m-%d").date().weekday() == weekday
    except Exception:
        return False


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

    섹터 열은 같은 섹터 종목들에 rowspan 으로 병합돼 있어서 2번째 행부터
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
        for col in sorted(carry):
            txt, left = carry[col]
            if col < ncol:
                row[col] = txt
            carry[col][1] = left - 1
        carry = {c: v for c, v in carry.items() if v[1] > 0}
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
        print(f"[map] {path} 없음 — 스크리너 매핑 생략")
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


def _pick_col(df, *cands):
    for c in cands:
        if c in df.columns:
            return c
    return None


def load_fallback_map(cache="kr_ticker_master.json", max_age_days=7):
    """KRX 전체 상장종목 name→code. 스크리너에서 못 찾은 종목용 폴백.

    스크리너는 자체 필터로 매일 종목 구성이 바뀐다(1635→1628). 단독 매핑
    소스로 쓰면 신규 편입 종목이 코드 없이 누락돼 매수 자체가 안 된다.
    """
    if os.path.exists(cache):
        try:
            c = json.load(open(cache, encoding="utf-8"))
            age = (date.today() - datetime.strptime(c["as_of"], "%Y-%m-%d").date()).days
            if age <= max_age_days and c.get("map"):
                print(f"[fallback] 캐시 사용 ({len(c['map'])}종목 · {age}일 전)")
                return c["map"]
        except Exception as e:
            print(f"[fallback] 캐시 손상({e}) — 재생성")

    try:
        import FinanceDataReader as fdr
        df = fdr.StockListing("KRX")
        cc = _pick_col(df, "Code", "Symbol")
        nc = _pick_col(df, "Name")
        mc = _pick_col(df, "Market")
        if not (cc and nc):
            print(f"[fallback] 컬럼 인식 실패: {list(df.columns)[:8]}")
            return {}
        rows = [{"code": str(r[cc]).zfill(6), "name": r[nc],
                 "market": r[mc] if mc else None} for _, r in df.iterrows()]
    except ImportError:
        print("[fallback] FinanceDataReader 없음 — 폴백 생략")
        return {}
    except Exception as e:
        print(f"[fallback] 목록 수집 실패({e}) — 폴백 생략")
        return {}

    m = {}
    for r in rows:
        n = _norm(r.get("name"))
        code = str(r["code"]).zfill(6)
        if n and code.isdigit() and len(code) == 6:
            m[n] = {"ticker": code, "market": r.get("market")}
    try:
        json.dump({"as_of": date.today().isoformat(), "map": m},
                  open(cache, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception as e:
        print(f"[fallback] 캐시 저장 실패({e})")
    print(f"[fallback] 갱신 {len(m)}종목")
    return m


def enrich(snap, code_map):
    """스크리너 매핑 우선, 실패분만 폴백으로 재시도."""
    unmapped = []
    for bucket in ("holdings", "exits"):
        for r in snap[bucket]:
            hit = code_map.get(_norm(r["name"]))
            if hit:
                r.update({k: v for k, v in hit.items() if v is not None})
            else:
                r["ticker"] = None
                unmapped.append(r)

    recovered = []
    if unmapped:
        fb = load_fallback_map()
        for r in unmapped:
            hit = fb.get(_norm(r["name"]))
            if hit:
                r["ticker"] = hit["ticker"]
                if hit.get("market"):
                    r["market"] = hit["market"]
                r["code_source"] = "fallback"
                recovered.append(r["name"])

    snap["unmapped"] = sorted({r["name"] for r in unmapped if not r.get("ticker")})
    snap["code_recovered"] = sorted(set(recovered))
    if recovered:
        print(f"[map] 폴백 복구 {len(recovered)}건: {', '.join(recovered[:5])}")
    return snap


# ─── diff · 이력 ─────────────────────────────────────────────────────
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


def append_history(path, snap, d):
    """일별 이력 누적 (jsonl · 한 줄 = 하루).

    최신 스냅샷 파일은 덮어쓰기라 과거가 사라진다. 몇 달치 데이터 축적이
    목적이므로 별도 append 파일을 둔다. 같은 data_date 는 중복 기록하지 않는다.
    """
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and json.loads(line).get("data_date") == snap["data_date"]:
                        return False
        except Exception as e:
            print(f"[hist] 기존 이력 확인 실패({e}) — 그대로 append")

    rec = {
        "data_date": snap["data_date"],
        "logged_at": snap.get("fetched_at"),
        "source": snap.get("source"),
        "n_holdings": len(snap["holdings"]),
        "n_exits": len(snap["exits"]),
        "n_new": len(d["new"]),
        "n_dropped": len(d["dropped"]),
        "holdings": [{"name": r["name"], "ticker": r.get("ticker"),
                      "sector": r.get("sector"),
                      "src_entry_px": r.get("src_entry_px"),
                      "cur": r.get("cur"), "entry_date": r.get("entry_date"),
                      "ret_pct": r.get("ret_pct")} for r in snap["holdings"]],
        "exits": [{"name": r["name"], "ticker": r.get("ticker"),
                   "src_entry_px": r.get("src_entry_px"),
                   "exit_px": r.get("exit_px"), "entry_date": r.get("entry_date"),
                   "ret_pct": r.get("ret_pct")} for r in snap["exits"]],
        "new": [r["name"] for r in d["new"]],
        "dropped": [r["name"] for r in d["dropped"]],
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return True


def count_history(path):
    if not os.path.exists(path):
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


# ─── 알림 ────────────────────────────────────────────────────────────
def build_message(snap, d, label, alerts=None, hist_n=None):
    L = []
    L.append(f"📋 <b>[국장] 스탁이지 {label}</b> · {snap['data_date']}")
    L.append(f"보유 {len(snap['holdings'])} · 신규 {len(d['new'])} · 이탈 {len(d['dropped'])}")

    for a in (alerts or []):
        L.append(f"\n⚠️ <b>{a}</b>")

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

    if snap.get("code_recovered"):
        L.append(f"\n🔧 코드 폴백 복구 {len(snap['code_recovered'])}건: "
                 f"{', '.join(snap['code_recovered'][:3])}")

    if snap.get("unmapped"):
        L.append(f"\n⚠️ 코드 미매칭 {len(snap['unmapped'])}건: {', '.join(snap['unmapped'][:5])}")

    if hist_n is not None:
        L.append(f"\n<i>※ 관찰용 · 자동 발주 없음 · 이력 {hist_n}일치</i>")
    else:
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
    ap.add_argument("--history", default="stockeasy_history.jsonl")
    ap.add_argument("--screener", default="kr_screener_output.json")
    ap.add_argument("--html", help="오프라인 테스트용 저장 HTML")
    ap.add_argument("--no-notify", action="store_true")
    ap.add_argument("--heartbeat-day", type=int, default=4,
                    help="주간 하트비트 요일 (0=월 … 4=금, -1=끄기)")
    a = ap.parse_args()

    today = date.today().isoformat()
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

    # ── 신뢰성 가드 ──────────────────────────────────────────────
    alerts = []
    stale, stale_bd = check_stale(snap["data_date"], today)
    if stale:
        alerts.append(f"사이트 미갱신 {stale_bd}영업일 — 데이터 정지 의심")
        print(f"[warn] stale: {snap['data_date']} 이후 {stale_bd}영업일 경과")

    prev_n = len(prev.get("holdings", [])) if prev else None
    shock, shock_msg = check_count_shock(prev_n, len(snap["holdings"]))
    if shock:
        alerts.append(f"종목수 급감 {shock_msg} — 파싱 오류 의심")
        print(f"[warn] count shock: {shock_msg}")

    # ── 같은 data_date 재실행 — 알림은 생략하되 가드/하트비트는 살린다 ──
    if prev and prev.get("data_date") == snap["data_date"]:
        print(f"[skip] {snap['data_date']} 스냅샷 이미 처리됨 — diff 알림 생략")
        snap["diff"] = prev.get("diff", {"new": [], "dropped": [], "first_run": False})
        snap["alerts"] = alerts
        json.dump(snap, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

        hb = (a.heartbeat_day >= 0 and is_heartbeat_day(today, a.heartbeat_day))
        if not a.no_notify and (alerts or hb):
            n = count_history(a.history)
            head = "🫀 <b>주간 점검</b>\n" if hb and not alerts else ""
            msg = head + build_message(snap, snap["diff"], STRATEGIES[a.strategy],
                                       alerts, hist_n=n)
            notify(msg)
        return

    d = diff(prev, snap)
    snap["diff"] = d
    snap["alerts"] = alerts
    json.dump(snap, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    added = append_history(a.history, snap, d)
    n = count_history(a.history)
    print(f"[out] {a.out} · 신규 {len(d['new'])} · 이탈 {len(d['dropped'])}")
    print(f"[hist] {a.history} · {'추가' if added else '중복 스킵'} · 누적 {n}일")

    if not a.no_notify:
        notify(build_message(snap, d, STRATEGIES[a.strategy], alerts, hist_n=n))


if __name__ == "__main__":
    main()
