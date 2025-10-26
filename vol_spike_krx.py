import os
import time
import requests
import pandas as pd
import yfinance as yf

BOT_TOKEN = os.getenv("TG_BOT_TOKEN") or "여기에_봇_토큰_붙여넣기"
CHAT_ID   = os.getenv("TG_CHAT_ID")   or "여기에_숫자_chat_id_붙여넣기"

# ---- 설정값(원하시면 바꿔 쓰세요) ---------------------------------
MIN_REL_VOL = 5.0          # 평균의 5배 이상
LOOKBACK_TRADING_DAYS = 63 # 약 3개월(거래일 기준)
TOP_N = 20                 # 전송할 상위 종목 개수
INCLUDE_SP400_SP600 = False  # True로 바꾸면 S&P400/600도 포함(느려질 수 있음)
MIN_PRICE = 1.0            # 1달러 미만 종목 제외 (원하시면 조정)
MIN_MKT_CAP = 0            # 예: 3e8 지정하면 시총 3억달러 미만 제외
# -------------------------------------------------------------------

def get_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tbl = pd.read_html(url)[0]
    return tbl["Symbol"].tolist()

def get_nasdaq100():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    # 표가 여러 개라 가장 큰 테이블을 선택
    tables = pd.read_html(url)
    df = max(tables, key=lambda t: t.shape[0])
    # 심볼 컬럼명 케이스가 종종 바뀌므로 유연 처리
    sym_col = [c for c in df.columns if "Ticker" in str(c) or "Symbol" in str(c)][0]
    return df[sym_col].astype(str).tolist()

def get_sp400():
    url = "https://en.wikipedia.org/wiki/S%26P_400"
    tbls = pd.read_html(url)
    df = max(tbls, key=lambda t: t.shape[0])
    sym_col = [c for c in df.columns if "Symbol" in str(c)][0]
    return df[sym_col].astype(str).tolist()

def get_sp600():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
    tbl = pd.read_html(url)[0]
    return tbl["Symbol"].astype(str).tolist()

def normalize_for_yf(ticker: str) -> str:
    # BRK.B -> BRK-B 처럼 yfinance 표기 보정
    return ticker.replace(".", "-").strip()

def fetch_basics(tickers):
    """시가총액, 현재가 등 요약치 (yfinance fast-info)"""
    out = {}
    # yfinance Ticker.info는 느릴 수 있어 fast_info/ get_info 대체
    for t in tickers:
        try:
            tk = yf.Ticker(t)
            fi = getattr(tk, "fast_info", None)
            if fi:
                mktcap = getattr(fi, "market_cap", None)
                last = getattr(fi, "last_price", None)
            else:
                mktcap = None
                last = None
            out[t] = {"mktcap": mktcap, "last": last}
            time.sleep(0.02)
        except Exception:
            out[t] = {"mktcap": None, "last": None}
    return out

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    # 1) 유니버스 구성
    base = set(get_sp500() + get_nasdaq100())
    if INCLUDE_SP400_SP600:
        base |= set(get_sp400() + get_sp600())
    tickers = [normalize_for_yf(t) for t in sorted(base)]
    
    # 2) 요약치(시총/현재가) 수집 후 필터(선택)
    basics = fetch_basics(tickers)
    tickers = [
        t for t in tickers
        if (basics[t]["last"] is None or basics[t]["last"] >= MIN_PRICE)
        and (basics[t]["mktcap"] is None or basics[t]["mktcap"] >= MIN_MKT_CAP)
    ]

    # 3) 가격/거래량 데이터 수집
    # yfinance는 많은 티커를 한 번에 받을 때 실패할 수도 있어 80개씩 청크
    results = []
    for batch in chunked(tickers, 80):
        try:
            data = yf.download(
                tickers=batch,
                period="200d", interval="1d", auto_adjust=False, threads=True, progress=False
            )
            # 멀티인덱스/단일 컬럼 상황 모두 처리
            for t in batch:
                try:
                    if isinstance(data.columns, pd.MultiIndex):
                        if t not in data.columns.levels[1]:
                            continue
                        df = data.xs(t, level=1, axis=1).copy()
                    else:
                        # 단일티커일 때
                        df = data.copy()

                    df = df.dropna(subset=["Close", "Volume"])
                    if len(df) < LOOKBACK_TRADING_DAYS + 2:
                        continue

                    recent = df.tail(LOOKBACK_TRADING_DAYS)
                    avg_vol = recent["Volume"].mean()
                    today = df.iloc[-1]
                    prev = df.iloc[-2]

                    if avg_vol is None or avg_vol == 0:
                        continue

                    rel_vol = today["Volume"] / avg_vol
                    if rel_vol >= MIN_REL_VOL:
                        # 52주 고저
                        year = df.tail(252)
                        hi_52w = year["Close"].max()
                        lo_52w = year["Close"].min()
                        px = today["Close"]
                        chg = (px / prev["Close"] - 1.0) * 100.0
                        dist_hi = (px / hi_52w - 1.0) * 100.0 if hi_52w and hi_52w > 0 else None

                        results.append({
                            "Ticker": t,
                            "Price": round(px, 4),
                            "Chg%": round(chg, 2),
                            "Vol_Today": int(today["Volume"]),
                            "Vol_Avg90": int(avg_vol),
                            "RelVol": round(rel_vol, 2),
                            "52w_Hi": round(hi_52w, 4) if hi_52w else None,
                            "52w_Lo": round(lo_52w, 4) if lo_52w else None,
                            "Dist_to_52w_Hi%": round(dist_hi, 2) if dist_hi is not None else None,
                            "MktCap": basics[t]["mktcap"]
                        })
                except Exception:
                    continue
        except Exception:
            continue

    if not results:
        text = "오늘(최근 3개월 평균 대비 5배↑) 조건을 만족한 종목이 없습니다."
    else:
        df = pd.DataFrame(results).sort_values("RelVol", ascending=False).head(TOP_N)
        lines = []
        lines.append("📈 *거래량 급증(≥5×, 3개월 평균 대비) 스크리너*")
        for _, r in df.iterrows():
            line = (
                f"- {r['Ticker']}: ${r['Price']:.2f} ({r['Chg%']:+.2f}%) | "
                f"Vol {r['Vol_Today']:,} (x{r['RelVol']}) | "
                f"Avg90 {r['Vol_Avg90']:,} | 52w {r['52w_Lo']}-{r['52w_Hi']} | "
                f"to High {r['Dist_to_52w_Hi%']:+.1f}%"
            )
            lines.append(line)
        text = "\n".join(lines)

    # 4) 텔레그램 전송
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(url, data={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }, timeout=30)
    if not resp.ok:
        print("Telegram send failed:", resp.text)
    else:
        print("Sent to Telegram OK.")

if __name__ == "__main__":
    main()

    