# tg_smoke.py
import os, requests, sys

tok = os.getenv("TG_BOT_TOKEN")
cid = os.getenv("TG_CHAT_ID")
if not tok or not cid:
    sys.exit("환경변수 TG_BOT_TOKEN / TG_CHAT_ID 를 설정하세요.")

r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                  data={"chat_id": cid, "text": "텔레그램 연결 OK (KRX smoke test)"},
                  timeout=30)
print("status:", r.status_code)
print(r.text)
if not r.ok:
    sys.exit("전송 실패: 토큰/채팅ID/차단 여부 확인 필요")
