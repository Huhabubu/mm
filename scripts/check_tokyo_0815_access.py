"""Check only the explicitly allowed 2026-08-15 Tardis calibration date.

No other candidate dates, no implicit substitution with 09/10/11. Download
only the first bytes, never historical raw tape; do not expose API keys.
"""
import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

DAY="2026-08-15"
URL="https://datasets.tardis.dev/v1/okex-swap/book_ticker/2026/08/15/SOL-USDT-SWAP.csv.gz"


def check(opener=urllib.request.urlopen,token=None):
    headers={"User-Agent":"tokyo-calibration-availability/1.0","Range":"bytes=0-255"}
    if token:
        headers["Authorization"]="Bearer "+token
    req=urllib.request.Request(URL,headers=headers)
    try:
        with opener(req,timeout=30) as response:
            data=response.read(256)
            status=getattr(response,"status",200)
            return {"day":DAY,"http_status":int(status),
                    "result":"may_be_accessible" if data and status in (200,206) else "empty_response",
                    "authenticated":bool(token),
                    "sample_bytes_read":len(data),
                    "cannot_infer_full_tape_coverage_from_preflight":True}
    except urllib.error.HTTPError as e:
        return {"day":DAY,"http_status":int(e.code),
                "result":"requires_authorized_Tardis_historical_access" if e.code in (401,403) else "HTTP_error",
                "authenticated":bool(token),"sample_bytes_read":0}
    except (urllib.error.URLError,TimeoutError) as e:
        return {"day":DAY,"http_status":None,"result":"network_check_failed",
                "authenticated":bool(token),"error_type":type(e).__name__,
                "sample_bytes_read":0}


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--out",required=True)
    args=p.parse_args()
    token=os.environ.get("TARDIS_API_KEY")
    d=check(token=token)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"tokyo_calibration_access.json").write_text(
        json.dumps(d,ensure_ascii=False,indent=2))
    (out/"tokyo_calibration_access.md").write_text(
        "# 2026-08-15 东京期独立校准数据准入\n\n"
        f"日期：{DAY}；HTTP状态：{d['http_status']}；结论：{d['result']}。\n\n"
        "无API Key时Tardis官方公开历史仅每月1日；本探测限08-15，"
        "读取最多256字节。若受阻，禁止利用9/10/11拟合Equal-Volume控制。"
        "不把其它未经探索日期代替08-15。\n"
    )
    print("TOKYO_CALIBRATION",d["day"],d["http_status"],d["result"])


if __name__=="__main__":
    main()
