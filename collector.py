#!/usr/bin/env python3
"""ตัวดึงข้อมูลระดับน้ำ/ปริมาณน้ำเจ้าพระยา 5 สถานี จาก SWOC กรมชลประทาน

- ดึงค่าล่าสุดของแต่ละสถานี (endpoint ให้ค่าล่าสุดเพียงค่าเดียวต่อสถานี)
- ต่อท้ายลง history.json แบบไม่ซ้ำ (key = สถานี + เวลาตรวจวัด)
- เก็บย้อนหลัง 48 ชม. (นับจากค่าล่าสุดของแต่ละสถานี)

ใช้งาน:
    python3 collector.py                      # ดึงจริงแล้วอัปเดต history.json
    python3 collector.py --from-file x.json   # ทดสอบด้วยไฟล์ตัวอย่าง (รูปแบบเดียวกับ endpoint)

ตั้งให้รันทุกชั่วโมง เช่น cron:  5 * * * *  cd /path && python3 collector.py
ใช้เฉพาะ standard library
"""
import argparse
import json
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone

SOURCE_URL = "https://bigdata-swoc.rid.go.th/api/ma/pier/all/get_pier_data"
HISTORY_HOURS = 48

# เรียงจากเหนือน้ำไปท้ายน้ำ ชื่อ/จังหวัดเป็นค่าสำรอง (ถ้า API ส่งมาจะใช้ค่าจาก API)
STATIONS = [
    ("C.2", "นครสวรรค์", "นครสวรรค์"),
    ("C.13", "ท้ายเขื่อนเจ้าพระยา", "ชัยนาท"),
    ("C.3", "บ้านบางพุทรา", "สิงห์บุรี"),
    ("C.7A", "บ้านบางแก้ว", "อ่างทอง"),
    ("C.35", "บ้านป้อม", "พระนครศรีอยุธยา"),
]
CODES = [s[0] for s in STATIONS]


def fetch_records(from_file=None):
    if from_file:
        with open(from_file, encoding="utf-8") as f:
            payload = json.load(f)
    else:
        req = urllib.request.Request(
            SOURCE_URL, headers={"User-Agent": "water-monitor-prototype/0.1"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.load(resp)
    if not payload.get("success", True) or "data" not in payload:
        raise RuntimeError("รูปแบบข้อมูลต้นทางไม่ตรงที่คาด: ไม่มี success/data")
    return payload["data"]


def parse_time(s):
    # เช่น 2026-10-05T01:00:00.000Z -> aware UTC datetime
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def num(v):
    return None if v is None else round(float(v), 3)


def to_row(rec):
    """แปลงเป็นแถวข้อมูล โดยใช้ระดับอ้างอิง ม.รทก. (msl) ทั้งค่าน้ำและตลิ่งเสมอ"""
    return {
        "t": iso(parse_time(rec["hourly_time_utc"])),
        "q": num(rec.get("q_values")),
        "wl": num(rec.get("wl_values_msl")),
        "bank": num(rec.get("brae_level_msl")),
        "qmax": num(rec.get("q_max")),
        "wl_trend": rec.get("wl_trend"),
        "q_trend": rec.get("q_trend"),
    }


def load_history(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"stations": {}}


def merge(history, records, fetched_at):
    by_code = {r["station_code"]: r for r in records if r.get("station_code") in CODES}
    missing = [c for c in CODES if c not in by_code]
    added = {}
    for code, fb_name, fb_prov in STATIONS:
        st = history["stations"].setdefault(
            code, {"name": fb_name, "province": fb_prov, "river": "แม่น้ำเจ้าพระยา", "series": []}
        )
        rec = by_code.get(code)
        if rec is not None:
            st["name"] = rec.get("station_detail") or rec.get("name") or st["name"]
            st["province"] = rec.get("province_t") or st["province"]
            st["river"] = rec.get("river") or st["river"]
            row = to_row(rec)
            have = {r["t"] for r in st["series"]}
            if row["t"] not in have:
                st["series"].append(row)
                added[code] = row["t"]
        # เรียงเวลา และตัดเหลือ 48 ชม. นับจากค่าล่าสุดของสถานีนั้น
        st["series"].sort(key=lambda r: r["t"])
        if st["series"]:
            newest = parse_time(st["series"][-1]["t"])
            cutoff = iso(newest - timedelta(hours=HISTORY_HOURS))
            st["series"] = [r for r in st["series"] if r["t"] >= cutoff]
    history["source"] = SOURCE_URL
    history["fetched_at"] = iso(fetched_at)
    # คงลำดับสถานีเหนือน้ำ -> ท้ายน้ำ
    history["order"] = CODES
    return added, missing


def atomic_write(path, obj):
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="history.json")
    ap.add_argument("--from-file", help="ใช้ไฟล์ตัวอย่างแทนการเรียก endpoint")
    args = ap.parse_args()

    fetched_at = datetime.now(timezone.utc)
    try:
        records = fetch_records(args.from_file)
    except Exception as e:  # ต้นทางล่ม/รูปแบบเปลี่ยน: ไม่แตะไฟล์เดิม
        print(f"ERROR: ดึงข้อมูลไม่สำเร็จ: {e}", file=sys.stderr)
        return 1

    history = load_history(args.out)
    added, missing = merge(history, records, fetched_at)

    if len(missing) == len(CODES):
        print("ERROR: ไม่พบสถานีใดในข้อมูลต้นทาง ไม่เขียนไฟล์", file=sys.stderr)
        return 1

    atomic_write(args.out, history)
    for code, t in added.items():
        print(f"เพิ่ม {code} @ {t}")
    if not added:
        print("ไม่มีค่าใหม่ (ต้นทางยังไม่อัปเดตรอบถัดไป)")
    if missing:
        print(f"WARNING: ไม่พบสถานีในข้อมูลต้นทางรอบนี้: {', '.join(missing)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
