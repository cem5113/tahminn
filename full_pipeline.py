#!/usr/bin/env python3
"""
full_pipeline.py
- Tek dosyada dev runner işlevi:
  * VISUAL_CROSSING_API_KEY var mı kontrol eder
  * Yoksa placeholder CSV'leri yazar (başlıklar)
  * Varsa Visual Crossing Timeline API'den yarın ve 7 günlük (yarından itibaren) veriyi çeker
  * Çıktılar: <OUTDIR>/yarin.csv ve <OUTDIR>/week.csv
  * Sütunlar: date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot

Kullanım ör:
  export VISUAL_CROSSING_API_KEY=YOUR_KEY
  python full_pipeline.py \
    --outdir crime_prediction_data \
    --location "San Francisco,CA" \
    --unit metric \
    --hot-threshold 30 \
    --tz America/Los_Angeles
"""
from __future__ import annotations
import os
import io
import sys
import argparse
import subprocess
from datetime import datetime, timedelta, date
from urllib.parse import quote

# === Basit bağımlılık yükleyici (gerektiğinde) ===
def _ensure_deps():
    needed = ["requests", "pandas", "tzdata"]
    missing = []
    for m in needed:
        try:
            __import__(m)
        except Exception:
            missing.append(m)
    if missing:
        print(f"ℹ️ Eksik paketler yükleniyor: {', '.join(missing)}", file=sys.stderr)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + missing)
        for m in missing:  # tekrar yükle
            __import__(m)
    # mypy: runtime import
    import requests  # type: ignore
    import pandas as pd  # type: ignore
    return requests, pd

BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

def _fetch_days_csv(api_key: str, location: str, start: date, end: date,
                    unit: str, hot_threshold: float, pd):
    import requests  # ensured by _ensure_deps
    url = (
        f"{BASE}/{quote(location)}/{start.isoformat()}/{end.isoformat()}"
        f"?unitGroup={unit}&include=days"
        f"&elements=datetime,temp,tempmin,tempmax,precip"
        f"&contentType=csv&key={api_key}"
    )
    r = requests.get(url, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print("HTTP status:", r.status_code, file=sys.stderr)
        print("Response preview:", r.text[:400], file=sys.stderr)
        raise

    df = pd.read_csv(io.StringIO(r.text))
    need = {"datetime", "temp", "tempmin", "tempmax", "precip"}
    miss = need - set(df.columns)
    if miss:
        raise RuntimeError(f"Beklenen kolon(lar) yok: {miss} | Gelenler: {list(df.columns)}")

    # yeniden adlandır
    df = df.rename(columns={
        "datetime": "date",
        "temp": "tavg",
        "tempmin": "tmin",
        "tempmax": "tmax",
        "precip": "prcp",
    })

    # gün adı (TR)
    tr_days = {0: "Pazartesi", 1: "Salı", 2: "Çarşamba", 3: "Perşembe", 4: "Cuma", 5: "Cumartesi", 6: "Pazar"}
    dd = pd.to_datetime(df["date"], errors="coerce")
    df["day"] = dd.dt.weekday.map(tr_days)

    # türetilmişler
    df["temp_range"] = df["tmax"] - df["tmin"]
    df["is_rainy"] = (df["prcp"] > 0).astype(int)
    df["is_hot"] = (df["tmax"] >= float(hot_threshold)).astype(int)

    return df[["date", "tavg", "tmin", "tmax", "prcp", "temp_range", "day", "is_rainy", "is_hot"]]

def _write_placeholder(outdir: str):
    os.makedirs(outdir, exist_ok=True)
    header = "date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot\n"
    with open(os.path.join(outdir, "yarin.csv"), "w", encoding="utf-8") as f:
        f.write(header)
    with open(os.path.join(outdir, "week.csv"), "w", encoding="utf-8") as f:
        f.write(header)
    print(f"⚠️ VISUAL_CROSSING_API_KEY yok; placeholder yazıldı → {outdir}/yarin.csv & week.csv")

def parse_args():
    # ENV fallback’ları
    env_outdir  = os.getenv("CRIME_DATA_DIR", "crime_prediction_data")
    env_loc     = os.getenv("WX_LOCATION", "San Francisco,CA")
    env_unit    = os.getenv("WX_UNIT", "metric")
    env_hot     = os.getenv("HOT_THRESHOLD_C", "30")
    env_tz      = os.getenv("WX_TZ", "America/Los_Angeles")

    p = argparse.ArgumentParser(description="Fetch Visual Crossing daily CSVs (yarın & 7 gün).")
    p.add_argument("--outdir", default=env_outdir, help=f"Çıktı klasörü (default: {env_outdir})")
    p.add_argument("--location", default=env_loc, help=f"Konum (default: {env_loc})")
    p.add_argument("--unit", default=env_unit, choices=["metric", "us", "uk", "base"],
                   help=f"Birim grubu (default: {env_unit})")
    p.add_argument("--hot-threshold", default=env_hot, type=float,
                   help=f"is_hot eşiği (°C, default: {env_hot})")
    p.add_argument("--tz", default=env_tz, help=f"Yerel TZ (default: {env_tz})")
    p.add_argument("--no-placeholder", action="store_true",
                   help="Secret yoksa fail ver; placeholder üretme.")
    return p.parse_args()

def main():
    args = parse_args()
    API_KEY = os.getenv("VISUAL_CROSSING_API_KEY", "")

    # Bağımlılıkları güvenceye al
    requests, pd = _ensure_deps()
    from zoneinfo import ZoneInfo  # stdlib

    # Secret yoksa davranış
    if not API_KEY:
        if args.no_placeholder:
            print("Missing VISUAL_CROSSING_API_KEY", file=sys.stderr)
            sys.exit(1)
        _write_placeholder(args.outdir)
        return 0

    # Tarihler: yerel timezone’a göre (VC yerel kabul eder)
    tz = ZoneInfo(args.tz)
    today_local = datetime.now(tz).date()
    tomorrow = today_local + timedelta(days=1)
    week_end = tomorrow + timedelta(days=6)  # yarın dahil 7 gün

    os.makedirs(args.outdir, exist_ok=True)

    # yarın
    df_t = _fetch_days_csv(API_KEY, args.location, tomorrow, tomorrow, args.unit, args.hot_threshold, pd)
    p_yarin = os.path.join(args.outdir, "yarin.csv")
    df_t.to_csv(p_yarin, index=False, encoding="utf-8")
    print("✓", p_yarin)

    # hafta
    df_w = _fetch_days_csv(API_KEY, args.location, tomorrow, week_end, args.unit, args.hot_threshold, pd)
    p_week = os.path.join(args.outdir, "week.csv")
    df_w.to_csv(p_week, index=False, encoding="utf-8")
    print("✓", p_week)

    # kısa önizleme
    try:
        print("---- yarin.csv (head) ----")
        print(pd.read_csv(p_yarin).head(5).to_string(index=False))
        print("---- week.csv (head) ----")
        print(pd.read_csv(p_week).head(5).to_string(index=False))
    except Exception:
        pass

    return 0

if __name__ == "__main__":
    sys.exit(main())
