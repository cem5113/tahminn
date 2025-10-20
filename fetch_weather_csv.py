#!/usr/bin/env python3
import os, io, sys, argparse, requests, pandas as pd
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import quote

BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

def fetch_days_csv(api_key: str, location: str, start: date, end: date,
                   unit: str, hot_threshold: float) -> pd.DataFrame:
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
    need = {"datetime","temp","tempmin","tempmax","precip"}
    miss = need - set(df.columns)
    if miss:
        raise RuntimeError(f"Beklenen kolon(lar) yok: {miss} | Gelenler: {list(df.columns)}")

    # yeniden adlandır
    df = df.rename(columns={
        "datetime":"date",
        "temp":"tavg",
        "tempmin":"tmin",
        "tempmax":"tmax",
        "precip":"prcp",
    })

    # gün adı (TR)
    tr_days = {0:"Pazartesi",1:"Salı",2:"Çarşamba",3:"Perşembe",4:"Cuma",5:"Cumartesi",6:"Pazar"}
    dd = pd.to_datetime(df["date"], errors="coerce")
    df["day"] = dd.dt.weekday.map(tr_days)

    # türetilmişler
    df["temp_range"] = df["tmax"] - df["tmin"]
    df["is_rainy"]   = (df["prcp"] > 0).astype(int)
    df["is_hot"]     = (df["tmax"] >= float(hot_threshold)).astype(int)

    return df[["date","tavg","tmin","tmax","prcp","temp_range","day","is_rainy","is_hot"]]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", required=True)
    p.add_argument("--location", default="San Francisco,CA")
    p.add_argument("--unit", default="metric", choices=["metric","us","uk","base"])
    p.add_argument("--hot-threshold", default="30")
    p.add_argument("--tz", default="America/Los_Angeles")
    args = p.parse_args()

    api_key = os.getenv("VISUAL_CROSSING_API_KEY", "")
    if not api_key:
        print("Missing VISUAL_CROSSING_API_KEY", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.outdir, exist_ok=True)

    # Yerel tarihler: Visual Crossing tarih aralığını lokalde yorumlar; SF için TZ veriyoruz
    tz = ZoneInfo(args.tz)
    today_local = datetime.now(tz).date()
    tomorrow    = today_local + timedelta(days=1)
    week_end    = tomorrow + timedelta(days=6)  # yarın dahil 7 gün

    # yarın
    df_t = fetch_days_csv(api_key, args.location, tomorrow, tomorrow, args.unit, float(args.hot_threshold))
    df_t.to_csv(os.path.join(args.outdir, "yarin.csv"), index=False, encoding="utf-8")
    print("✓", os.path.join(args.outdir, "yarin.csv"))

    # hafta
    df_w = fetch_days_csv(api_key, args.location, tomorrow, week_end, args.unit, float(args.hot_threshold))
    df_w.to_csv(os.path.join(args.outdir, "week.csv"), index=False, encoding="utf-8")
    print("✓", os.path.join(args.outdir, "week.csv"))

if __name__ == "__main__":
    main()
