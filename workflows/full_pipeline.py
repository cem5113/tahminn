#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
full_pipeline.py  (Streamlit + CLI uyumlu)

ÜRETİLEN DOSYALAR:
- <outdir>/yarin.csv
- <outdir>/week.csv

SÜTUNLAR:
date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot
"""
from __future__ import annotations
import os, io, sys, argparse, json
from datetime import datetime, timedelta, date
from urllib.parse import quote

# ---- Ortak bağımlılıklar (pip install YOK; Streamlit'te zaten kurulu varsayıyoruz) ----
import requests
import pandas as pd
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter, Retry

BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

TR_DAYS = {0:"Pazartesi",1:"Salı",2:"Çarşamba",3:"Perşembe",4:"Cuma",5:"Cumartesi",6:"Pazar"}

def _session_with_retry(total=4, backoff=0.5, timeout=30):
    sess = requests.Session()
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.request_timeout = timeout
    return sess

def fetch_days_csv(api_key: str, location: str, start: date, end: date,
                   unit: str, hot_threshold: float, sess: requests.Session) -> pd.DataFrame:
    url = (
        f"{BASE}/{quote(location)}/{start.isoformat()}/{end.isoformat()}"
        f"?unitGroup={unit}&include=days"
        f"&elements=datetime,temp,tempmin,tempmax,precip"
        f"&contentType=csv&key={api_key}"
    )
    r = sess.get(url, timeout=getattr(sess, "request_timeout", 30))
    try:
        r.raise_for_status()
    except requests.HTTPError:
        # görsel debug için kısa gövde
        preview = r.text[:400] if r.text else ""
        raise RuntimeError(f"HTTP {r.status_code} — {preview}")

    df = pd.read_csv(io.StringIO(r.text))
    need = {"datetime","temp","tempmin","tempmax","precip"}
    miss = need - set(df.columns)
    if miss:
        raise RuntimeError(f"Beklenen kolon(lar) yok: {miss} | Gelenler: {list(df.columns)}")

    df = df.rename(columns={
        "datetime":"date",
        "temp":"tavg",
        "tempmin":"tmin",
        "tempmax":"tmax",
        "precip":"prcp",
    })

    dd = pd.to_datetime(df["date"], errors="coerce")
    df["day"] = dd.dt.weekday.map(TR_DAYS)

    df["temp_range"] = df["tmax"] - df["tmin"]
    df["is_rainy"]   = (df["prcp"] > 0).astype(int)
    df["is_hot"]     = (df["tmax"] >= float(hot_threshold)).astype(int)

    return df[["date","tavg","tmin","tmax","prcp","temp_range","day","is_rainy","is_hot"]]

def write_placeholder(outdir: str):
    os.makedirs(outdir, exist_ok=True)
    header = "date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot\n"
    open(os.path.join(outdir, "yarin.csv"), "w", encoding="utf-8").write(header)
    open(os.path.join(outdir, "week.csv"), "w", encoding="utf-8").write(header)

def compute_dates(tz_name: str):
    tz = ZoneInfo(tz_name)
    today_local = datetime.now(tz).date()
    tomorrow = today_local + timedelta(days=1)
    week_end = tomorrow + timedelta(days=6)
    return tomorrow, week_end

# =========================
# STREAMLIT MODU
# =========================
def _running_in_streamlit() -> bool:
    # streamlit runtime var mı?
    return bool(os.environ.get("STREAMLIT_SERVER_ENABLED") or os.environ.get("STREAMLIT_RUNTIME") or "streamlit" in sys.argv[0])

def run_streamlit():
    import streamlit as st

    st.set_page_config(page_title="Weather CSV Builder", page_icon="⛅", layout="centered")

    # Secrets > Env fallback
    api_key = None
    if "VISUAL_CROSSING_API_KEY" in st.secrets:
        api_key = st.secrets["VISUAL_CROSSING_API_KEY"]
    if not api_key:
        api_key = os.getenv("VISUAL_CROSSING_API_KEY", "")

    st.title("⛅ Weather CSV Builder (Visual Crossing)")

    with st.sidebar:
        st.markdown("### Ayarlar")
        outdir = st.text_input("Çıktı klasörü", os.getenv("CRIME_DATA_DIR", "crime_prediction_data"))
        location = st.text_input("Konum", os.getenv("WX_LOCATION", "San Francisco,CA"))
        unit = st.selectbox("Birim", ["metric","us","uk","base"], index=["metric","us","uk","base"].index(os.getenv("WX_UNIT","metric")))
        hot_thr = st.number_input("is_hot eşiği (°C)", min_value=-50.0, max_value=60.0, value=float(os.getenv("HOT_THRESHOLD_C","30")), step=0.5)
        tz_name = st.text_input("TZ", os.getenv("WX_TZ","America/Los_Angeles"))

        with st.expander("Debug", expanded=False):
            st.code(json.dumps({
                "CRIME_DATA_DIR": outdir,
                "WX_LOCATION": location,
                "WX_UNIT": unit,
                "HOT_THRESHOLD_C": hot_thr,
                "WX_TZ": tz_name,
                "HasAPIKey": bool(api_key),
            }, ensure_ascii=False, indent=2))

    st.write("Bu araç **yarın** ve **7 günlük** (yarın dahil) günlük hava verisini CSV olarak üretir.")
    tomorrow, week_end = compute_dates(tz_name)
    st.info(f"Aralıklar: **yarın = {tomorrow.isoformat()}**, **hafta = {tomorrow.isoformat()} → {week_end.isoformat()}**")

    @st.cache_data(show_spinner=True, ttl=60*15)
    def _run_fetch_cached(_api_key, _location, _unit, _hot_thr, _tz_name):
        sess = _session_with_retry()
        tmrw, wend = compute_dates(_tz_name)
        df_t = fetch_days_csv(_api_key, _location, tmrw, tmrw, _unit, _hot_thr, sess)
        df_w = fetch_days_csv(_api_key, _location, tmrw, wend, _unit, _hot_thr, sess)
        return df_t, df_w

    col1, col2 = st.columns(2)
    with col1:
        do_placeholder = st.button("🔧 Placeholder yaz (secret yoksa)", use_container_width=True)
    with col2:
        do_fetch = st.button("🚀 Veriyi çek ve CSV yaz", type="primary", use_container_width=True)

    if do_placeholder:
        write_placeholder(outdir)
        st.success(f"Placeholder yazıldı → `{outdir}/yarin.csv`, `{outdir}/week.csv`")

    if do_fetch:
        if not api_key:
            st.error("VISUAL_CROSSING_API_KEY bulunamadı. `st.secrets` veya `ENV` üzerinden sağlayın.")
            st.stop()

        try:
            with st.spinner("Veri çekiliyor…"):
                df_t, df_w = _run_fetch_cached(api_key, location, unit, hot_thr, tz_name)
            os.makedirs(outdir, exist_ok=True)
            p1 = os.path.join(outdir, "yarin.csv")
            p2 = os.path.join(outdir, "week.csv")
            df_t.to_csv(p1, index=False, encoding="utf-8")
            df_w.to_csv(p2, index=False, encoding="utf-8")

            st.success(f"CSV’ler yazıldı: `{p1}` ve `{p2}`")
            st.divider()
            st.subheader("Yarın (önizleme)")
            st.dataframe(df_t.head(10), use_container_width=True)
            st.download_button("⬇️ yarin.csv indir", df_t.to_csv(index=False).encode("utf-8"),
                               file_name="yarin.csv", mime="text/csv")
            st.subheader("Hafta (önizleme)")
            st.dataframe(df_w.head(10), use_container_width=True)
            st.download_button("⬇️ week.csv indir", df_w.to_csv(index=False).encode("utf-8"),
                               file_name="week.csv", mime="text/csv")
        except Exception as e:
            st.exception(e)

# =========================
# CLI MODU
# =========================
def parse_args():
    env_outdir  = os.getenv("CRIME_DATA_DIR", "crime_prediction_data")
    env_loc     = os.getenv("WX_LOCATION", "San Francisco,CA")
    env_unit    = os.getenv("WX_UNIT", "metric")
    env_hot     = os.getenv("HOT_THRESHOLD_C", "30")
    env_tz      = os.getenv("WX_TZ", "America/Los_Angeles")
    p = argparse.ArgumentParser(description="Fetch Visual Crossing daily CSVs (yarın & 7 gün).")
    p.add_argument("--outdir", default=env_outdir)
    p.add_argument("--location", default=env_loc)
    p.add_argument("--unit", default=env_unit, choices=["metric","us","uk","base"])
    p.add_argument("--hot-threshold", default=env_hot, type=float)
    p.add_argument("--tz", default=env_tz)
    p.add_argument("--no-placeholder", action="store_true", help="Secret yoksa fail ver; placeholder üretme.")
    return p.parse_args()

def run_cli():
    args = parse_args()
    api_key = os.getenv("VISUAL_CROSSING_API_KEY", "")
    if not api_key:
        if args.no_placeholder:
            print("Missing VISUAL_CROSSING_API_KEY", file=sys.stderr)
            sys.exit(1)
        os.makedirs(args.outdir, exist_ok=True)
        write_placeholder(args.outdir)
        print(f"⚠️ Secret yok; placeholder yazıldı → {args.outdir}/yarin.csv & week.csv")
        return 0

    sess = _session_with_retry()
    tmrw, wend = compute_dates(args.tz)
    os.makedirs(args.outdir, exist_ok=True)

    df_t = fetch_days_csv(api_key, args.location, tmrw, tmrw, args.unit, args.hot_threshold, sess)
    p1 = os.path.join(args.outdir, "yarin.csv"); df_t.to_csv(p1, index=False, encoding="utf-8")
    print("✓", p1)

    df_w = fetch_days_csv(api_key, args.location, tmrw, wend, args.unit, args.hot_threshold, sess)
    p2 = os.path.join(args.outdir, "week.csv"); df_w.to_csv(p2, index=False, encoding="utf-8")
    print("✓", p2)

    try:
        print("---- yarin.csv (head) ----")
        print(pd.read_csv(p1).head(5).to_string(index=False))
        print("---- week.csv (head) ----")
        print(pd.read_csv(p2).head(5).to_string(index=False))
    except Exception:
        pass
    return 0

# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    if _running_in_streamlit():
        run_streamlit()
    else:
        sys.exit(run_cli())
