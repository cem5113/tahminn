#!/usr/bin/env bash
set -euo pipefail

# === Defaults (override with env) ===
: "${CRIME_DATA_DIR:=crime_prediction_data}"
: "${WX_LOCATION:=San Francisco,CA}"     # ör: "san francisco" da olur
: "${WX_UNIT:=metric}"                   # metric → °C & mm | us → °F & in
: "${HOT_THRESHOLD_C:=30}"               # is_hot eşiği
: "${WX_TZ:=America/Los_Angeles}"        # SF yereli

mkdir -p "${CRIME_DATA_DIR}"

# 1) Secret kontrolü (değeri log’a yazmayız)
if [ -z "${VISUAL_CROSSING_API_KEY:-}" ]; then
  echo "⚠️  VISUAL_CROSSING_API_KEY yok; placeholder CSV yazıyorum (başlıklar var, veri yok)."
  printf "date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot\n" > "${CRIME_DATA_DIR}/yarin.csv"
  printf "date,tavg,tmin,tmax,prcp,temp_range,day,is_rainy,is_hot\n" > "${CRIME_DATA_DIR}/week.csv"
  echo "→ ${CRIME_DATA_DIR}/yarin.csv & week.csv hazır (boş)."
  exit 0
fi

# 2) Python bağımlılıkları (tek komutla güvene al)
python - <<'PY'
import sys, subprocess
pkgs = ["requests","pandas","tzdata"]
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + pkgs)
print("✓ Python deps OK:", ", ".join(pkgs))
PY

# 3) fetch_weather_csv.py’yi çalıştır
python fetch_weather_csv.py \
  --outdir "${CRIME_DATA_DIR}" \
  --location "${WX_LOCATION}" \
  --unit "${WX_UNIT}" \
  --hot-threshold "${HOT_THRESHOLD_C}" \
  --tz "${WX_TZ}"

echo "---- yarin.csv (head) ----"; head -n 5 "${CRIME_DATA_DIR}/yarin.csv" || true
echo "---- week.csv  (head) ----"; head -n 5 "${CRIME_DATA_DIR}/week.csv"  || true
