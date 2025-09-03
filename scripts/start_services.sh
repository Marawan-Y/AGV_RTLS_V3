#!/usr/bin/env bash
# =========================================
# AGV RTLS Dashboard - Start All Services
# =========================================
set -euo pipefail

echo "========================================="
echo " AGV RTLS Dashboard - Start Services"
echo "========================================="

# ---------- Ask OS ----------
echo "Select your operating system:"
echo "  1) Linux / macOS"
echo "  2) Windows (Git Bash / MSYS)"
read -r -p "Enter 1 or 2: " OS_CHOICE
case "$OS_CHOICE" in
  1) OS="unix" ;;
  2) OS="windows" ;;
  *) echo "Invalid choice."; exit 1 ;;
esac

die() { echo "ERROR: $*" >&2; exit 1; }

# ---------- Resolve venv python/pip without 'activate' ----------
if [ "$OS" = "windows" ]; then
  VENV_PY="venv/Scripts/python.exe"
  VENV_PIP="venv/Scripts/pip.exe"
else
  VENV_PY="venv/bin/python"
  VENV_PIP="venv/bin/pip"
fi

[ -x "$VENV_PY" ] || die "Virtualenv not found at $VENV_PY. Run ./scripts/install.sh first."

# ---------- Optional: load .env into current shell (safe, handles spaces & quotes) ----------
if [ -f ".env" ]; then
  TMP_ENV="$(mktemp)"
  tr -d '\r' < .env > "$TMP_ENV"   # normalize CRLF -> LF for Git Bash/MSYS
  set -a
  # shellcheck disable=SC1090
  . "$TMP_ENV"
  set +a
  rm -f "$TMP_ENV"
fi

# ---------- Ensure logs dir ----------
mkdir -p logs

# ---------- Helpful status checks ----------
echo "Checking Mosquitto..."
if [ "$OS" = "unix" ]; then
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet mosquitto; then
      echo "  Mosquitto: active"
    else
      echo "  Mosquitto: not active (start if needed)"
    fi
  else
    echo "  Mosquitto: (no systemd check)."
  fi
else
  if command -v sc >/dev/null 2>&1; then
    if sc query mosquitto | grep -qi RUNNING; then
      echo "  Mosquitto: RUNNING"
    else
      echo "  Mosquitto: not running (start with: net start mosquitto)"
    fi
  fi
fi

# ---------- Start services ----------
echo "Starting AGV RTLS Dashboard Services..."

# Clean up background jobs on exit/Ctrl-C
cleanup() {
  echo
  echo "Shutting down background services..."
  for pid in ${CONSUMER_PID:-} ${API_PID:-}; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 1
  for pid in ${CONSUMER_PID:-} ${API_PID:-}; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  echo "Done."
}
trap cleanup INT TERM EXIT

# 1) MQTT consumer (background)
echo "Starting MQTT consumer..."
"$VENV_PY" -m src.ingestion.mqtt_consumer \
  >> logs/mqtt_consumer.out 2>> logs/mqtt_consumer.err &
CONSUMER_PID=$!
echo "  mqtt_consumer PID: $CONSUMER_PID"

# 2) FastAPI (Uvicorn, background)
echo "Starting API server..."
"$VENV_PY" -m uvicorn src.api.fastapi_app:app \
  --host 0.0.0.0 --port 8000 --reload \
  >> logs/api.out 2>> logs/api.err &
API_PID=$!
echo "  uvicorn PID: $API_PID"

# 3) Streamlit (foreground)
echo "Starting dashboard (Streamlit)..."
"$VENV_PY" -m streamlit run src/dashboard/app.py \
  --server.port 8501 --server.address 127.0.0.1 \
  2>> logs/streamlit.err

# When Streamlit exits, trap will run and clean up the others.
