#!/usr/bin/env bash
# =========================================
# AGV RTLS Dashboard - Cross-Platform Installer
# =========================================
set -euo pipefail

echo "========================================="
echo " AGV RTLS Dashboard - Installation Script"
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

in_repo_root() {
  if [ -f "requirements.txt" ] || [ -f "pyproject.toml" ]; then
    [ -d "scripts" ] || die "Run from repo root (missing scripts/)."
    return 0
  fi
  die "Run from repo root (missing requirements.txt/pyproject.toml)."
}

# ---------- Python detection (robust; stores command as an ARRAY) ----------
# Will set PYEXE to an array, e.g. PYEXE=(py -3.10) or PYEXE=(python3)
find_python() {
  local candidates=()
  if [ "$OS" = "windows" ]; then
    candidates+=("py -3.12" "py -3.11" "py -3.10" "py -3" "python" "python3")
  else
    candidates+=("python3" "python")
  fi

  for c in "${candidates[@]}"; do
    # Split candidate to array safely
    # shellcheck disable=SC2206
    local arr=($c)
    if "${arr[@]}" -V >/dev/null 2>&1; then
      PYEXE=("${arr[@]}")
      return 0
    fi
  done
  return 1
}

py_major_minor() {
  "${PYEXE[@]}" - <<'PY'
import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
}

ensure_mysql_client() {
  if command -v mysql >/dev/null 2>&1; then
    echo "MySQL client found."
    return 0
  fi
  echo "MySQL client not found."
  if [ "$OS" = "unix" ]; then
    echo "Install (Debian/Ubuntu): sudo apt-get install mysql-client"
    echo "Install (macOS Homebrew): brew install mysql"
  else
    echo "Windows: Install MySQL and ensure mysql.exe is on PATH."
  fi
  return 1
}

read_secret() {
  local prompt="$1"
  if read -r -s -p "$prompt" REPLY < /dev/tty; then
    echo
    printf "%s" "$REPLY"
  else
    echo
    read -r -p "$prompt (visible): " REPLY
    printf "%s" "$REPLY"
  fi
}

# ---------- Pre-flight ----------
in_repo_root
if ! find_python; then
  die "Python 3.10+ required, but none found (tried py -3.12/-3.11/-3.10/-3, python3, python)."
fi

PYVER=$(py_major_minor)
echo "Using Python via: ${PYEXE[*]} (detected $PYVER)"
REQ="3.10"
awk -v A="$PYVER" -v B="$REQ" 'BEGIN{
  split(A,a,"."); split(B,b,".");
  if (a[1]>b[1] || (a[1]==b[1] && a[2]>=b[2])) exit 0; exit 1;
}' || die "Python $REQ+ is required."

# # ---------- Virtualenv ----------
# echo "Creating virtual environment (./venv)..."
# "${PYEXE[@]}" -m venv venv

# if [ "$OS" = "windows" ]; then
#   VENV_PY=(venv/Scripts/python.exe)
#   VENV_PIP=(venv/Scripts/pip.exe)
# else
#   VENV_PY=(venv/bin/python)
#   VENV_PIP=(venv/bin/pip)
# fi

# echo "Upgrading pip..."
# "${VENV_PY[@]}" -m pip install --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org

# if [ -f "requirements.txt" ]; then
#   echo "Installing Python dependencies from requirements.txt..."
#   "${VENV_PIP[@]}" install -r requirements.txt
# else
#   echo "No requirements.txt found. Skipping Python dependency install."
# fi

# # ---------- Assets & config skeleton ----------
# mkdir -p assets
# [ -f ".env" ] || { [ -f ".env.example" ] && cp .env.example .env || touch .env; }
# [ -f "assets/zones.geojson" ] || echo '{"type":"FeatureCollection","features":[]}' > assets/zones.geojson
# [ -f "assets/plant_map.png" ] || true

# ---------- Mosquitto ----------
echo "Checking Mosquitto MQTT broker..."
if [ "$OS" = "unix" ]; then
  if ! command -v mosquitto >/dev/null 2>&1; then
    echo "Mosquitto not found."
    if command -v apt-get >/dev/null 2>&1; then
      echo "Try: sudo apt-get update && sudo apt-get install -y mosquitto mosquitto-clients"
    elif command -v brew >/dev/null 2>&1; then
      echo "Try: brew install mosquitto && brew services start mosquitto"
    else
      echo "Install Mosquitto via your package manager."
    fi
  else
    if command -v systemctl >/dev/null 2>&1; then
      sudo systemctl enable mosquitto || true
      sudo systemctl start mosquitto || true
    elif command -v brew >/dev/null 2>&1; then
      brew services start mosquitto || true
    fi
  fi
else
  if command -v sc >/dev/null 2>&1; then
    if sc query mosquitto | grep -qi "RUNNING"; then
      echo "Mosquitto service is RUNNING."
    else
      echo "Mosquitto service not running/installed."
      echo "Windows: download from https://mosquitto.org/download/ then:  net start mosquitto"
    fi
  else
    echo "Could not query services (no 'sc')."
  fi
fi

# ---------- MySQL client & optional DB bootstrap ----------
# MYSQL_OK=0
# if ensure_mysql_client; then MYSQL_OK=1; fi

# if [ "$MYSQL_OK" -eq 1 ]; then
#   echo
#   echo "Database setup (optional)."
#   read -r -p "Run DB bootstrap now? [y/N]: " DO_DB
#   DO_DB=${DO_DB:-N}
#   if [[ "$DO_DB" =~ ^[Yy]$ ]]; then
#     read -r -p "MySQL host [localhost]: " DB_HOST; DB_HOST=${DB_HOST:-localhost}
#     read -r -p "MySQL port [3306]: " DB_PORT; DB_PORT=${DB_PORT:-3306}
#     read -r -p "Root username [root]: " DB_ROOT; DB_ROOT=${DB_ROOT:-root}
#     ROOT_PASS=$(read_secret "Root password: ")

#     read -r -p "App DB name [agv_rtls]: " APP_DB; APP_DB=${APP_DB:-agv_rtls}
#     read -r -p "App DB user [agv_user]: " APP_USER; APP_USER=${APP_USER:-agv_user}
#     APP_PASS=$(read_secret "App DB password: ")

#     echo "Creating schema and user..."
#     mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_ROOT" -p"$ROOT_PASS" <<SQL
# CREATE DATABASE IF NOT EXISTS \`$APP_DB\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
# CREATE USER IF NOT EXISTS '$APP_USER'@'%' IDENTIFIED BY '$APP_PASS';
# GRANT ALL PRIVILEGES ON \`$APP_DB\`.* TO '$APP_USER'@'%';
# FLUSH PRIVILEGES;
# SQL

#     if [ -f "database/schema.sql" ]; then
#       read -r -p "Load database/schema.sql into $APP_DB now? [y/N]: " LOAD_SCHEMA
#       LOAD_SCHEMA=${LOAD_SCHEMA:-N}
#       if [[ "$LOAD_SCHEMA" =~ ^[Yy]$ ]]; then
#         mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_ROOT" -p"$ROOT_PASS" "$APP_DB" < database/schema.sql
#         echo "Schema loaded."
#       fi
#     fi

#     if [ -f ".env" ]; then
#       echo "Updating .env (host/user/db)."
#       grep -q '^DB_HOST=' .env  && sed -i.bak "s|^DB_HOST=.*|DB_HOST=$DB_HOST|" .env || echo "DB_HOST=$DB_HOST" >> .env
#       grep -q '^DB_PORT=' .env  && sed -i.bak "s|^DB_PORT=.*|DB_PORT=$DB_PORT|" .env || echo "DB_PORT=$DB_PORT" >> .env
#       grep -q '^DB_NAME=' .env  && sed -i.bak "s|^DB_NAME=.*|DB_NAME=$APP_DB|"   .env || echo "DB_NAME=$APP_DB" >> .env
#       grep -q '^DB_USER=' .env  && sed -i.bak "s|^DB_USER=.*|DB_USER=$APP_USER|" .env || echo "DB_USER=$APP_USER" >> .env
#       read -r -p "Write DB password into .env? [y/N]: " WRITE_PW
#       WRITE_PW=${WRITE_PW:-N}
#       if [[ "$WRITE_PW" =~ ^[Yy]$ ]]; then
#         grep -q '^DB_PASSWORD=' .env && sed -i.bak "s|^DB_PASSWORD=.*|DB_PASSWORD=$APP_PASS|" .env || echo "DB_PASSWORD=$APP_PASS" >> .env
#       else
#         echo "Skipped writing DB password to .env."
#       fi
#     fi
#   fi
# fi

# ---------- Final output ----------
echo
echo "========================================="
echo " Installation complete!"
echo "========================================="
echo
echo "Next steps:"
echo "1) Review and edit your .env"
echo "2) Place plant_map.png in assets/"
echo "3) Configure zones in assets/zones.geojson"
echo "4) Calibrate: ${VENV_PY[*]} scripts/calibrate_transform.py"
echo "5) Start services:"
if [ "$OS" = "unix" ]; then
  echo "   - Linux/macOS: ./scripts/start_services.sh"
else
  echo "   - Windows PowerShell: .\\scripts\\start_services.ps1"
  echo "   - Ensure Mosquitto service is running:  net start mosquitto"
fi
echo
echo "Tip: Use the venv interpreter directly without activating:"
echo "     ${VENV_PY[*]} your_script.py"
