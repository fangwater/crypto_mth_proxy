#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v pm2 >/dev/null 2>&1; then
  echo "[ERROR] pm2 未安装，请先安装 pm2" >&2
  exit 1
fi

NAMESPACE="$(basename "${BASE_DIR}")"
CONFIG_DIR="${BASE_DIR}/configs"
LEGACY_CONFIG_PATH="${BASE_DIR}/config.toml"

BIN_CANDIDATES=(
  "${BASE_DIR}/crypto_mth_proxy"
  "${BASE_DIR}/target/release/crypto_mth_proxy"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] crypto_mth_proxy binary not found. Build first with: cargo build --release" >&2
  exit 1
fi

sanitize_name() {
  local file_path="$1"
  local base
  base="$(basename "$file_path" .toml)"
  base="${base//[^a-zA-Z0-9_-]/_}"
  echo "mth_proxy_${base}"
}

declare -a CONFIG_FILES=()
if [[ -d "$CONFIG_DIR" ]]; then
  shopt -s nullglob
  CONFIG_FILES=( "$CONFIG_DIR"/*.toml )
  shopt -u nullglob
fi

if [[ ${#CONFIG_FILES[@]} -eq 0 ]]; then
  if [[ -f "$LEGACY_CONFIG_PATH" ]]; then
    CONFIG_FILES=( "$LEGACY_CONFIG_PATH" )
  else
    echo "[ERROR] 未找到配置文件: ${CONFIG_DIR}/*.toml 或 ${LEGACY_CONFIG_PATH}" >&2
    exit 1
  fi
fi

mapfile -t CONFIG_FILES < <(printf '%s\n' "${CONFIG_FILES[@]}" | sort)

# If switching to multi-config mode, clean up the old single-process name.
if [[ ${#CONFIG_FILES[@]} -gt 1 ]]; then
  pm2 delete "mth_proxy" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi

STARTED=0
for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
  if [[ "$(basename "$CONFIG_PATH")" == "config.toml" ]]; then
    NAME="mth_proxy"
  else
    NAME="$(sanitize_name "$CONFIG_PATH")"
  fi

  echo "[INFO] Restarting ${NAME} with $(basename "$CONFIG_PATH")"
  pm2 delete "$NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

  RUST_LOG="${RUST_LOG:-info}" pm2 start "$BIN_PATH" \
    --name "$NAME" \
    --namespace "$NAMESPACE" \
    --cwd "$BASE_DIR" \
    -- \
    --config "$CONFIG_PATH"
  STARTED=$((STARTED + 1))
done

echo ""
echo "[INFO] Started ${STARTED} process(es)"
echo "Namespace: ${NAMESPACE}"
echo "Logs: pm2 logs --namespace ${NAMESPACE}"
echo "Status: pm2 status --namespace ${NAMESPACE}"
