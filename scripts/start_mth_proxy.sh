#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v pm2 >/dev/null 2>&1; then
  echo "[ERROR] pm2 未安装，请先安装 pm2" >&2
  exit 1
fi

NAMESPACE="$(basename "${BASE_DIR}")"
NAME="mth_proxy"
CONFIG_PATH="${BASE_DIR}/config.toml"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config.toml 不存在: ${CONFIG_PATH}" >&2
  exit 1
fi

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

echo "[INFO] Restarting ${NAME}"
pm2 delete "$NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG:-info}" pm2 start "$BIN_PATH" \
  --name "$NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR" \
  -- \
  --config "$CONFIG_PATH"

echo ""
echo "[INFO] Started: ${NAME}"
echo "Namespace: ${NAMESPACE}"
echo "Logs: pm2 logs --namespace ${NAMESPACE} ${NAME}"
echo "Status: pm2 status --namespace ${NAMESPACE}"
