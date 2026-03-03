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

if [[ ${#CONFIG_FILES[@]} -gt 0 ]]; then
  mapfile -t CONFIG_FILES < <(printf '%s\n' "${CONFIG_FILES[@]}" | sort)
fi

declare -a NAMES=()
for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
  NAMES+=( "$(sanitize_name "$CONFIG_PATH")" )
done

STOPPED=0
for NAME in "${NAMES[@]}"; do
  if pm2 delete "$NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[INFO] Stopped: ${NAME} (namespace: ${NAMESPACE})"
    STOPPED=$((STOPPED + 1))
  fi
done

# Also try legacy single-process name in case deployment switched modes.
if [[ ! " ${NAMES[*]} " =~ [[:space:]]mth_proxy[[:space:]] ]]; then
  if pm2 delete "mth_proxy" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[INFO] Stopped: mth_proxy (namespace: ${NAMESPACE})"
    STOPPED=$((STOPPED + 1))
  fi
fi

if [[ $STOPPED -eq 0 ]]; then
  echo "[INFO] No process stopped in namespace: ${NAMESPACE}"
fi
