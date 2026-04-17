#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILTER=""

usage() {
  cat <<'EOF'
Usage:
  stop_dat_proxy.sh [--config <config_name_or_prefix>]

Examples:
  bash scripts/stop_dat_proxy.sh
  bash scripts/stop_dat_proxy.sh --config 04
  bash scripts/stop_dat_proxy.sh --config 04_ok_futures_ok_futures
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_FILTER="${2:-}"
      if [[ -z "$CONFIG_FILTER" ]]; then
        echo "[ERROR] --config 需要一个配置名或前缀" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

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
  echo "dat_proxy_${base}"
}

legacy_names() {
  local file_path="$1"
  local base
  base="$(basename "$file_path" .toml)"
  base="${base//[^a-zA-Z0-9_-]/_}"
  echo "dat_proxy_${base}"
  if [[ "$base" =~ ^([0-9]+)_ ]]; then
    echo "dat_proxy_${BASH_REMATCH[1]}"
  fi
}

matches_config_filter() {
  local file_path="$1"
  local filter="$2"
  local base
  base="$(basename "$file_path" .toml)"

  if [[ -z "$filter" ]]; then
    return 0
  fi

  filter="${filter%.toml}"
  if [[ "$base" == "$filter" ]]; then
    return 0
  fi
  if [[ "$base" =~ ^([0-9]+)_ ]] && [[ "${BASH_REMATCH[1]}" == "$filter" ]]; then
    return 0
  fi
  return 1
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

if [[ -n "$CONFIG_FILTER" ]]; then
  MATCHED=()
  for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
    if matches_config_filter "$CONFIG_PATH" "$CONFIG_FILTER"; then
      MATCHED+=( "$CONFIG_PATH" )
    fi
  done
  CONFIG_FILES=( "${MATCHED[@]}" )
fi

if [[ -n "$CONFIG_FILTER" ]] && [[ ${#CONFIG_FILES[@]} -eq 0 ]]; then
  echo "[ERROR] 未找到匹配配置: ${CONFIG_FILTER}" >&2
  exit 1
fi

declare -a NAMES=()
for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
  NAME="$(sanitize_name "$CONFIG_PATH")"
  NAMES+=( "$NAME" )
  mapfile -t LEGACY_NAMES < <(legacy_names "$CONFIG_PATH")
  for OLD_NAME in "${LEGACY_NAMES[@]}"; do
    if [[ "$OLD_NAME" != "$NAME" ]]; then
      NAMES+=( "$OLD_NAME" )
    fi
  done
done

STOPPED=0
for NAME in "${NAMES[@]}"; do
  if pm2 delete "$NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[INFO] Stopped: ${NAME} (namespace: ${NAMESPACE})"
    STOPPED=$((STOPPED + 1))
  fi
done

# Also try single-process name in case deployment switched modes.
if [[ -z "$CONFIG_FILTER" ]] && [[ ! " ${NAMES[*]} " =~ [[:space:]]dat_proxy[[:space:]] ]]; then
  if pm2 delete "dat_proxy" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[INFO] Stopped: dat_proxy (namespace: ${NAMESPACE})"
    STOPPED=$((STOPPED + 1))
  fi
fi

if [[ $STOPPED -eq 0 ]]; then
  echo "[INFO] No process stopped in namespace: ${NAMESPACE}"
fi
