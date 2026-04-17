#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILTER=""

usage() {
  cat <<'EOF'
Usage:
  start_dat_proxy.sh [--config <config_name_or_prefix>]

Examples:
  bash scripts/start_dat_proxy.sh
  bash scripts/start_dat_proxy.sh --config 04
  bash scripts/start_dat_proxy.sh --config 04_ok_futures_ok_futures
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

BIN_CANDIDATES=(
  "${BASE_DIR}/dat_proxy"
  "${BASE_DIR}/target/release/dat_proxy"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] dat_proxy binary not found. Build first with: cargo build --release --bin dat_proxy" >&2
  exit 1
fi

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

if [[ ${#CONFIG_FILES[@]} -eq 0 ]]; then
  echo "[ERROR] 未找到配置文件: ${CONFIG_DIR}/*.toml" >&2
  exit 1
fi

mapfile -t CONFIG_FILES < <(printf '%s\n' "${CONFIG_FILES[@]}" | sort)

if [[ -n "$CONFIG_FILTER" ]]; then
  MATCHED=()
  for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
    if matches_config_filter "$CONFIG_PATH" "$CONFIG_FILTER"; then
      MATCHED+=( "$CONFIG_PATH" )
    fi
  done
  CONFIG_FILES=( "${MATCHED[@]}" )
fi

if [[ ${#CONFIG_FILES[@]} -eq 0 ]]; then
  echo "[ERROR] 未找到匹配配置: ${CONFIG_FILTER:-<all>}" >&2
  exit 1
fi

# If switching to multi-config mode, clean up old single-process names.
if [[ ${#CONFIG_FILES[@]} -gt 1 ]]; then
  pm2 delete "dat_proxy" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi

STARTED=0
for CONFIG_PATH in "${CONFIG_FILES[@]}"; do
  NAME="$(sanitize_name "$CONFIG_PATH")"
  mapfile -t LEGACY_NAMES < <(legacy_names "$CONFIG_PATH")

  echo "[INFO] Restarting ${NAME} with $(basename "$CONFIG_PATH")"
  for OLD_NAME in "${LEGACY_NAMES[@]}"; do
    pm2 delete "$OLD_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  done

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
