#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="crypto_mth_proxy"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
Usage:
  deploy_mth_proxy.sh [--dir <path>]

Defaults:
  --dir $HOME/mth_pub/crypto_mth_proxy

Examples:
  bash scripts/deploy_mth_proxy.sh
  bash scripts/deploy_mth_proxy.sh --dir "$HOME/mth_pub/crypto_mth_proxy"
EOF
}

TARGET_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "[ERROR] --dir 需要一个路径" >&2
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

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/mth_pub/crypto_mth_proxy"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

mkdir -p "$TARGET_DIR/scripts"
for script in start_mth_proxy.sh stop_mth_proxy.sh pm2_log_set.sh; do
  if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
    rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

if [[ -d "$ROOT_DIR/configs" ]]; then
  mkdir -p "$TARGET_DIR/configs"
  rsync -a --delete "$ROOT_DIR/configs/" "$TARGET_DIR/configs/"
fi

if [[ -f "$ROOT_DIR/config.toml" ]]; then
  rsync -a "$ROOT_DIR/config.toml" "$TARGET_DIR/"
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动示例: cd $TARGET_DIR && ./scripts/start_mth_proxy.sh"
