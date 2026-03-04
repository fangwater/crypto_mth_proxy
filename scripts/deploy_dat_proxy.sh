#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="dat_proxy"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
DEFAULT_REMOTE_HOST="u171@10.61.10.32"
DEFAULT_REMOTE_DIR='$HOME/mth_pub/dat_proxy'

usage() {
  cat <<'EOF'
Usage:
  deploy_dat_proxy.sh [--host <user@host>] [--dir <remote_path>]

Defaults:
  --host u171@10.61.10.32
  --dir  $HOME/mth_pub/dat_proxy

Examples:
  bash scripts/deploy_dat_proxy.sh
  bash scripts/deploy_dat_proxy.sh --host "u171@10.61.10.32"
  bash scripts/deploy_dat_proxy.sh --dir "$HOME/mth_pub/dat_proxy"
EOF
}

REMOTE_HOST="$DEFAULT_REMOTE_HOST"
TARGET_DIR="$DEFAULT_REMOTE_DIR"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      REMOTE_HOST="${2:-}"
      if [[ -z "$REMOTE_HOST" ]]; then
        echo "[ERROR] --host 需要一个 user@host" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
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

if ! command -v ssh >/dev/null 2>&1; then
  echo "[ERROR] ssh 未安装，请先安装 openssh-client" >&2
  exit 1
fi

if ! command -v scp >/dev/null 2>&1; then
  echo "[ERROR] scp 未安装，请先安装 openssh-client" >&2
  exit 1
fi

echo "[INFO] 获取远端 HOME"
REMOTE_HOME="$(ssh "$REMOTE_HOST" 'printf %s "$HOME"')"
if [[ -z "$REMOTE_HOME" ]]; then
  echo "[ERROR] 无法获取远端 HOME: $REMOTE_HOST" >&2
  exit 1
fi

if [[ "$TARGET_DIR" == \$HOME/* ]]; then
  TARGET_DIR="${REMOTE_HOME}/${TARGET_DIR#\$HOME/}"
elif [[ "$TARGET_DIR" == "~/"* ]]; then
  TARGET_DIR="${REMOTE_HOME}/${TARGET_DIR#\~/}"
elif [[ "$TARGET_DIR" != /* ]]; then
  TARGET_DIR="${REMOTE_HOME}/${TARGET_DIR}"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 远端部署目标: ${REMOTE_HOST}:${TARGET_DIR}"
echo "[INFO] 准备远端目录"
ssh "$REMOTE_HOST" "mkdir -p '$TARGET_DIR/scripts' '$TARGET_DIR/configs'"

echo "[INFO] 上传二进制"
scp "$BIN_PATH" "$REMOTE_HOST:$TARGET_DIR/$BIN_NAME"
ssh "$REMOTE_HOST" "chmod +x '$TARGET_DIR/$BIN_NAME'"

echo "[INFO] 上传脚本"
for script in start_dat_proxy.sh stop_dat_proxy.sh pm2_log_set.sh test_ipc_sub.py; do
  if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
    scp "$ROOT_DIR/scripts/$script" "$REMOTE_HOST:$TARGET_DIR/scripts/$script"
    ssh "$REMOTE_HOST" "chmod +x '$TARGET_DIR/scripts/$script'"
  fi
done

if [[ ! -d "$ROOT_DIR/configs" ]]; then
  echo "[ERROR] 未找到配置目录: $ROOT_DIR/configs" >&2
  exit 1
fi

echo "[INFO] 同步配置文件"
ssh "$REMOTE_HOST" "rm -f '$TARGET_DIR/configs/'*.toml"
for cfg in "$ROOT_DIR"/configs/*.toml; do
  scp "$cfg" "$REMOTE_HOST:$TARGET_DIR/configs/"
done

echo "[INFO] $BIN_NAME 部署完成到 ${REMOTE_HOST}:${TARGET_DIR}"
echo "[INFO] 启动示例: ssh $REMOTE_HOST 'cd $TARGET_DIR && ./scripts/start_dat_proxy.sh'"
echo "[INFO] IPC 测试示例: ssh $REMOTE_HOST 'cd $TARGET_DIR && python3 scripts/test_ipc_sub.py --channel binance-futures-binance-futures --symbol BTCUSDT'"
