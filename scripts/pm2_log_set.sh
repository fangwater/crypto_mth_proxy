#!/usr/bin/env bash
set -euo pipefail

MAX_SIZE="100M"
LOG_RETAIN_DAYS=3
CRON_SCHEDULE="0 0 * * *"
ENABLE_COMPRESS=false

if ! command -v pm2 >/dev/null 2>&1; then
  echo "[ERROR] pm2 未安装，请先安装 pm2" >&2
  exit 1
fi

if pm2 describe pm2-logrotate >/dev/null 2>&1; then
  echo "[INFO] pm2-logrotate 已安装"
else
  echo "[INFO] 安装 pm2-logrotate"
  pm2 install pm2-logrotate
fi

echo "[INFO] 配置日志轮转"
pm2 set pm2-logrotate:max_size "$MAX_SIZE"
pm2 set pm2-logrotate:retain "$LOG_RETAIN_DAYS"
pm2 set pm2-logrotate:rotateInterval "$CRON_SCHEDULE"
pm2 set pm2-logrotate:compress "$ENABLE_COMPRESS"
pm2 set pm2-logrotate:rotateModule true
pm2 save

echo "[INFO] 完成日志轮转配置 (每天轮转, 保留 ${LOG_RETAIN_DAYS} 天)"
