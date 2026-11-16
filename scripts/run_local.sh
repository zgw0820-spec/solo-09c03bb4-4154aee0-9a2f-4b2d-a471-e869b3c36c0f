#!/usr/bin/env bash
# Local end-to-end harness: starts two upstreams + gateway instances (no docker
# needed), runs the smoke test phases, kills gateways mid-request, and checks
# recovery + cross-instance drain semantics. Mirrors what docker-compose.yml
# wires up for containers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-/tmp/edge-gw-demo}"
SAVE="$WORK/phase1.json"
PY="${PYTHON:-python3}"
GW_PORT="${GW_PORT:-8000}"
GW2_PORT="${GW2_PORT:-8001}"
UP_A_PORT="${UP_A_PORT:-9001}"
UP_B_PORT="${UP_B_PORT:-9002}"

rm -rf "$WORK"
mkdir -p "$WORK"

PIDS=()
LAST_GW_PID=""
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

start_upstream() { # name port delay
  UPSTREAM_NAME="$1" DEFAULT_DELAY_MS="$3" \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/upstream" \
      --host 127.0.0.1 --port "$2" --log-level warning \
      >"$WORK/upstream-$1.log" 2>&1 &
  PIDS+=($!)
}

start_gateway() { # port
  GATEWAY_DB="$WORK/gateway.db" \
  UPSTREAMS="alpha=http://127.0.0.1:$UP_A_PORT;beta=http://127.0.0.1:$UP_B_PORT;dead=http://127.0.0.1:9" \
  INITIAL_TARGET=alpha \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/gateway" \
      --host 127.0.0.1 --port "$1" --log-level warning \
      >>"$WORK/gateway-$1.log" 2>&1 &
  LAST_GW_PID=$!
  PIDS+=($!)
}

wait_ready() { # url
  for _ in $(seq 1 100); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timeout waiting for $1" >&2
  return 1
}

wait_file() { # path
  for _ in $(seq 1 300); do [ -f "$1" ] && return 0; sleep 0.1; done
  echo "timeout waiting for $1" >&2
  return 1
}

echo "== 启动上游 alpha(:$UP_A_PORT, 延迟2000ms) / beta(:$UP_B_PORT, 延迟500ms) =="
start_upstream alpha "$UP_A_PORT" 2000
start_upstream beta  "$UP_B_PORT" 500
wait_ready "http://127.0.0.1:$UP_A_PORT/health"
wait_ready "http://127.0.0.1:$UP_B_PORT/health"

echo "== 启动网关 gw1(:$GW_PORT) =="
start_gateway "$GW_PORT"
wait_ready "http://127.0.0.1:$GW_PORT/api/health"

echo "== 阶段一：切换语义 =="
"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW_PORT" "$SAVE" phase1 &
SMOKE=$!

# wait until phase1 has armed the to-be-interrupted in-flight request
wait_file "$SAVE"

echo "== kill -9 gw1（模拟崩溃，beta 上仍有 1 条在途请求） =="
kill -9 "$LAST_GW_PID" 2>/dev/null || true
wait "$SMOKE"

echo "== 重启 gw1（同一 SQLite 数据文件） =="
start_gateway "$GW_PORT"
GW1_PID="$LAST_GW_PID"
wait_ready "http://127.0.0.1:$GW_PORT/api/health"

echo "== 阶段二：重启恢复（被杀实例的中断请求按租约回收） =="
"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW_PORT" "$SAVE" phase2

echo "== 阶段三：滚动重启（并行实例共享排空视图） =="
"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW_PORT" "$SAVE" phase3a
wait_file "$SAVE.ready3a"

echo "== gw1 慢请求在途期间启动第二实例 gw2(:$GW2_PORT, 共享同一 SQLite) =="
start_gateway "$GW2_PORT"
wait_ready "http://127.0.0.1:$GW2_PORT/api/health"

"$PY" "$ROOT/scripts/smoke_test.py" "http://127.0.0.1:$GW2_PORT" "$SAVE" phase3b \
  "http://127.0.0.1:$GW_PORT" &
SMOKE=$!
wait_file "$SAVE.ready3b"

echo "== kill -9 gw1（其 1 条在途请求被中断；gw2 的在途请求必须保留） =="
kill -9 "$GW1_PID" 2>/dev/null || true
wait "$SMOKE"

echo
echo "== 全部通过。日志目录: $WORK =="
