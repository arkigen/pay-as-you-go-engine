#!/bin/zsh
set -euo pipefail

AIRFLOW_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$AIRFLOW_DIR"

export AIRFLOW_HOME="$AIRFLOW_DIR"
export PATH="$AIRFLOW_DIR/.venv/bin:$PATH"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW__CORE__MP_START_METHOD=spawn
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=5
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=5
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=5
export AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=1
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__WORKERS=1
export NO_PROXY='*'
export no_proxy='*'

mkdir -p logs

pkill -f "$AIRFLOW_DIR/.venv/bin/airflow api-server" 2>/dev/null || true
pkill -f "$AIRFLOW_DIR/.venv/bin/airflow dag-processor" 2>/dev/null || true
pkill -f "$AIRFLOW_DIR/.venv/bin/airflow scheduler" 2>/dev/null || true
sleep 1

start_bg() {
  local name="$1"
  shift
  nohup "$@" >> "logs/${name}.log" 2>&1 < /dev/null &
}

start_bg api-server-ui "$AIRFLOW_DIR/.venv/bin/airflow" api-server --host 127.0.0.1 --port 8080 --workers 1
start_bg dag-processor-ui "$AIRFLOW_DIR/.venv/bin/airflow" dag-processor
start_bg scheduler-ui "$AIRFLOW_DIR/.venv/bin/airflow" scheduler

for i in {1..30}; do
  if curl -sf http://127.0.0.1:8080/login >/dev/null 2>&1; then
    echo "Airflow UI is ready at http://127.0.0.1:8080"
    echo "DAG page: http://127.0.0.1:8080/dags/golden_sync"
    echo "Parallelism: up to 5 golden_sync runs/tasks at once"
    exit 0
  fi
  sleep 1
done

echo "Airflow UI failed to start. Check logs/api-server-ui.log"
exit 1
