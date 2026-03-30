#!/bin/bash

# Stop all BlockEmulator-related processes for both launch modes:
# 1) go run main.go ... (wrapper process)
# 2) /tmp/go-build.../exe/main ... (compiled child spawned by go run)
# 3) ./blockEmulator ... / blockEmulator_linux_Precompile ... (precompiled mode)

set +e

patterns=(
  "[g]o run main.go"
  "/tmp/go-build.*/exe/main"
  "[b]lockEmulator( |$)"
  "[b]lockEmulator_linux_Precompile( |$)"
)

killed=0
for pat in "${patterns[@]}"; do
  pids=$(pgrep -f "$pat")
  if [[ -n "$pids" ]]; then
    echo "$pids" | xargs -r kill -TERM
    killed=1
  fi
done

# Give processes time to exit gracefully.
sleep 1

# Force kill any leftovers.
for pat in "${patterns[@]}"; do
  pids=$(pgrep -f "$pat")
  if [[ -n "$pids" ]]; then
    echo "$pids" | xargs -r kill -KILL
    killed=1
  fi
done

if [[ "$killed" -eq 1 ]]; then
  echo "BlockEmulator processes stopped."
else
  echo "No BlockEmulator-related process found."
fi
