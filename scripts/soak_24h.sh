#!/usr/bin/env bash
set -euo pipefail

# Configuration (can be overridden via env vars)
LEADER_ADDR="${LEADER_ADDR:-127.0.0.1:8000}"
OPS="${OPS:-10000}"
BATCH="${BATCH:-100}"
DURATION_SECS="${DURATION_SECS:-86400}"  # default 24h
LOADGEN_BIN="${LOADGEN_BIN:-./raft_write_load}"
AUTO_LEADER="${AUTO_LEADER:-1}"

# Chaos configuration (can be disabled with CHAOS_ENABLED=0)
CHAOS_ENABLED="${CHAOS_ENABLED:-1}"
CHAOS_NODE_BIN="${CHAOS_NODE_BIN:-./target/release/chronos}"
CHAOS_DATA_DIR_BASE="${CHAOS_DATA_DIR_BASE:-./data}"
CHAOS_NODE1_SESSION="${CHAOS_NODE1_SESSION:-node1}"
CHAOS_NODE2_SESSION="${CHAOS_NODE2_SESSION:-node2}"
CHAOS_NODE3_SESSION="${CHAOS_NODE3_SESSION:-node3}"
CHAOS_RESTART_FOLLOWER_PERIOD="${CHAOS_RESTART_FOLLOWER_PERIOD:-5}"
CHAOS_RESTART_LEADER_PERIOD="${CHAOS_RESTART_LEADER_PERIOD:-15}"
CHAOS_KILL_TWO_ENABLED="${CHAOS_KILL_TWO_ENABLED:-0}"
CHAOS_KILL_TWO_PERIOD="${CHAOS_KILL_TWO_PERIOD:-0}"

detect_leader_addr() {
  # Probe HTTP admin ports 9000/9001/9002 to find current Leader.
  for p in 9000 9001 9002; do
    h=$(curl -s "http://127.0.0.1:${p}/health" || echo '{}')
    role=$(printf '%s\n' "$h" | sed -n 's/.*"role":"\([^" ]*\)".*/\1/p')
    if [ "$role" = "Leader" ]; then
      # gRPC port is HTTP port - 1000
      echo "127.0.0.1:$((p-1000))"
      return 0
    fi
  done

  # Fallback: keep previous LEADER_ADDR if no leader detected
  echo "$LEADER_ADDR"
}

END_TS=$(( $(date +%s) + DURATION_SECS ))

i=0
METRICS_FILE="soak_metrics_$(date +%Y%m%d_%H%M%S).csv"

# Header CSV
echo "loop,timestamp,success,errors,not_leader,other,leader_http_port,leader_term,leader_changes,term_changes" >"$METRICS_FILE"

prev_leader=""
prev_term=0
leader_changes=0
term_changes=0

while [ "$(date +%s)" -lt "$END_TS" ]; do
  ts="$(date --iso-8601=seconds)"
  echo "=== [$ts] loop $i ==="

  # Optionally auto-detect leader each loop.
  if [ "$AUTO_LEADER" = "1" ]; then
    detected="$(detect_leader_addr || true)"
    if [ -n "$detected" ]; then
      LEADER_ADDR="$detected"
    fi
  fi

  # 1) Run loadgen; never let it kill the script
  out=$("$LOADGEN_BIN" \
    --leader "$LEADER_ADDR" \
    --ops "$OPS" \
    --batch-size "$BATCH" 2>&1 || true)

  summary=$(printf '%s
' "$out" | grep '^Done:' | tail -n1 || true)
  echo "$summary"

  # Parse metrics from summary
  success=$(printf '%s
' "$summary" | sed -n 's/.*success=\([0-9]\+\).*/\1/p')
  errors=$(printf '%s
' "$summary" | sed -n 's/.*errors=\([0-9]\+\).*/\1/p')
  not_leader=$(printf '%s
' "$summary" | sed -n 's/.*not_leader_errors=\([0-9]\+\).*/\1/p')
  other=$(printf '%s
' "$summary" | sed -n 's/.*other_errors=\([0-9]\+\).*/\1/p')

  success=${success:-0}
  errors=${errors:-0}
  not_leader=${not_leader:-0}
  other=${other:-0}

  # 2) Sample /health from nodes (optional if not running locally)
  h0=$(curl -s http://127.0.0.1:9000/health || echo '{}')
  h1=$(curl -s http://127.0.0.1:9001/health || echo '{}')
  h2=$(curl -s http://127.0.0.1:9002/health || echo '{}')

  role0=$(printf '%s
' "$h0" | sed -n 's/.*"role":"\([^"]*\)".*/\1/p')
  term0=$(printf '%s
' "$h0" | sed -n 's/.*"term":\([0-9]\+\).*/\1/p'); term0=${term0:-0}
  role1=$(printf '%s
' "$h1" | sed -n 's/.*"role":"\([^"]*\)".*/\1/p')
  term1=$(printf '%s
' "$h1" | sed -n 's/.*"term":\([0-9]\+\).*/\1/p'); term1=${term1:-0}
  role2=$(printf '%s
' "$h2" | sed -n 's/.*"role":"\([^"]*\)".*/\1/p')
  term2=$(printf '%s
' "$h2" | sed -n 's/.*"term":\([0-9]\+\).*/\1/p'); term2=${term2:-0}

  leader_port=""
  leader_term=0

  if [ "$role0" = "Leader" ]; then
    leader_port="9000"
    leader_term=$term0
  elif [ "$role1" = "Leader" ]; then
    leader_port="9001"
    leader_term=$term1
  elif [ "$role2" = "Leader" ]; then
    leader_port="9002"
    leader_term=$term2
  fi

  # 3) Track leader / term changes
  if [ -n "$leader_port" ]; then
    if [ -n "$prev_leader" ] && [ "$leader_port" != "$prev_leader" ]; then
      leader_changes=$((leader_changes + 1))
    fi

    if (( leader_term > prev_term )); then
      term_changes=$((term_changes + leader_term - prev_term))
    fi

    prev_leader="$leader_port"
    prev_term=$leader_term
  fi

  # 4) Append one CSV line
  echo "$i,$ts,$success,$errors,$not_leader,$other,${leader_port:-},$leader_term,$leader_changes,$term_changes" >>"$METRICS_FILE"

  # 4b) Client-like adaptation: if this loop was entirely NotLeader, rediscover.
  if [ "$success" -eq 0 ] && [ "$not_leader" -eq "$OPS" ]; then
    echo "[CLIENT] loop $i: all ops NotLeader, rediscovering leader"
    if [ "$AUTO_LEADER" = "1" ]; then
      detected="$(detect_leader_addr || true)"
      if [ -n "$detected" ]; then
        LEADER_ADDR="$detected"
      fi
    fi
  fi

  # 5) Optional chaos actions: restart a follower and occasionally the leader.
  if [ "$CHAOS_ENABLED" = "1" ]; then
    leader_sess=""
    follower1_sess=""
    follower2_sess=""
    did_follower_restart=0

    case "$leader_port" in
      9000)
        leader_sess="$CHAOS_NODE1_SESSION"
        follower1_sess="$CHAOS_NODE2_SESSION"
        follower2_sess="$CHAOS_NODE3_SESSION"
        ;;
      9001)
        leader_sess="$CHAOS_NODE2_SESSION"
        follower1_sess="$CHAOS_NODE1_SESSION"
        follower2_sess="$CHAOS_NODE3_SESSION"
        ;;
      9002)
        leader_sess="$CHAOS_NODE3_SESSION"
        follower1_sess="$CHAOS_NODE1_SESSION"
        follower2_sess="$CHAOS_NODE2_SESSION"
        ;;
    esac

    # Restart one follower every CHAOS_RESTART_FOLLOWER_PERIOD loops (skip loop 0).
    if [ -n "$follower1_sess" ] \
       && [ "$CHAOS_RESTART_FOLLOWER_PERIOD" -gt 0 ] \
       && [ "$i" -gt 0 ] \
       && (( i % CHAOS_RESTART_FOLLOWER_PERIOD == 0 )); then
      echo "[CHAOS] loop $i: restarting follower session $follower1_sess"
      tmux kill-session -t "$follower1_sess" 2>/dev/null || true
      did_follower_restart=1

      case "$follower1_sess" in
        "$CHAOS_NODE1_SESSION")
          tmux new -d -s "$CHAOS_NODE1_SESSION" \
            "$CHAOS_NODE_BIN node --id node1 --data-dir $CHAOS_DATA_DIR_BASE/node1 --address 127.0.0.1:8000 --peers node2=127.0.0.1:8001,node3=127.0.0.1:8002";;
        "$CHAOS_NODE2_SESSION")
          tmux new -d -s "$CHAOS_NODE2_SESSION" \
            "$CHAOS_NODE_BIN node --id node2 --data-dir $CHAOS_DATA_DIR_BASE/node2 --address 127.0.0.1:8001 --peers node1=127.0.0.1:8000,node3=127.0.0.1:8002";;
        "$CHAOS_NODE3_SESSION")
          tmux new -d -s "$CHAOS_NODE3_SESSION" \
            "$CHAOS_NODE_BIN node --id node3 --data-dir $CHAOS_DATA_DIR_BASE/node3 --address 127.0.0.1:8002 --peers node1=127.0.0.1:8000,node2=127.0.0.1:8001";;
      esac
    fi

    # Optional kill-two mode: on specific loops, also allow leader restart even if a
    # follower was restarted in this same loop. This causes both the leader and one
    # follower to be killed and restarted together, simulating a short full-outage.
    if [ "$CHAOS_KILL_TWO_ENABLED" = "1" ] \
       && [ "$CHAOS_KILL_TWO_PERIOD" -gt 0 ] \
       && [ "$i" -gt 0 ] \
       && (( i % CHAOS_KILL_TWO_PERIOD == 0 )); then
      did_follower_restart=0
    fi

    # Restart the current leader every CHAOS_RESTART_LEADER_PERIOD loops (skip loop 0),
    # but never in the same loop where we already restarted a follower (unless
    # CHAOS_KILL_TWO_ENABLED override above has reset did_follower_restart).
    if [ -n "$leader_sess" ] \
       && [ "$CHAOS_RESTART_LEADER_PERIOD" -gt 0 ] \
       && [ "$i" -gt 0 ] \
       && [ "$did_follower_restart" -eq 0 ] \
       && (( i % CHAOS_RESTART_LEADER_PERIOD == 0 )); then
      echo "[CHAOS] loop $i: restarting leader session $leader_sess"
      tmux kill-session -t "$leader_sess" 2>/dev/null || true

      case "$leader_sess" in
        "$CHAOS_NODE1_SESSION")
          tmux new -d -s "$CHAOS_NODE1_SESSION" \
            "$CHAOS_NODE_BIN node --id node1 --data-dir $CHAOS_DATA_DIR_BASE/node1 --address 127.0.0.1:8000 --peers node2=127.0.0.1:8001,node3=127.0.0.1:8002";;
        "$CHAOS_NODE2_SESSION")
          tmux new -d -s "$CHAOS_NODE2_SESSION" \
            "$CHAOS_NODE_BIN node --id node2 --data-dir $CHAOS_DATA_DIR_BASE/node2 --address 127.0.0.1:8001 --peers node1=127.0.0.1:8000,node3=127.0.0.1:8002";;
        "$CHAOS_NODE3_SESSION")
          tmux new -d -s "$CHAOS_NODE3_SESSION" \
            "$CHAOS_NODE_BIN node --id node3 --data-dir $CHAOS_DATA_DIR_BASE/node3 --address 127.0.0.1:8002 --peers node1=127.0.0.1:8000,node2=127.0.0.1:8001";;
      esac
    fi
  fi

  i=$((i + 1))
  sleep 5
done

echo "# Soak finished at $(date), metrics in $METRICS_FILE"
