#!/usr/bin/env bash
# Sample CPU utilization and pagefault rates into a CSV.
# Usage: ./pf_cpu_monitor.sh [interval_seconds] [output_csv]
# Example: ./pf_cpu_monitor.sh 0.1 pf_cpu_metrics.csv

set -euo pipefail

INTERVAL="${1:-0.1}"                   # default 100 ms
OUT="${2:-pf_cpu_metrics.csv}"

# GNU date supports %3N (milliseconds). Fallback if unavailable.
ts_ms() {
  if date +%s%3N >/dev/null 2>&1; then
    date +%s%3N
  else
    # seconds * 1000 (less precise fallback)
    echo $(( $(date +%s) * 1000 ))
  fi
}

# Read /proc/stat "cpu" line and return TOTAL and IDLE_ALL
read_cpu() {
  # Fields: user nice system idle iowait irq softirq steal guest guest_nice
  read -r _ u n s i io irq sirq st _ _ < /proc/stat
  local idle_all=$(( i + io ))
  local total=$(( u + n + s + i + io + irq + sirq + st ))
  echo "$total $idle_all"
}

# Read cumulative page fault counters (minor & major) from /proc/vmstat
read_vm() {
  # shellcheck disable=SC2154
  local pgfault pgmajfault
  pgfault=$(grep -m1 '^pgfault ' /proc/vmstat | awk '{print $2}')
  pgmajfault=$(grep -m1 '^pgmajfault ' /proc/vmstat | awk '{print $2}')
  echo "${pgfault:-0} ${pgmajfault:-0}"
}

# Header
echo "timestamp_ms,cpu_util_pct,pgfaults_per_sec,pgmajfaults_per_sec" > "$OUT"

# Initial snapshots
read -r prev_total prev_idle < <(read_cpu)
read -r prev_pgf prev_pgmaj < <(read_vm)

# Clean exit
trap 'echo "Stopped. CSV at: ${OUT}" >&2; exit 0' INT TERM

while :; do
  sleep "$INTERVAL"

  # Current snapshots
  read -r cur_total cur_idle < <(read_cpu)
  read -r cur_pgf cur_pgmaj < <(read_vm)

  # Deltas
  dt_total=$(( cur_total - prev_total ))
  dt_idle=$(( cur_idle - prev_idle ))
  d_pgf=$(( cur_pgf - prev_pgf ))
  d_pgmaj=$(( cur_pgmaj - prev_pgmaj ))

  # Rates & CPU %
  # Avoid division by zero if dt_total=0 (very rare on tiny intervals)
  cpu_util_pct="0.0"
  if [ "$dt_total" -gt 0 ]; then
    # CPU% = 100 * (1 - idle_delta / total_delta)
    cpu_util_pct=$(awk -v idle="$dt_idle" -v total="$dt_total" 'BEGIN { printf "%.2f", 100*(1 - idle/total) }')
  fi

  # Convert event counts to per-second rates given the interval
  pgf_ps=$(awk -v d="$d_pgf" -v s="$INTERVAL" 'BEGIN { if(s>0) printf "%.2f", d/s; else print "0.00" }')
  pgmaj_ps=$(awk -v d="$d_pgmaj" -v s="$INTERVAL" 'BEGIN { if(s>0) printf "%.2f", d/s; else print "0.00" }')

  # Timestamp & write row
  echo "$(ts_ms),${cpu_util_pct},${pgf_ps},${pgmaj_ps}" >> "$OUT"

  # Roll snapshots
  prev_total=$cur_total
  prev_idle=$cur_idle
  prev_pgf=$cur_pgf
  prev_pgmaj=$cur_pgmaj
done
