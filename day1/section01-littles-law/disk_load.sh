#!/usr/bin/env bash
set -euo pipefail

# Disk I/O Load Generator - Demonstrates Little's Law with Disk Write Operations
#
# Little's Law: L = λ × R
#   L = aqu-sz (average queue depth - I/O requests in flight)
#   λ = w/s (write operations per second - arrival rate)
#   R = w_await (average write response time in milliseconds)
#
# This script generates disk write I/O load to demonstrate:
#   - Queue depth (aqu-sz) increases as disk becomes busy
#   - Response time (w_await) rises when utilization approaches 100%
#   - Relationship: aqu-sz ≈ w/s × (w_await/1000)
#
# Monitor with: iostat -x 1

FILE=${1:-/tmp/diskload.bin} # target file path
BS=${2:-4K}                   # block size (affects IOPS vs throughput)
DURATION=${3:-60}             # how long to run

# Calculate end time (seconds since epoch)
end_time=$(( $(date +%s) + DURATION ))

echo "Starting disk WRITE load for ${DURATION}s (bs=${BS})..."
echo "Monitor with: iostat -x 1"

while (( $(date +%s) < end_time )); do
    # dd: disk/data duplicator - copies data from input to output
    # - if=/dev/zero: read from /dev/zero (infinite stream of null bytes)
    # - of=FILE: write to file (using $$ for unique PID-based filename)
    # - bs=4K: block size (smaller = more IOPS, larger = more throughput)
    # - count=16384: number of blocks (16384 × 4K = 64MB per iteration)
    # - oflag=direct: bypass page cache, forces real disk I/O
    #     ↳ CRITICAL for accurate iostat measurement!
    #     ↳ Without this, writes go to memory cache, not disk
    # - status=none: suppress dd output
    #
    # Rationale: Each dd operation is an I/O "request" in Little's Law
    # The oflag=direct ensures we measure actual disk queueing, not cache
    dd if=/dev/zero of="${FILE}.$$" bs="$BS" count=16384 oflag=direct status=none 2>/dev/null || 
    dd if=/dev/zero of="${FILE}.$$" bs="$BS" count=16384 status=none 2>/dev/null
    
    # sync: flush file system buffers to disk
    # Ensures all writes are committed before we continue
    # Note: This serializes operations, limiting parallelism
    #       For higher load, remove sync or use multiple parallel processes
    sync
    
    # Clean up the test file after each iteration
    rm -f "${FILE}.$$"
done

echo "Disk load test completed."
