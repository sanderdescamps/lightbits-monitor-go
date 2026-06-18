# lightbits-monitor

A lightweight storage-resilience and IOPS monitoring tool for Linux systems. It checks every subdirectory under a given mount directory, verifies that each one is reachable (read + write), and measures its IOPS using `/proc/diskstats` — all checks run in parallel.

## Features

- **Auto-discovers volumes** — scans subdirectories of `--mount-dir` every iteration; no fixed volume list required.
- **Parallel checks** — each volume is tested concurrently via goroutines.
- **Hang-safe I/O** — filesystem operations (`create`, `open`, `remove`) are wrapped with a configurable timeout so a stalled NVMe-oF or iSCSI mount cannot block the monitor.
- **IOPS measurement** — reads `/proc/diskstats` and computes read/write IOPS over the actual elapsed time between iterations (no fixed-interval rounding).
- **Outage timer** — in continuous mode, records when the first volume fails and prints the running duration until all volumes recover.
- **Mount-point filter** — by default only directories that appear in `/proc/mounts` are monitored; plain directories on the root filesystem are silently skipped.
- **Structured log file** — every iteration is appended to a timestamped log file.
- **Test description** — optional free-text labels (repeatable) written as a header block at the top of the log file.

## Requirements

- Linux (uses `/proc/mounts` and `/proc/diskstats`)
- Go 1.21+

## Build

```bash
go build -o lightbits-monitor .
```

## Usage

```
lightbits-monitor [flags]
```

### Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--mount-dir` | `-m` | `/mnt` | Directory whose subdirectories are the volumes to monitor |
| `--interval` | `-i` | `5` | Check interval in seconds (continuous mode only) |
| `--log-dir` | `-l` | `./logs` | Directory where log files are written |
| `--io-timeout` | | `200` | Per-operation I/O timeout in **milliseconds** |
| `--continuous` | `-c` | `false` | Run indefinitely, repeating every `--interval` seconds |
| `--all-dirs` | `-a` | `false` | Monitor all subdirectories, not just actual mount points |
| `--description` | `-d` | | Free-text label appended to the log header; **repeatable** |

### Examples

```bash
# Single check of all volumes mounted under /mnt
./lightbits-monitor -m /mnt

# Continuous monitoring every 10 seconds
./lightbits-monitor -m /mnt -c -i 10

# Include plain (non-mounted) directories as well
./lightbits-monitor -m /mnt -a

# Custom log directory and aggressive I/O timeout
./lightbits-monitor -m /mnt -c -i 5 -l /var/log/resilience --io-timeout 500

# Annotate the test with multiple description lines
./lightbits-monitor -m /mnt -c \
  -d "Test: rolling node reboot" \
  -d "Cluster: prod-cluster-01" \
  -d "Operator: sdescamps"
```

## Output

Each iteration prints a table followed by a summary:

```
=====================================
Monitoring Cycle: 2026-06-18 14:30:00
=====================================
Timestamp              Volume          Status          Read IOPS       Write IOPS
---------              ------          ------          ---------       ----------
2026-06-18 14:30:00    vol_1           ACCESSIBLE      1024            512
2026-06-18 14:30:00    vol_2           UNMOUNTED       N/A             N/A

                       TOTAL                           1024            512
Summary: 1/2 accessible, 1 failures
```

In continuous mode, outage events are also printed:

```
[2026-06-18 14:30:00] [WARNING] Outage started: 1/2 volumes unavailable
...
Outage in progress: started 2026-06-18 14:30:00, duration 45s
...
[2026-06-18 14:30:45] [INFO] Outage resolved after 45s
```

### Volume status values

| Status | Meaning |
|---|---|
| `ACCESSIBLE` | Directory exists, is mounted, and passed read + write tests |
| `UNMOUNTED` | Directory does not exist or is not listed in `/proc/mounts` |
| `WRITE_FAILED` | `create` timed out or returned an error |
| `READ_FAILED` | `open` timed out or returned an error |
| `CLEANUP_FAILED` | `remove` of the test file timed out or returned an error |

## Log files

Log files are written to `--log-dir` and named `lightbits-monitor-YYYYMMDD_HHMM.log`.

When `--description` lines are provided they are written as a header block before any measurement data:

```
================================================================================
[2026-06-18 14:30:00] Test description:
  Test: rolling node reboot
  Cluster: prod-cluster-01
================================================================================
```

Subsequent lines follow one of these formats:

```
[2026-06-18 14:30:00] vol_1 Status=ACCESSIBLE ReadIOPS=1024 WriteIOPS=512
[2026-06-18 14:30:00] [WARNING] Outage started: 1/2 volumes unavailable
[2026-06-18 14:30:45] [INFO] Outage resolved after 45s
```
