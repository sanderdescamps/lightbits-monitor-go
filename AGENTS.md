# Agent Summary — lightbits-monitor

This document describes the codebase structure, key design decisions, and conventions to help AI agents work effectively on this project.

## Project purpose

`lightbits-monitor` is a single-binary Go tool that periodically checks whether storage volumes mounted under a directory are reachable and measures their IOPS. It targets LightBits NVMe-oF storage clusters but works with any Linux block devices.

## File structure

```
lightbits-monitor-go/
  main.go      — entire implementation (single file, ~430 lines)
  go.mod       — module: lightbits-monitor; only external dep: github.com/spf13/cobra
  go.sum       — dependency checksums
  README.md    — user-facing documentation
  AGENTS.md    — this file
```

## Architecture

All code lives in `main.go`. Key types and their responsibilities:

| Type | Role |
|---|---|
| `volumeState` | Immutable snapshot: last diskstat counters + timestamp. Stored in memory per volume. |
| `volumeResult` | Output of one volume check: name, status string, read/write IOPS, timestamp. |
| `monitor` | Owns all shared state (volume states map, outage timer). Drives iteration logic. |

### Data flow per iteration

1. `runIteration` calls `listVolumes` → returns sorted subdirectory names from `mountDir`
2. One goroutine per volume calls `checkAccessibility` + `calculateIOPS` in parallel
3. Results are collected into a pre-allocated slice (each goroutine writes to a distinct index — no mutex needed for the slice itself)
4. `wg.Wait()` — then print + log results sequentially
5. Outage tracking (continuous mode only) updates `monitor.outageStart`

### Key design decisions

- **No temp files for state** — IOPS state (`map[string]*volumeState`) lives in memory keyed by the full mount-point path.
- **Hang protection via `withTimeout`** — wraps blocking `os.Create/Open/Remove` in a goroutine; returns after `ioTimeout` regardless. The goroutine may outlive the timeout (unavoidable for blocking syscalls in Go) but the iteration is never stalled.
- **`/proc/mounts` instead of `mountpoint`/`df` commands** — non-blocking even when a volume is hung. Used by both `isMounted` and `getDeviceForMount`.
- **`/proc/diskstats` fields** — column 4 (index 3) = reads completed; column 8 (index 7) = writes completed. These map to awk `$4` and `$8`.
- **32-bit counter wraparound** — handled in `calculateIOPS` via `math.MaxUint32`.
- **Outage timer** — gated on `m.continuous`; skipped entirely for single-shot runs.
- **Mount-point filter** — `onlyMounted bool` in `monitor`; set to `!allDirs` from the CLI flag. When true, `listVolumes` calls `isMounted` per entry before including it.

## State map key

The `states` map uses the **full absolute mount-point path** as key (e.g. `/mnt/vol_1`). This means state persists correctly across iterations even if the volume list changes at runtime.

## CLI flags (cobra/pflag)

| Long | Short | Variable | Default |
|---|---|---|---|
| `--mount-dir` | `-m` | `mountDir` | `/mnt` |
| `--interval` | `-i` | `intervalSec` | `5` |
| `--log-dir` | `-l` | `logDir` | `./logs` |
| `--io-timeout` | — | `ioTimeoutMs` | `200` |
| `--continuous` | `-c` | `continuous` | `false` |
| `--all-dirs` | `-a` | `allDirs` | `false` |
| `--description` | `-d` | `descriptions` | `nil` |

`newMonitor` receives `!allDirs` as `onlyMounted` and `continuous` directly. `descriptions` is consumed before `newMonitor` is called: it is written as a header block at the top of the log file and is not stored in the `monitor` struct.

## Volume status strings

`ACCESSIBLE` · `UNMOUNTED` · `WRITE_FAILED` · `READ_FAILED` · `CLEANUP_FAILED`

## Log format

When `--description` lines are provided they are written first:

```
================================================================================
[YYYY-MM-DD HH:MM:SS] Test description:
  <line 1>
  <line 2>
================================================================================
```

Subsequent lines:

```
[YYYY-MM-DD HH:MM:SS] <volname> Status=<status> ReadIOPS=<n> WriteIOPS=<n>
[YYYY-MM-DD HH:MM:SS] [WARNING] Outage started: N/M volumes unavailable
[YYYY-MM-DD HH:MM:SS] [INFO] Outage resolved after <duration>
```

Log file name pattern: `lightbits-monitor-YYYYMMDD_HHMM.log`

## Extending the project

- **Add a new status** — return a new string from `checkAccessibility`; the rest of the pipeline handles it automatically (red color, counted as failure).
- **Persist state across restarts** — serialize `monitor.states` to a JSON file on shutdown and reload on startup.
- **Add metrics export** — add a Prometheus HTTP handler alongside the monitoring loop in `main`; read from `monitor.states` under `monitor.mu`.
- **Per-volume outage tracking** — move `outageStart` from `monitor` to `volumeState` and update per-result instead of per-cycle.
