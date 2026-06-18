package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

const (
	colorRed   = "\033[0;31m"
	colorGreen = "\033[0;32m"
	colorReset = "\033[0m"

	tsFormat = "2006-01-02 15:04:05"
	tsFile   = "20060102_1504"
)

// volumeState holds the last-seen diskstat counters and wall-clock time for one volume.
// All state lives in memory – no temp files needed.
type volumeState struct {
	readIOs   uint64
	writeIOs  uint64
	timestamp time.Time
}

// volumeResult is the outcome of one volume check cycle.
type volumeResult struct {
	name      string
	status    string
	readIOPS  int64 // -1 when unavailable (first sample or no device)
	writeIOPS int64
	timestamp time.Time
}

// monitor owns the shared state and drives all checks.
type monitor struct {
	mountDir    string
	interval    time.Duration
	ioTimeout   time.Duration
	onlyMounted bool // when true, listVolumes skips dirs that are not actual mount points
	continuous  bool // when false, outage tracking is disabled
	logFile     *os.File

	mu          sync.Mutex
	states      map[string]*volumeState // keyed by full mount-point path
	outageStart *time.Time              // non-nil while an outage is active; set on first failure, cleared on full recovery
}

func newMonitor(mountDir string, interval, ioTimeout time.Duration, onlyMounted, continuous bool, logFile *os.File) *monitor {
	return &monitor{
		mountDir:    mountDir,
		interval:    interval,
		ioTimeout:   ioTimeout,
		onlyMounted: onlyMounted,
		continuous:  continuous,
		logFile:     logFile,
		states:      make(map[string]*volumeState),
	}
}

// listVolumes returns the sorted list of subdirectory names inside mountDir.
// Called at the start of every iteration so volumes added or removed at runtime
// are picked up automatically.
// When onlyMounted is true, directories that are not actual mount points
// (e.g. plain directories on the root filesystem) are silently skipped.
func (m *monitor) listVolumes() ([]string, error) {
	entries, err := os.ReadDir(m.mountDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read mount-dir %s: %w", m.mountDir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if m.onlyMounted && !isMounted(filepath.Join(m.mountDir, e.Name())) {
			continue
		}
		names = append(names, e.Name())
	}
	return names, nil
}

// withTimeout runs fn in a goroutine and returns an error if it does not finish
// within d. The spawned goroutine may outlive the timeout when blocked on a
// kernel syscall (e.g. a hung NVMe-oF mount), but the caller is never blocked.
func withTimeout(d time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return fmt.Errorf("timed out after %v", d)
	}
}

// isMounted returns true when mountPoint appears as a mount target in /proc/mounts.
// Reading /proc/mounts is non-blocking even on hung mounts.
func isMounted(mountPoint string) bool {
	abs, _ := filepath.Abs(mountPoint)
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) >= 2 && fields[1] == abs {
			return true
		}
	}
	return false
}

// getDeviceForMount returns the block device mounted at mountPoint from /proc/mounts.
// Returns an empty string when the mount is not found.
func getDeviceForMount(mountPoint string) string {
	abs, _ := filepath.Abs(mountPoint)
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) >= 2 && fields[1] == abs {
			return fields[0]
		}
	}
	return ""
}

// getDiskStats reads /proc/diskstats and returns (readsCompleted, writesCompleted)
// for device. The fields match awk columns $4 and $8 from the bash script.
func getDiskStats(device string) (uint64, uint64, error) {
	devName := filepath.Base(device)
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		// /proc/diskstats: major minor name reads_completed reads_merged sectors_read … writes_completed …
		if len(fields) < 8 || fields[2] != devName {
			continue
		}
		r, err1 := strconv.ParseUint(fields[3], 10, 64)
		w, err2 := strconv.ParseUint(fields[7], 10, 64)
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("diskstats parse error for %s", devName)
		}
		return r, w, nil
	}
	return 0, 0, fmt.Errorf("device %s not found in /proc/diskstats", devName)
}

// checkAccessibility tests whether mountPoint is reachable, returning one of:
// ACCESSIBLE, UNMOUNTED, WRITE_FAILED, READ_FAILED, CLEANUP_FAILED.
func (m *monitor) checkAccessibility(mountPoint string) string {
	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		return "UNMOUNTED"
	}
	if !isMounted(mountPoint) {
		return "UNMOUNTED"
	}

	testFile := filepath.Join(mountPoint, fmt.Sprintf(".resilience_test_%d", os.Getpid()))

	// Test write – use timeout so a hung mount doesn't stall the whole iteration.
	if err := withTimeout(m.ioTimeout, func() error {
		f, err := os.Create(testFile)
		if err != nil {
			return err
		}
		return f.Close()
	}); err != nil {
		return "WRITE_FAILED"
	}

	// Test read.
	if err := withTimeout(m.ioTimeout, func() error {
		f, err := os.Open(testFile)
		if err != nil {
			return err
		}
		return f.Close()
	}); err != nil {
		withTimeout(m.ioTimeout, func() error { return os.Remove(testFile) }) //nolint:errcheck
		return "READ_FAILED"
	}

	// Cleanup.
	if err := withTimeout(m.ioTimeout, func() error {
		return os.Remove(testFile)
	}); err != nil {
		return "CLEANUP_FAILED"
	}

	return "ACCESSIBLE"
}

// calculateIOPS returns (readIOPS, writeIOPS) for mountPoint based on the delta since
// the last call. Returns -1 when data is not yet available (first sample, no device,
// or no elapsed time).
func (m *monitor) calculateIOPS(mountPoint string) (int64, int64) {
	device := getDeviceForMount(mountPoint)
	if device == "" {
		return -1, -1
	}

	readIOs, writeIOs, err := getDiskStats(device)
	if err != nil {
		return -1, -1
	}
	now := time.Now()

	m.mu.Lock()
	prev, exists := m.states[mountPoint]
	m.states[mountPoint] = &volumeState{readIOs: readIOs, writeIOs: writeIOs, timestamp: now}
	m.mu.Unlock()

	if !exists {
		return -1, -1
	}

	elapsed := now.Sub(prev.timestamp).Seconds()
	if elapsed <= 0 {
		return -1, -1
	}

	// Handle 32-bit counter wraparound.
	var readDelta, writeDelta uint64
	if readIOs >= prev.readIOs {
		readDelta = readIOs - prev.readIOs
	} else {
		readDelta = (math.MaxUint32 - prev.readIOs) + readIOs + 1
	}
	if writeIOs >= prev.writeIOs {
		writeDelta = writeIOs - prev.writeIOs
	} else {
		writeDelta = (math.MaxUint32 - prev.writeIOs) + writeIOs + 1
	}

	return int64(float64(readDelta) / elapsed), int64(float64(writeDelta) / elapsed)
}

func (m *monitor) appendLog(line string) {
	if m.logFile != nil {
		m.logFile.WriteString(line) //nolint:errcheck
	}
}

// runIteration discovers volume subdirectories, checks all of them in parallel,
// and prints a results table.
func (m *monitor) runIteration() {
	volumes, err := m.listVolumes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARNING] %v\n", err)
		return
	}
	if len(volumes) == 0 {
		fmt.Fprintf(os.Stderr, "[WARNING] No subdirectories found in %s\n", m.mountDir)
		return
	}

	results := make([]volumeResult, len(volumes))
	var wg sync.WaitGroup

	for i, name := range volumes {
		wg.Add(1)
		go func(idx int, volName string) {
			defer wg.Done()
			mountPoint := filepath.Join(m.mountDir, volName)
			ts := time.Now()
			status := m.checkAccessibility(mountPoint)
			r, w := m.calculateIOPS(mountPoint)
			// Each goroutine writes to a distinct slice index – no data race.
			results[idx] = volumeResult{
				name:      volName,
				status:    status,
				readIOPS:  r,
				writeIOPS: w,
				timestamp: ts,
			}
		}(i, name)
	}
	wg.Wait()

	cycleNow := time.Now()
	cycleTs := cycleNow.Format(tsFormat)
	fmt.Println("=====================================")
	fmt.Printf("Monitoring Cycle: %s\n", cycleTs)
	fmt.Println("=====================================")
	fmt.Printf("%-22s %-15s %-15s %-15s %-15s\n", "Timestamp", "Volume", "Status", "Read IOPS", "Write IOPS")
	fmt.Printf("%-22s %-15s %-15s %-15s %-15s\n", "---------", "------", "------", "---------", "----------")

	var failures, accessible int
	var totalRead, totalWrite int64

	m.appendLog(fmt.Sprintf("%s\n", strings.Repeat("-", 80)))

	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})

	for _, r := range results {
		color := colorGreen
		if r.status != "ACCESSIBLE" {
			color = colorRed
			failures++
		} else {
			accessible++
		}

		readStr, writeStr := "N/A", "N/A"
		if r.readIOPS >= 0 {
			readStr = strconv.FormatInt(r.readIOPS, 10)
			totalRead += r.readIOPS
		}
		if r.writeIOPS >= 0 {
			writeStr = strconv.FormatInt(r.writeIOPS, 10)
			totalWrite += r.writeIOPS
		}

		volTs := r.timestamp.Format(tsFormat)
		fmt.Printf("%-22s %-15s %s%-15s%s %-15s %-15s\n",
			volTs, r.name, color, r.status, colorReset, readStr, writeStr)

		m.appendLog(fmt.Sprintf("[%s] %s Status=%s ReadIOPS=%s WriteIOPS=%s\n",
			volTs, r.name, r.status, readStr, writeStr))
	}

	// Outage tracking: start timer on first failure, stop it when all volumes recover.
	// Only active in continuous mode – a single-shot run has no prior state to compare against.
	if m.continuous {
		if failures > 0 && m.outageStart == nil {
			t := cycleNow
			m.outageStart = &t
			msg := fmt.Sprintf("[%s] [WARNING] Outage started: %d/%d volumes unavailable\n",
				cycleTs, failures, len(volumes))
			fmt.Printf("%s%s%s", colorRed, msg, colorReset)
			m.appendLog(msg)
		} else if failures == 0 && m.outageStart != nil {
			duration := cycleNow.Sub(*m.outageStart).Round(time.Second)
			msg := fmt.Sprintf("[%s] [INFO] Outage resolved after %s\n", cycleTs, duration)
			fmt.Printf("%s%s%s", colorGreen, msg, colorReset)
			m.appendLog(msg)
			m.outageStart = nil
		}
	}

	fmt.Println()
	fmt.Printf("%-22s %-15s %-15s %-15d %-15d\n", "", "TOTAL", "", totalRead, totalWrite)
	if m.continuous && m.outageStart != nil {
		elapsed := cycleNow.Sub(*m.outageStart).Round(time.Second)
		fmt.Printf("%sOutage in progress: started %s, duration %s%s\n",
			colorRed, m.outageStart.Format(tsFormat), elapsed, colorReset)
	}
	fmt.Printf("Summary: %d/%d accessible, %d failures\n\n", accessible, len(volumes), failures)
}

func main() {
	var (
		mountDir     string
		intervalSec  float64
		logDir       string
		ioTimeoutMs  float64
		continuous   bool
		allDirs      bool
		descriptions []string
	)

	rootCmd := &cobra.Command{
		Use:          "lightbits-monitor",
		Short:        "Test availability of all volumes mounted under a directory and measure their IOPS",
		Long:         "This tool checks the accessibility of all subdirectories in a specified mount directory, which represent mounted volumes. It performs read/write tests and calculates IOPS based on /proc/diskstats. The results are printed in a table and optionally logged to a file. It can run once or continuously at a specified interval.",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := os.MkdirAll(logDir, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: Failed to create log directory '%s': %v. Exiting.\n", logDir, err)
				os.Exit(1)
			}

			logPath := filepath.Join(logDir, fmt.Sprintf("lightbits-monitor-%s.log", time.Now().Format(tsFile)))
			logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: Failed to open log file '%s': %v. Exiting.\n", logPath, err)
				os.Exit(1)
			}
			defer logFile.Close()

			// Write description header at the top of the log file.
			if len(descriptions) > 0 {
				logFile.WriteString(fmt.Sprintf("%s\n", strings.Repeat("=", 80)))                         //nolint:errcheck
				logFile.WriteString(fmt.Sprintf("[%s] Test description:\n", time.Now().Format(tsFormat))) //nolint:errcheck
				for _, d := range descriptions {
					logFile.WriteString(fmt.Sprintf("  %s\n", d)) //nolint:errcheck
				}
				logFile.WriteString(fmt.Sprintf("%s\n", strings.Repeat("=", 80))) //nolint:errcheck
			}

			interval := time.Duration(float64(time.Second) * intervalSec)
			ioTimeout := time.Duration(float64(time.Millisecond) * ioTimeoutMs)

			mon := newMonitor(mountDir, interval, ioTimeout, !allDirs, continuous, logFile)

			if continuous {
				startMsg := fmt.Sprintf("[%s] [INFO] Starting resilience monitor (interval: %v, mount-dir: %s)\n",
					time.Now().Format(tsFormat), interval, mountDir)
				fmt.Print(startMsg)
				mon.appendLog(startMsg)
				for {
					mon.runIteration()
					time.Sleep(interval)
				}
			} else {
				mon.runIteration()
			}
			return nil
		},
	}

	f := rootCmd.Flags()
	f.StringVarP(&mountDir, "mount-dir", "m", "/mnt", "Directory whose subdirectories are the volumes to monitor")
	f.Float64VarP(&intervalSec, "interval", "i", 5, "Check interval in seconds")
	f.StringVarP(&logDir, "log-dir", "l", "./logs", "Log directory")
	f.Float64Var(&ioTimeoutMs, "io-timeout", 200, "I/O operation timeout in milliseconds")
	f.BoolVarP(&continuous, "continuous", "c", false, "Run continuously")
	f.BoolVarP(&allDirs, "all-dirs", "a", false, "Monitor all subdirectories, not just actual mount points")
	f.StringArrayVarP(&descriptions, "description", "d", nil, "Description of the test; can be repeated to add multiple lines")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
