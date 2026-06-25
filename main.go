package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/sanderdescamps/lightbits-monitor/lightbits"
	"github.com/spf13/cobra"
)

const (
	colorRed    = "\033[0;31m"
	colorGreen  = "\033[0;32m"
	colorYellow = "\033[0;33m"
	colorReset  = "\033[0m"

	tsFormat        = "2006-01-02 15:04:05"
	tsFile          = "20060102_1504"
	testFileContent = "probe\n"
	testFileName    = ".resilience_readwrite_test_%d"
)

// volumeIOCounter holds the last-seen diskstat counters and wall-clock time for one volume.
// All state lives in memory.
type volumeIOCounter struct {
	readIOs   uint64
	writeIOs  uint64
	timestamp time.Time
}

type NodeStatus struct {
	Status string `json:"status"`
	IP     string `json:"ip"`
}

// volumeResult is the outcome of one volume check cycle.
type volumeResult struct {
	name       string
	status     string
	readIOPS   int64 // -1 when unavailable (first sample or no device)
	writeIOPS  int64
	timestamp  time.Time
	nodeStatus []NodeStatus // NVMe-oF paths from "nvme list-subsys -o json"; nil when not applicable
}

// monitor owns the shared state and drives all checks.
type monitor struct {
	mountDir     string
	interval     time.Duration
	ioTimeout    time.Duration
	onlyMounted  bool // when true, listVolumes skips dirs that are not actual mount points
	readOnlyTest bool // when true, only read tests are performed, no write test (except for creating a test file if it doesn't exist)
	logFile      *os.File

	mu                sync.Mutex
	ioCounters        map[string]*volumeIOCounter // keyed by full mount-point path
	outageStart       *time.Time                  // non-nil while a cluster-wide outage is active
	volumeOutageStart map[string]*time.Time       // per-volume outage start time; keyed by full mount-point path
}

func newMonitor(mountDir string, interval, ioTimeout time.Duration, readOnlyTest, onlyMounted bool, logFile *os.File) *monitor {
	return &monitor{
		mountDir:          mountDir,
		interval:          interval,
		ioTimeout:         ioTimeout,
		onlyMounted:       onlyMounted,
		readOnlyTest:      readOnlyTest,
		logFile:           logFile,
		ioCounters:        make(map[string]*volumeIOCounter),
		volumeOutageStart: make(map[string]*time.Time),
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

var errorTimeout = fmt.Errorf("timed out")

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
		return fmt.Errorf("timed out after %w", errorTimeout)
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

// JSON structs for "nvme list-subsys -o json" output (unexported, local use only).
type nvmeSubsysPath struct {
	Name      string `json:"Name"`      // e.g. "nvme0n1"
	Transport string `json:"Transport"` // e.g. "tcp"
	Address   string `json:"Address"`   // e.g. "traddr=10.0.0.1,trsvcid=4420,src_addr=..."
	State     string `json:"State"`     // e.g. "live"
	ANAState  string `json:"ANAState"`  // e.g. "optimized", "inaccessible"
}

func (p nvmeSubsysPath) TrAddr() string {
	for _, field := range strings.Split(p.Address, ",") {
		if strings.HasPrefix(field, "traddr=") {
			return strings.TrimPrefix(field, "traddr=")
		}
	}
	return ""
}

type nvmeSubsystem struct {
	Name  string           `json:"Name"` // e.g. "subsys1"
	NQN   string           `json:"NQN"`  // e.g. "nqn.2014-08.org.nvmexpress:uuid:1234-5678-90ab-cdef"
	Paths []nvmeSubsysPath `json:"Paths"`
}

type nvmeHostEntry struct {
	Subsystems []nvmeSubsystem `json:"Subsystems"`
}

// getNVMeNodeStatus returns the NVMe-oF paths currently connected for device
// by running "nvme list-subsys <device> -o json" and parsing the JSON output.
// Each entry carries the target IP (traddr) and its ANA state.
// Returns nil when the device is not an NVMe device, nvme-cli is unavailable,
// or the command fails.
func getNVMeNodeStatus(device string, timeout time.Duration) []NodeStatus {
	if device == "" || !strings.HasPrefix(filepath.Base(device), "nvme") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvme", "list-subsys", device, "-o", "json").Output()
	if err != nil {
		return nil
	}
	var entries []nvmeHostEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		return nil
	}
	var nodes []NodeStatus
	for _, entry := range entries {
		for _, subsys := range entry.Subsystems {
			for _, path := range subsys.Paths {
				// Address format: "traddr=10.0.0.1,trsvcid=4420,src_addr=..."
				nodes = append(nodes, NodeStatus{
					IP:     path.TrAddr(),
					Status: fmt.Sprintf("%s %s", path.State, path.ANAState),
				})

			}
		}
	}
	return nodes
}

func (m *monitor) prepTest(mountPoint string) error {
	return withTimeout(m.ioTimeout, func() error {
		testFile := filepath.Join(mountPoint, fmt.Sprintf(testFileName, os.Getpid()))
		// f, err := os.OpenFile(testFile, os.O_RDWR|os.O_CREATE|os.O_SYNC|syscall.O_DIRECT, 0666) //nolint:errcheck
		f, err := os.Create(testFile)
		if err != nil {
			return err
		}
		if _, err := f.WriteString(testFileContent); err != nil {
			f.Close() //nolint:errcheck
			return err
		}
		return f.Close()
	})
}

func (m *monitor) checkAccessibility(mountPoint string, readOnly bool) string {
	testFile := filepath.Join(mountPoint, fmt.Sprintf(testFileName, os.Getpid()))

	// Test read.
	if err := withTimeout(m.ioTimeout, func() error {
		f, err := os.OpenFile(testFile, os.O_RDONLY|os.O_SYNC|syscall.O_NOATIME, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "debug: failed to open file %s: %v\n", testFile, err)
			return err
		}
		defer f.Close()
		buf := make([]byte, len(testFileContent))
		_, err = f.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "debug: failed to read file %s: %v\n", testFile, err)
			return err
		} else if string(buf) != testFileContent {
			return fmt.Errorf("read content mismatch")
		}
		return nil
	}); err != nil {
		return "READ_FAILED"
	}

	// Test write – use timeout so a hung mount doesn't stall the whole iteration.
	if !readOnly {
		if err := withTimeout(m.ioTimeout, func() error {
			f, err := os.OpenFile(testFile, os.O_RDWR|os.O_SYNC|syscall.O_NOATIME, 0666)
			if err != nil {
				fmt.Fprintf(os.Stderr, "debug: failed to open file %s: %v\n", testFile, err)
				return err
			}
			if _, err := f.WriteString(testFileContent); err != nil {
				f.Close() //nolint:errcheck
				fmt.Fprintf(os.Stderr, "debug: failed to write file %s: %v\n", testFile, err)
				return err
			}
			return f.Close()
		}); err != nil {
			return "WRITE_FAILED"
		}
	}

	return "ACCESSIBLE"
}

func (m *monitor) cleanupTest() {
	volumes, err := m.listVolumes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to list volumes for cleanup: %v\n", err)
		return
	}
	for _, volume := range volumes {
		fullPath := filepath.Join(m.mountDir, volume, fmt.Sprintf(testFileName, os.Getpid()))
		if err := withTimeout(m.ioTimeout, func() error {
			return os.Remove(fullPath)
		}); err != nil && !errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "[WARNING] Failed to remove test file on %s: %v\n", fullPath, err)
		}
	}
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
	prev, exists := m.ioCounters[mountPoint]
	m.ioCounters[mountPoint] = &volumeIOCounter{readIOs: readIOs, writeIOs: writeIOs, timestamp: now}
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

func (m *monitor) printError(line string) {
	fmt.Fprintf(os.Stderr, "%s[ERROR]%s %v\n", colorRed, colorReset, line)
}

func (m *monitor) printWarning(line string) {
	fmt.Fprintf(os.Stderr, "%s[WARNING]%s %v\n", colorYellow, colorReset, line)
}

func (m *monitor) printInfo(line string) {
	fmt.Fprintf(os.Stderr, "[INFO] %v\n", line)
}

// runIteration discovers volume subdirectories, checks all of them in parallel,
// and prints a results table.
func (m *monitor) run() {
	volumes, err := m.listVolumes()
	if err != nil {
		m.printWarning(fmt.Sprintf("%v", err))
		return
	}
	if len(volumes) == 0 {
		m.printWarning(fmt.Sprintf("No subdirectories found in %s", m.mountDir))
		return
	}

	mountPoints := []string{}
	for _, name := range volumes {
		mountPoint := filepath.Join(m.mountDir, name)
		err := m.prepTest(mountPoint)
		if err != nil {
			m.printError(fmt.Sprintf("Failed to prepare test file for %s: %v", mountPoint, err))
		} else {
			mountPoints = append(mountPoints, mountPoint)
		}
	}

	for {
		results := make([]volumeResult, len(mountPoints))
		var wg sync.WaitGroup

		for i, mountPoint := range mountPoints {
			wg.Add(1)
			go func(idx int, mountPoint string) {
				defer wg.Done()
				ts := time.Now()
				var status string
				status = m.checkAccessibility(mountPoint, m.readOnlyTest)
				r, w := m.calculateIOPS(mountPoint)
				device := getDeviceForMount(mountPoint)
				nodes := getNVMeNodeStatus(device, m.ioTimeout)
				// Each goroutine writes to a distinct slice index – no data race.
				results[idx] = volumeResult{
					name:       filepath.Base(mountPoint),
					status:     status,
					readIOPS:   r,
					writeIOPS:  w,
					timestamp:  ts,
					nodeStatus: nodes,
				}
			}(i, mountPoint)
		}
		wg.Wait()

		cycleNow := time.Now()
		cycleTs := cycleNow.Format(tsFormat)
		fmt.Println("=====================================")
		fmt.Printf("Monitoring Cycle: %s\n", cycleTs)
		fmt.Println("=====================================")
		fmt.Printf("%-22s %-15s %-16s %-14s %-12s %-12s %s\n", "Timestamp", "Volume", "Status", "Outage", "Read IOPS", "Write IOPS", "NodeStatus")
		fmt.Printf("%-22s %-15s %-16s %-14s %-12s %-12s %s\n", "---------", "------", "------", "------", "---------", "----------", "----------")

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

			// Per-volume outage timer (continuous mode only).
			outageStr := "-"
			mountPoint := filepath.Join(m.mountDir, r.name)
			if r.status != "ACCESSIBLE" {
				if m.volumeOutageStart[mountPoint] == nil {
					m.volumeOutageStart[mountPoint] = &cycleNow
				}
				outageStr = cycleNow.Sub(*m.volumeOutageStart[mountPoint]).Round(time.Second).String()
			} else if m.volumeOutageStart[mountPoint] != nil {
				duration := cycleNow.Sub(*m.volumeOutageStart[mountPoint]).Round(time.Second)
				recovMsg := fmt.Sprintf("%s recovered after %s\n", r.name, duration)
				m.printInfo(recovMsg)
				m.appendLog(fmt.Sprintf("[%s] [INFO] %s\n", cycleTs, recovMsg))
				delete(m.volumeOutageStart, mountPoint)
			}

			nodesStr := "-"
			if len(r.nodeStatus) > 0 {
				parts := make([]string, len(r.nodeStatus))
				for i, n := range r.nodeStatus {
					parts[i] = n.IP + "[" + n.Status + "]"
				}
				nodesStr = strings.Join(parts, ", ")
			}
			volTs := r.timestamp.Format(tsFormat)
			fmt.Printf("%-22s %-15s %s%-16s %-14s%s %-12s %-12s %s\n",
				volTs, r.name, color, r.status, outageStr, colorReset, readStr, writeStr, nodesStr)

			m.appendLog(fmt.Sprintf("[%s] %s Status=%s Outage=%s ReadIOPS=%s WriteIOPS=%s Nodes=%s\n",
				volTs, r.name, r.status, outageStr, readStr, writeStr, nodesStr))
		}

		// Outage tracking: start timer on first failure, stop it when all volumes recover.
		if failures > 0 && m.outageStart == nil {
			t := cycleNow
			m.outageStart = &t
			msg := fmt.Sprintf("Outage started: %d/%d volumes unavailable\n", failures, len(mountPoints))
			m.printWarning(msg)
			m.appendLog(fmt.Sprintf("[%s] [WARNING] %s", cycleTs, msg))
		} else if failures == 0 && m.outageStart != nil {
			duration := cycleNow.Sub(*m.outageStart).Round(time.Second)
			msg := fmt.Sprintf("Outage resolved after %s\n", duration)
			m.printInfo(msg)
			m.appendLog(fmt.Sprintf("[%s] [INFO] %s", cycleTs, msg))
			m.outageStart = nil
		}

		fmt.Println()

		fmt.Printf("%-22s %-15s %-16s %-14s %-12d %-12d %s\n", "", "TOTAL", "", "", totalRead, totalWrite, "")
		if m.outageStart != nil {
			elapsed := cycleNow.Sub(*m.outageStart).Round(time.Second)
			fmt.Printf("%sOutage in progress: started %s, duration %s%s\n",
				colorRed, m.outageStart.Format(tsFormat), elapsed, colorReset)
		}
		fmt.Printf("Summary: %d/%d accessible, %d failures\n\n", accessible, len(mountPoints), failures)

		time.Sleep(m.interval)
	}

}

func main() {
	var (
		mountDir     string
		intervalSec  float64
		logDir       string
		ioTimeoutMs  float64
		allDirs      bool
		readOnlyTest bool
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

			mon := newMonitor(mountDir, interval, ioTimeout, readOnlyTest, !allDirs, logFile)

			// Clean up probe files on Ctrl+C or SIGTERM.
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			go func() {
				sig := <-sigCh
				shutdownMsg := fmt.Sprintf("\n[%s] [INFO] Received %v, cleaning up test files...\n",
					time.Now().Format(tsFormat), sig)
				fmt.Print(shutdownMsg)
				mon.appendLog(shutdownMsg)
				mon.cleanupTest()
				logFile.Close()
				os.Exit(0)
			}()

			startMsg := fmt.Sprintf("[%s] [INFO] Starting resilience monitor (interval: %v, mount-dir: %s)\n",
				time.Now().Format(tsFormat), interval, mountDir)
			fmt.Print(startMsg)
			mon.appendLog(startMsg)
			mon.run()

			return nil
		},
	}

	f := rootCmd.Flags()
	f.StringVarP(&mountDir, "mount-dir", "m", "/mnt", "Directory whose subdirectories are the volumes to monitor")
	f.Float64VarP(&intervalSec, "interval", "i", 5, "Check interval in seconds")
	f.StringVarP(&logDir, "log-dir", "l", "./logs", "Log directory")
	f.Float64Var(&ioTimeoutMs, "io-timeout", 200, "I/O operation timeout in milliseconds")
	f.BoolVarP(&allDirs, "all-dirs", "a", false, "Monitor all subdirectories, not just actual mount points")
	f.BoolVarP(&readOnlyTest, "read-only", "r", false, "Perform only read tests (no write test except for creating a test file if it doesn't exist)")
	f.StringArrayVarP(&descriptions, "description", "d", nil, "Description of the test; can be repeated to add multiple lines")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
