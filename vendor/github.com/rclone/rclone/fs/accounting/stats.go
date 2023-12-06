package accounting

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/lib/terminal"
)

// MaxCompletedTransfers specifies maximum number of completed transfers in startedTransfers list
var MaxCompletedTransfers = 100

var startTime = time.Now()

// StatsInfo accounts all transfers
type StatsInfo struct {
	mu                sync.RWMutex
	ctx               context.Context
	ci                *fs.ConfigInfo
	bytes             int64
	errors            int64
	lastError         error
	fatalError        bool
	retryError        bool
	retryAfter        time.Time
	checks            int64
	checking          *transferMap
	checkQueue        int
	checkQueueSize    int64
	transfers         int64
	transferring      *transferMap
	transferQueue     int
	transferQueueSize int64
	renames           int64
	renameQueue       int
	renameQueueSize   int64
	deletes           int64
	deletedDirs       int64
	inProgress        *inProgress

	// The root cause of SM issue #3298 were transfer stats (startedTransfers)
	// accumulated and never pruned during the whole backup run.
	// Even though by default MaxCompletedTransfers should take care of that,
	// SM relies on full access to transfer statistics and disables it by setting it to -1 in agent setup.
	// The solution is to keep aggregated stats for transfers pruned from startedTransfers in oldTransfers.
	startedTransfers []*Transfer // currently active transfers
	oldTransfers     AggregatedTransferInfo
	oldTimeRanges    timeRanges    // a merged list of time ranges for the transfers
	oldDuration      time.Duration // duration of transfers we have culled
	group            string
}

// NewStats creates an initialised StatsInfo
func NewStats(ctx context.Context) *StatsInfo {
	ci := fs.GetConfig(ctx)
	return &StatsInfo{
		ctx:          ctx,
		ci:           ci,
		checking:     newTransferMap(ci.Checkers, "checking"),
		transferring: newTransferMap(ci.Transfers, "transferring"),
		inProgress:   newInProgress(ctx),
	}
}

// RemoteStats returns stats for rc
func (s *StatsInfo) RemoteStats() (out rc.Params, err error) {
	out = make(rc.Params)
	s.mu.RLock()
	out["speed"] = s.Speed()
	out["bytes"] = s.bytes
	out["errors"] = s.errors
	out["fatalError"] = s.fatalError
	out["retryError"] = s.retryError
	out["checks"] = s.checks
	out["transfers"] = s.transfers
	out["deletes"] = s.deletes
	out["deletedDirs"] = s.deletedDirs
	out["renames"] = s.renames
	out["transferTime"] = s.totalDuration().Seconds()
	out["elapsedTime"] = time.Since(startTime).Seconds()
	s.mu.RUnlock()
	if !s.checking.empty() {
		out["checking"] = s.checking.remotes()
	}
	if !s.transferring.empty() {
		out["transferring"] = s.transferring.rcStats(s.inProgress)
	}
	if s.errors > 0 {
		out["lastError"] = s.lastError.Error()
	}
	return out, nil
}

// Speed returns the average speed of the transfer in bytes/second
func (s *StatsInfo) Speed() float64 {
	dt := s.totalDuration()
	dtSeconds := dt.Seconds()
	speed := 0.0
	if dt > 0 {
		speed = float64(s.bytes) / dtSeconds
	}
	return speed
}

// timeRange is a start and end time of a transfer
type timeRange struct {
	start time.Time
	end   time.Time
}

// timeRanges is a list of non-overlapping start and end times for
// transfers
type timeRanges []timeRange

// merge all the overlapping time ranges
func (trs *timeRanges) merge() {
	Trs := *trs

	// Sort by the starting time.
	sort.Slice(Trs, func(i, j int) bool {
		return Trs[i].start.Before(Trs[j].start)
	})

	// Merge overlaps and add distinctive ranges together
	var (
		newTrs = Trs[:0]
		i, j   = 0, 1
	)
	for i < len(Trs) {
		if j < len(Trs) {
			if !Trs[i].end.Before(Trs[j].start) {
				if Trs[i].end.Before(Trs[j].end) {
					Trs[i].end = Trs[j].end
				}
				j++
				continue
			}
		}
		newTrs = append(newTrs, Trs[i])
		i = j
		j++
	}

	*trs = newTrs
}

// cull remove any ranges whose start and end are before cutoff
// returning their duration sum
func (trs *timeRanges) cull(cutoff time.Time) (d time.Duration) {
	var newTrs = (*trs)[:0]
	for _, tr := range *trs {
		if cutoff.Before(tr.start) || cutoff.Before(tr.end) {
			newTrs = append(newTrs, tr)
		} else {
			d += tr.end.Sub(tr.start)
		}
	}
	*trs = newTrs
	return d
}

// total the time out of the time ranges
func (trs timeRanges) total() (total time.Duration) {
	for _, tr := range trs {
		total += tr.end.Sub(tr.start)
	}
	return total
}

// Total duration is union of durations of all transfers belonging to this
// object.
// Needs to be protected by mutex.
func (s *StatsInfo) totalDuration() time.Duration {
	// copy of s.oldTimeRanges with extra room for the current transfers
	timeRanges := make(timeRanges, len(s.oldTimeRanges), len(s.oldTimeRanges)+len(s.startedTransfers))
	copy(timeRanges, s.oldTimeRanges)

	// Extract time ranges of all transfers.
	now := time.Now()
	for i := range s.startedTransfers {
		start, end := s.startedTransfers[i].TimeRange()
		if end.IsZero() {
			end = now
		}
		timeRanges = append(timeRanges, timeRange{start, end})
	}

	timeRanges.merge()
	return s.oldDuration + timeRanges.total()
}

// eta returns the ETA of the current operation,
// rounded to full seconds.
// If the ETA cannot be determined 'ok' returns false.
func eta(size, total int64, rate float64) (eta time.Duration, ok bool) {
	if total <= 0 || size < 0 || rate <= 0 {
		return 0, false
	}
	remaining := total - size
	if remaining < 0 {
		return 0, false
	}
	seconds := float64(remaining) / rate
	return time.Second * time.Duration(seconds), true
}

// etaString returns the ETA of the current operation,
// rounded to full seconds.
// If the ETA cannot be determined it returns "-"
func etaString(done, total int64, rate float64) string {
	d, ok := eta(done, total, rate)
	if !ok {
		return "-"
	}
	return fs.Duration(d).ReadableString()
}

// percent returns a/b as a percentage rounded to the nearest integer
// as a string
//
// if the percentage is invalid it returns "-"
func percent(a int64, b int64) string {
	if a < 0 || b <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d%%", int(float64(a)*100/float64(b)+0.5))
}

// String convert the StatsInfo to a string for printing
func (s *StatsInfo) String() string {
	// checking and transferring have their own locking so read
	// here before lock to prevent deadlock on GetBytes
	transferring, checking := s.transferring.count(), s.checking.count()
	transferringBytesDone, transferringBytesTotal := s.transferring.progress(s)

	s.mu.RLock()

	elapsedTime := time.Since(startTime)
	elapsedTimeSecondsOnly := elapsedTime.Truncate(time.Second/10) % time.Minute
	dt := s.totalDuration()
	dtSeconds := dt.Seconds()
	speed := 0.0
	if dt > 0 {
		speed = float64(s.bytes) / dtSeconds
	}

	displaySpeed := speed
	if s.ci.DataRateUnit == "bits" {
		displaySpeed *= 8
	}

	var (
		totalChecks   = int64(s.checkQueue) + s.checks + int64(checking)
		totalTransfer = int64(s.transferQueue) + s.transfers + int64(transferring)
		// note that s.bytes already includes transferringBytesDone so
		// we take it off here to avoid double counting
		totalSize    = s.transferQueueSize + s.bytes + transferringBytesTotal - transferringBytesDone
		currentSize  = s.bytes
		buf          = &bytes.Buffer{}
		xfrchkString = ""
		dateString   = ""
	)

	if !s.ci.StatsOneLine {
		_, _ = fmt.Fprintf(buf, "\nTransferred:   	")
	} else {
		xfrchk := []string{}
		if totalTransfer > 0 && s.transferQueue > 0 {
			xfrchk = append(xfrchk, fmt.Sprintf("xfr#%d/%d", s.transfers, totalTransfer))
		}
		if totalChecks > 0 && s.checkQueue > 0 {
			xfrchk = append(xfrchk, fmt.Sprintf("chk#%d/%d", s.checks, totalChecks))
		}
		if len(xfrchk) > 0 {
			xfrchkString = fmt.Sprintf(" (%s)", strings.Join(xfrchk, ", "))
		}
		if s.ci.StatsOneLineDate {
			t := time.Now()
			dateString = t.Format(s.ci.StatsOneLineDateFormat) // Including the separator so people can customize it
		}
	}

	_, _ = fmt.Fprintf(buf, "%s%10s / %s, %s, %s, ETA %s%s",
		dateString,
		fs.SizeSuffix(s.bytes),
		fs.SizeSuffix(totalSize).Unit("Bytes"),
		percent(s.bytes, totalSize),
		fs.SizeSuffix(displaySpeed).Unit(strings.Title(s.ci.DataRateUnit)+"/s"),
		etaString(currentSize, totalSize, speed),
		xfrchkString,
	)

	if s.ci.ProgressTerminalTitle {
		// Writes ETA to the terminal title
		terminal.WriteTerminalTitle("ETA: " + etaString(currentSize, totalSize, speed))
	}

	if !s.ci.StatsOneLine {
		_, _ = buf.WriteRune('\n')
		errorDetails := ""
		switch {
		case s.fatalError:
			errorDetails = " (fatal error encountered)"
		case s.retryError:
			errorDetails = " (retrying may help)"
		case s.errors != 0:
			errorDetails = " (no need to retry)"

		}

		// Add only non zero stats
		if s.errors != 0 {
			_, _ = fmt.Fprintf(buf, "Errors:        %10d%s\n",
				s.errors, errorDetails)
		}
		if s.checks != 0 || totalChecks != 0 {
			_, _ = fmt.Fprintf(buf, "Checks:        %10d / %d, %s\n",
				s.checks, totalChecks, percent(s.checks, totalChecks))
		}
		if s.deletes != 0 || s.deletedDirs != 0 {
			_, _ = fmt.Fprintf(buf, "Deleted:       %10d (files), %d (dirs)\n", s.deletes, s.deletedDirs)
		}
		if s.renames != 0 {
			_, _ = fmt.Fprintf(buf, "Renamed:       %10d\n", s.renames)
		}
		if s.transfers != 0 || totalTransfer != 0 {
			_, _ = fmt.Fprintf(buf, "Transferred:   %10d / %d, %s\n",
				s.transfers, totalTransfer, percent(s.transfers, totalTransfer))
		}
		_, _ = fmt.Fprintf(buf, "Elapsed time:  %10ss\n", strings.TrimRight(elapsedTime.Truncate(time.Minute).String(), "0s")+fmt.Sprintf("%.1f", elapsedTimeSecondsOnly.Seconds()))
	}

	// checking and transferring have their own locking so unlock
	// here to prevent deadlock on GetBytes
	s.mu.RUnlock()

	// Add per transfer stats if required
	if !s.ci.StatsOneLine {
		if !s.checking.empty() {
			_, _ = fmt.Fprintf(buf, "Checking:\n%s\n", s.checking.String(s.ctx, s.inProgress, s.transferring))
		}
		if !s.transferring.empty() {
			_, _ = fmt.Fprintf(buf, "Transferring:\n%s\n", s.transferring.String(s.ctx, s.inProgress, nil))
		}
	}

	return buf.String()
}

// Transferred returns list of all completed transfers including checked and
// failed ones.
func (s *StatsInfo) Transferred() []TransferSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts := make([]TransferSnapshot, 0, len(s.startedTransfers))

	for _, tr := range s.startedTransfers {
		if tr.IsDone() {
			ts = append(ts, tr.Snapshot())
		}
	}

	return ts
}

// Aggregated returns aggregated stats for all completed and running transfers.
func (s *StatsInfo) Aggregated() AggregatedTransferInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ai := s.oldTransfers
	for _, tr := range s.startedTransfers {
		ai.update(tr)
	}
	return ai
}

// Log outputs the StatsInfo to the log
func (s *StatsInfo) Log() {
	if s.ci.UseJSONLog {
		out, _ := s.RemoteStats()
		fs.LogLevelPrintf(s.ci.StatsLogLevel, nil, "%v%v\n", s, fs.LogValueHide("stats", out))
	} else {
		fs.LogLevelPrintf(s.ci.StatsLogLevel, nil, "%v\n", s)
	}

}

// Bytes updates the stats for bytes bytes
func (s *StatsInfo) Bytes(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes += bytes
}

// GetBytes returns the number of bytes transferred so far
func (s *StatsInfo) GetBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}

// GetBytesWithPending returns the number of bytes transferred and remaining transfers
func (s *StatsInfo) GetBytesWithPending() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pending := int64(0)
	for _, tr := range s.startedTransfers {
		if tr.acc != nil {
			bytes, size := tr.acc.progress()
			if bytes < size {
				pending += size - bytes
			}
		}
	}
	return s.bytes + pending
}

// Errors updates the stats for errors
func (s *StatsInfo) Errors(errors int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors += errors
}

// GetErrors reads the number of errors
func (s *StatsInfo) GetErrors() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errors
}

// GetLastError returns the lastError
func (s *StatsInfo) GetLastError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

// GetChecks returns the number of checks
func (s *StatsInfo) GetChecks() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checks
}

// FatalError sets the fatalError flag
func (s *StatsInfo) FatalError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fatalError = true
}

// HadFatalError returns whether there has been at least one FatalError
func (s *StatsInfo) HadFatalError() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fatalError
}

// RetryError sets the retryError flag
func (s *StatsInfo) RetryError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retryError = true
}

// HadRetryError returns whether there has been at least one non-NoRetryError
func (s *StatsInfo) HadRetryError() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.retryError
}

// Deletes updates the stats for deletes
func (s *StatsInfo) Deletes(deletes int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes += deletes
	return s.deletes
}

// DeletedDirs updates the stats for deletedDirs
func (s *StatsInfo) DeletedDirs(deletedDirs int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedDirs += deletedDirs
	return s.deletedDirs
}

// Renames updates the stats for renames
func (s *StatsInfo) Renames(renames int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renames += renames
	return s.renames
}

// ResetCounters sets the counters (bytes, checks, errors, transfers, deletes, renames) to 0 and resets lastError, fatalError and retryError
func (s *StatsInfo) ResetCounters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes = 0
	s.errors = 0
	s.lastError = nil
	s.fatalError = false
	s.retryError = false
	s.retryAfter = time.Time{}
	s.checks = 0
	s.transfers = 0
	s.deletes = 0
	s.deletedDirs = 0
	s.renames = 0
	s.startedTransfers = nil
	s.oldTransfers = AggregatedTransferInfo{}
	s.oldDuration = 0
}

// ResetErrors sets the errors count to 0 and resets lastError, fatalError and retryError
func (s *StatsInfo) ResetErrors() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = 0
	s.lastError = nil
	s.fatalError = false
	s.retryError = false
	s.retryAfter = time.Time{}
	s.oldTransfers.Failed = 0
	s.oldTransfers.Error = nil
}

// Errored returns whether there have been any errors
func (s *StatsInfo) Errored() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errors != 0
}

// Error adds a single error into the stats, assigns lastError and eventually sets fatalError or retryError
func (s *StatsInfo) Error(err error) error {
	if err == nil || fserrors.IsCounted(err) {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors++
	s.lastError = err
	err = fserrors.FsError(err)
	fserrors.Count(err)
	switch {
	case fserrors.IsFatalError(err):
		s.fatalError = true
	case fserrors.IsRetryAfterError(err):
		retryAfter := fserrors.RetryAfterErrorTime(err)
		if s.retryAfter.IsZero() || retryAfter.Sub(s.retryAfter) > 0 {
			s.retryAfter = retryAfter
		}
		s.retryError = true
	case !fserrors.IsNoRetryError(err):
		s.retryError = true
	}
	return err
}

// RetryAfter returns the time to retry after if it is set.  It will
// be Zero if it isn't set.
func (s *StatsInfo) RetryAfter() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retryAfter
}

// NewCheckingTransfer adds a checking transfer to the stats, from the object.
func (s *StatsInfo) NewCheckingTransfer(obj fs.Object) *Transfer {
	tr := newCheckingTransfer(s, obj)
	s.checking.add(tr)
	return tr
}

// DoneChecking removes a check from the stats
func (s *StatsInfo) DoneChecking(remote string) {
	s.checking.del(remote)
	s.mu.Lock()
	s.checks++
	s.mu.Unlock()
}

// GetTransfers reads the number of transfers
func (s *StatsInfo) GetTransfers() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transfers
}

// NewTransfer adds a transfer to the stats from the object.
func (s *StatsInfo) NewTransfer(obj fs.Object) *Transfer {
	tr := newTransfer(s, obj)
	s.transferring.add(tr)
	return tr
}

// NewTransferRemoteSize adds a transfer to the stats based on remote and size.
func (s *StatsInfo) NewTransferRemoteSize(remote string, size int64) *Transfer {
	tr := newTransferRemoteSize(s, remote, size, false)
	s.transferring.add(tr)
	return tr
}

// DoneTransferring removes a transfer from the stats
//
// if ok is true then it increments the transfers count
func (s *StatsInfo) DoneTransferring(remote string, ok bool) {
	s.transferring.del(remote)
	if ok {
		s.mu.Lock()
		s.transfers++
		s.mu.Unlock()
	}
}

// UpdateSkipped marks file bytes as skipped in oldTransfers.
func (s *StatsInfo) UpdateSkipped(size int64) {
	s.mu.Lock()
	s.oldTransfers.Skipped += size
	s.mu.Unlock()
}

// SetCheckQueue sets the number of queued checks
func (s *StatsInfo) SetCheckQueue(n int, size int64) {
	s.mu.Lock()
	s.checkQueue = n
	s.checkQueueSize = size
	s.mu.Unlock()
}

// SetTransferQueue sets the number of queued transfers
func (s *StatsInfo) SetTransferQueue(n int, size int64) {
	s.mu.Lock()
	s.transferQueue = n
	s.transferQueueSize = size
	s.mu.Unlock()
}

// SetRenameQueue sets the number of queued transfers
func (s *StatsInfo) SetRenameQueue(n int, size int64) {
	s.mu.Lock()
	s.renameQueue = n
	s.renameQueueSize = size
	s.mu.Unlock()
}

// AddTransfer adds reference to the started transfer.
func (s *StatsInfo) AddTransfer(transfer *Transfer) {
	s.mu.Lock()
	s.startedTransfers = append(s.startedTransfers, transfer)
	s.mu.Unlock()
}

// removeTransfer removes a reference to the started transfer in
// position i.
//
// Must be called with the lock held
func (s *StatsInfo) removeTransfer(transfer *Transfer, i int) {
	now := time.Now()

	// add finished transfer onto old time ranges
	start, end := transfer.TimeRange()
	if end.IsZero() {
		end = now
	}
	s.oldTimeRanges = append(s.oldTimeRanges, timeRange{start, end})
	s.oldTimeRanges.merge()
	s.oldTransfers.update(transfer)

	// remove the found entry
	s.startedTransfers = append(s.startedTransfers[:i], s.startedTransfers[i+1:]...)

	// Find the youngest active transfer
	oldestStart := now
	for i := range s.startedTransfers {
		start, _ := s.startedTransfers[i].TimeRange()
		if start.Before(oldestStart) {
			oldestStart = start
		}
	}

	// remove old entries older than that
	s.oldDuration += s.oldTimeRanges.cull(oldestStart)
}

// RemoveTransfer removes a reference to the started transfer.
func (s *StatsInfo) RemoveTransfer(transfer *Transfer) {
	s.mu.Lock()
	for i, tr := range s.startedTransfers {
		if tr == transfer {
			s.removeTransfer(tr, i)
			break
		}
	}
	s.mu.Unlock()
}

// PruneTransfers makes sure there aren't too many old transfers by removing
// single finished transfer.
func (s *StatsInfo) PruneTransfers() {
	if MaxCompletedTransfers < 0 {
		return
	}
	s.mu.Lock()
	// remove a transfer from the start if we are over quota
	if len(s.startedTransfers) > MaxCompletedTransfers+s.ci.Transfers {
		for i, tr := range s.startedTransfers {
			if tr.IsDone() {
				s.removeTransfer(tr, i)
				break
			}
		}
	}
	s.mu.Unlock()
}
