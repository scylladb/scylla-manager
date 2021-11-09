// Copyright (C) 2017 ScyllaDB

package managerclient

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const rfc822WithSec = "02 Jan 06 15:04:05 MST"

// TaskSplit attempts to split a string into type and id.
func TaskSplit(s string) (taskType string, taskID uuid.UUID, err error) {
	i := strings.LastIndex(s, "/")
	if i != -1 {
		taskType = s[:i]
	}
	taskID, err = uuid.Parse(s[i+1:])
	return
}

// TaskJoin creates a new type id string in the form `taskType/taskId`.
func TaskJoin(taskType string, taskID interface{}) string {
	return fmt.Sprint(taskType, "/", taskID)
}

func uuidFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	_, id := path.Split(l.Path)

	return uuid.Parse(id)
}

// FormatRepairProgress returns string representation of percentage of
// successful and error repair ranges.
func FormatRepairProgress(total, success, failed int64) string {
	if total == 0 {
		return "-"
	}
	out := fmt.Sprintf("%.f%%",
		float64(success)*100/float64(total),
	)
	if failed > 0 {
		out += fmt.Sprintf("/%.f%%", float64(failed)*100/float64(total))
	}
	return out
}

// FormatUploadProgress calculates percentage of success and failed uploads.
func FormatUploadProgress(size, uploaded, skipped, failed int64) string {
	if size == 0 {
		return "100%"
	}
	transferred := uploaded + skipped
	out := fmt.Sprintf("%d%%",
		transferred*100/size,
	)
	if failed > 0 {
		out += fmt.Sprintf("/%d%%", failed*100/size)
	}
	return out
}

// StringByteCount returns string representation of the byte count with proper
// unit.
func StringByteCount(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	// No decimals by default, two decimals for GiB and three for more than
	// that.
	format := "%.0f%ciB"
	if exp == 2 {
		format = "%.2f%ciB"
	} else if exp > 2 {
		format = "%.3f%ciB"
	}
	return fmt.Sprintf(format, float64(b)/float64(div), "KMGTPE"[exp])
}

var (
	byteCountRe          = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)(B|[KMGTPE]iB)`)
	byteCountReValueIdx  = 1
	byteCountReSuffixIdx = 2
)

// ParseByteCount returns byte count parsed from input string.
// This is opposite of StringByteCount function.
func ParseByteCount(s string) (int64, error) {
	const unit = 1024
	exps := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	parts := byteCountRe.FindStringSubmatch(s)
	if len(parts) != 3 {
		return 0, errors.Errorf("invalid byte size string: %q; it must be real number with unit suffix: %s", s, strings.Join(exps, ","))
	}

	v, err := strconv.ParseFloat(parts[byteCountReValueIdx], 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing value for byte size string: %s", s)
	}

	pow := 0
	for i, e := range exps {
		if e == parts[byteCountReSuffixIdx] {
			pow = i
		}
	}

	mul := math.Pow(unit, float64(pow))

	return int64(v * mul), nil
}

// FormatTime formats the supplied DateTime in `02 Jan 06 15:04:05 MST` format.
func FormatTime(t strfmt.DateTime) string {
	if isZero(t) {
		return ""
	}
	return time.Time(t).Local().Format(rfc822WithSec)
}

// FormatTimePointer see FormatTime.
func FormatTimePointer(t *strfmt.DateTime) string {
	var tf strfmt.DateTime
	if t != nil {
		tf = *t
	}
	return FormatTime(tf)
}

// FormatDuration creates and formats the duration between
// the supplied DateTime values.
// If t1 is zero it will default to current time.
func FormatDuration(t0, t1 strfmt.DateTime) string {
	if isZero(t0) && isZero(t1) {
		return "0s"
	}
	var d time.Duration
	if isZero(t1) {
		d = timeutc.Now().Sub(time.Time(t0))
	} else {
		d = time.Time(t1).Sub(time.Time(t0))
	}

	return fmt.Sprint(d.Truncate(time.Second))
}

// FormatMsDuration returns string representation of duration as number of
// milliseconds.
func FormatMsDuration(d int64) string {
	return fmt.Sprint((time.Duration(d) * time.Millisecond).Truncate(time.Second))
}

func isZero(t strfmt.DateTime) bool {
	return time.Time(t).IsZero()
}

// FormatTables returns tables listing if number of tables is lower than
// threshold. It prints (n tables) or (table_a, table_b, ...).
func FormatTables(threshold int, tables []string, all bool) string {
	var out string
	if len(tables) == 0 || threshold == 0 || (threshold > 0 && len(tables) > threshold) {
		if len(tables) == 1 {
			out = "(1 table)"
		} else {
			out = fmt.Sprintf("(%d tables)", len(tables))
		}
	}
	if out == "" {
		out = "(" + strings.Join(tables, ", ") + ")"
	}
	if all {
		out = "all " + out
	}
	return out
}

// FormatClusterName writes cluster name and id to the writer.
func FormatClusterName(w io.Writer, c *Cluster) {
	fmt.Fprintf(w, "Cluster: %s (%s)\n", c.Name, c.ID)
}

// FormatIntensity returns text representation of repair intensity.
func FormatIntensity(v float64) string {
	if v == -1 {
		return "max"
	}
	if math.Floor(v) == v {
		return fmt.Sprintf("%d", int(v))
	}
	return fmt.Sprintf("%.2f", v)
}
