//go:build linux

package runstate

import (
	"os"
	"strconv"
	"strings"
)

func platformProcessStartToken(pid int) string {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return ""
	}
	// Fields after the command start at proc(5) field 3. Start time is field 22.
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) <= 19 {
		return ""
	}
	boot, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(boot)) + ":" + fields[19]
}
