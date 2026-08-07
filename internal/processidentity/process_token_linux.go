//go:build linux

package processidentity

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

func platformProcessStartToken(pid int) (string, bool, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", false, errors.New("malformed process stat record")
	}
	// Fields after the command start at proc(5) field 3. Start time is field 22.
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) <= 19 {
		return "", false, errors.New("process stat record has no start time")
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", false, err
	}
	bootID := strings.TrimSpace(string(boot))
	if bootID == "" {
		return "", false, errors.New("kernel boot identity is empty")
	}
	return bootID + ":" + fields[19], true, nil
}
