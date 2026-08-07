//go:build windows

package processidentity

import (
	"errors"
	"strconv"

	"golang.org/x/sys/windows"
)

func platformProcessStartToken(pid int) (string, bool, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_NOT_FOUND) {
			return "", false, nil
		}
		return "", false, err
	}
	defer windows.CloseHandle(handle)
	var exitCode uint32
	if err = windows.GetExitCodeProcess(handle, &exitCode); err != nil {
		return "", false, err
	}
	if exitCode != 259 { // STILL_ACTIVE
		return "", false, nil
	}
	var created, exited, kernel, user windows.Filetime
	if err = windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return "", false, err
	}
	return strconv.FormatInt(created.Nanoseconds(), 10), true, nil
}
