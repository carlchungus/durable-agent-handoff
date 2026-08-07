//go:build windows

package privatepath

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func validatePlatform(file *os.File, _ os.FileInfo, _ bool) error {
	descriptor, err := windows.GetSecurityInfo(windows.Handle(file.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("read Windows security descriptor: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return errors.New("Windows path owner is unavailable")
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return errors.New("current Windows user identity is unavailable")
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return err
	}
	allowed := []*windows.SID{user.User.Sid, system, administrators}
	if !sidAllowed(owner, allowed) {
		return errors.New("Windows path is not owned by the current user or a trusted system principal")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return errors.New("Windows path has no restrictive DACL")
	}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err = windows.GetAce(dacl, index, &ace); err != nil || ace == nil {
			return fmt.Errorf("read Windows access entry %d: %w", index, err)
		}
		if ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 || ace.Header.AceType == windows.ACCESS_DENIED_ACE_TYPE {
			continue
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return fmt.Errorf("Windows path has unsupported allow entry type %d", ace.Header.AceType)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sidAllowed(sid, allowed) {
			return fmt.Errorf("Windows path grants access to untrusted principal %s", sid.String())
		}
	}
	return nil
}

func sidAllowed(candidate *windows.SID, allowed []*windows.SID) bool {
	if candidate == nil {
		return false
	}
	for _, trusted := range allowed {
		if trusted != nil && candidate.Equals(trusted) {
			return true
		}
	}
	return false
}
