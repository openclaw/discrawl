//go:build windows

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const windowsFileAllAccess windows.ACCESS_MASK = 0x001f01ff

func createTailFailureFallbackDir(path string) error {
	descriptor, err := tailFailureWindowsSecurityDescriptor()
	if err != nil {
		return err
	}
	path, err = tailFailureExtendedWindowsPath(path)
	if err != nil {
		return err
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes := &windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: descriptor,
	}
	return windows.CreateDirectory(pathPtr, attributes)
}

func validateTailFailureFallbackDir(file *os.File, _ os.FileInfo) error {
	return validateTailFailureWindowsACL(file, true)
}

func validateTailFailureFallbackFile(file *os.File, _ os.FileInfo) error {
	return validateTailFailureWindowsACL(file, false)
}

func secureTailFailureFallbackTempFile(dir *os.File, name string) error {
	userSID, err := tailFailureWindowsUserSID()
	if err != nil {
		return err
	}
	handle, err := openTailFailureWindowsFile(
		dir,
		name,
		windows.WRITE_OWNER|windows.READ_CONTROL|windows.SYNCHRONIZE,
		windows.FILE_WRITE_THROUGH,
	)
	if err != nil {
		return fmt.Errorf("open private Windows fallback owner handle: %w", err)
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	if err := windows.SetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
		userSID,
		nil,
		nil,
		nil,
	); err != nil {
		return fmt.Errorf("set private Windows fallback owner: %w", err)
	}
	return nil
}

func openTailFailureWindowsFile(dir *os.File, name string, access uint32, options uint32) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: windows.Handle(dir.Fd()),
		ObjectName:    objectName,
	}
	var handle windows.Handle
	err = windows.NtCreateFile(
		&handle,
		access,
		attributes,
		&windows.IO_STATUS_BLOCK{},
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_OPEN_REPARSE_POINT|
			windows.FILE_NON_DIRECTORY_FILE|
			windows.FILE_SYNCHRONOUS_IO_NONALERT|
			options,
		0,
		0,
	)
	if err != nil {
		return 0, windowsNTStatusError(err)
	}
	return handle, nil
}

func syncTailFailureDirectory(_ *os.File) error {
	// Windows commits the fallback rename with MOVEFILE_WRITE_THROUGH. Receipt
	// rows make later artifact cleanup idempotent across a restart.
	return nil
}

func normalizeTailFailureFileURLPath(path string) string {
	path = filepath.FromSlash(path)
	if len(path) >= 3 && os.IsPathSeparator(path[0]) && path[2] == ':' {
		path = path[1:]
	}
	return path
}

func tailFailureWindowsUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("read current Windows user: %w", err)
	}
	if user == nil || user.User.Sid == nil {
		return nil, errors.New("current Windows user SID is unavailable")
	}
	return user.User.Sid, nil
}

func tailFailureWindowsSecurityDescriptor() (*windows.SECURITY_DESCRIPTOR, error) {
	userSID, err := tailFailureWindowsUserSID()
	if err != nil {
		return nil, err
	}
	sddl := "O:" + userSID.String() +
		"D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;" + userSID.String() + ")"
	descriptor, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return nil, fmt.Errorf("build private Windows fallback ACL: %w", err)
	}
	return descriptor, nil
}

func validateTailFailureWindowsACL(file *os.File, requireInheritance bool) error {
	userSID, err := tailFailureWindowsUserSID()
	if err != nil {
		return err
	}
	systemSID, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return fmt.Errorf("build Windows LocalSystem SID: %w", err)
	}
	descriptor, err := windows.GetSecurityInfo(
		windows.Handle(file.Fd()),
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("inspect Windows fallback ACL: %w", err)
	}
	if descriptor == nil {
		return errors.New("Windows fallback ACL is unavailable")
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil || !owner.Equals(userSID) {
		return errors.New("Windows fallback owner must be the current user")
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return fmt.Errorf("inspect Windows fallback ACL control: %w", err)
	}
	if requireInheritance && control&windows.SE_DACL_PROTECTED == 0 {
		return errors.New("Windows fallback ACL must disable inherited access")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil || dacl.AceCount != 2 {
		return errors.New("Windows fallback ACL must grant exactly two principals")
	}
	seenUser := false
	seenSystem := false
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			return fmt.Errorf("inspect Windows fallback ACL entry: %w", err)
		}
		if ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return errors.New("Windows fallback ACL contains a non-allow entry")
		}
		if ace.Mask != windowsFileAllAccess && ace.Mask != windows.GENERIC_ALL {
			return errors.New("Windows fallback ACL entry lacks full control")
		}
		if requireInheritance && ace.Header.AceFlags&(windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE) !=
			windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE {
			return errors.New("Windows fallback ACL does not protect child files")
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		switch {
		case sid.Equals(userSID) && !seenUser:
			seenUser = true
		case sid.Equals(systemSID) && !seenSystem:
			seenSystem = true
		default:
			return errors.New("Windows fallback ACL grants an unexpected principal")
		}
	}
	if !seenUser || !seenSystem {
		return errors.New("Windows fallback ACL is missing a required principal")
	}
	return nil
}

func tailFailureExtendedWindowsPath(path string) (string, error) {
	full := filepath.Clean(path)
	if !filepath.IsAbs(full) {
		var err error
		full, err = filepath.Abs(full)
		if err != nil {
			return "", fmt.Errorf("resolve Windows fallback path: %w", err)
		}
	}
	switch {
	case strings.HasPrefix(full, `\\?\`),
		strings.HasPrefix(full, `\??\`),
		strings.HasPrefix(full, `\\.\`):
		return full, nil
	case strings.HasPrefix(full, `\\`):
		return `\\?\UNC\` + strings.TrimPrefix(full, `\\`), nil
	default:
		return `\\?\` + full, nil
	}
}
