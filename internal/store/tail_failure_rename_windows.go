//go:build windows

package store

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type tailFailureWindowsRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [windows.MAX_LONG_PATH]uint16
}

func renameTailFailureNoReplace(_ *os.Root, dir *os.File, oldName, newName string) error {
	source, err := openTailFailureWindowsFile(
		dir,
		oldName,
		windows.DELETE|windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE,
		windows.FILE_WRITE_THROUGH,
	)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(source) }()

	newNameUTF16, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	if len(newNameUTF16) > len(tailFailureWindowsRenameInformation{}.FileName) {
		return windows.ERROR_FILENAME_EXCED_RANGE
	}
	info := tailFailureWindowsRenameInformation{
		RootDirectory:  windows.Handle(dir.Fd()),
		FileNameLength: uint32((len(newNameUTF16) - 1) * 2),
	}
	copy(info.FileName[:], newNameUTF16)
	err = windows.NtSetInformationFile(
		source,
		&windows.IO_STATUS_BLOCK{},
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Offsetof(info.FileName))+info.FileNameLength,
		windows.FileRenameInformation,
	)
	if err != nil {
		return windowsNTStatusError(err)
	}
	return windows.FlushFileBuffers(source)
}

func windowsNTStatusError(err error) error {
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}
