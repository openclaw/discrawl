package config

import (
	"encoding/binary"
	"unsafe"

	"golang.org/x/sys/unix"
)

func credentialDirectoryFlags() int {
	// SDK sys/fcntl.h: O_SEARCH = O_EXEC | O_DIRECTORY. A search-only
	// descriptor also supports fstat/fgetattrlist on execute-only ancestors.
	return 0x40000000 | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
}

func credentialACLProtected(fd int) bool {
	attrs := unix.Attrlist{Bitmapcount: unix.ATTR_BIT_MAP_COUNT, Commonattr: unix.ATTR_CMN_EXTENDED_SECURITY}
	buf := make([]byte, 4096) // 128 ACEs, the SDK maximum, fit within this bound.
	// x/sys has no libSystem fgetattrlist wrapper. The descriptor-relative call
	// keeps the ACL check bound to the inode whose owner/mode were checked.
	//lint:ignore SA1019 no non-deprecated descriptor-relative wrapper exists.
	//nolint:staticcheck // SA1019: no non-deprecated descriptor-relative wrapper exists.
	_, _, errno := unix.Syscall6(unix.SYS_FGETATTRLIST, uintptr(fd), uintptr(unsafe.Pointer(&attrs)), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), 0, 0) // #nosec G103 -- fixed buffers sized for the kernel ABI.
	return errno == 0 && credentialDarwinACLProtected(buf)
}

func credentialDarwinACLProtected(buf []byte) bool {
	// getattrlist's packed attrreference is relative to its own offset (4).
	// The referenced kauth_filesec is defined by SDK sys/kauth.h. Both
	// supported macOS architectures use little-endian native fields.
	if len(buf) < 12 {
		return false
	}
	u32 := binary.LittleEndian.Uint32
	length, offset, size := uint64(u32(buf[:4])), uint64(u32(buf[4:8])), uint64(u32(buf[8:12]))
	if length < 12 || length > uint64(len(buf)) || offset != 8 || length != 12+size {
		return false
	}
	if size == 0 {
		return true
	}
	data := buf[4+offset : 4+offset+size]
	if len(data) < 44 || u32(data[:4]) != 0x012cc16d {
		return false
	}
	count := uint64(u32(data[36:40]))
	if count == 0xffffffff { // KAUTH_FILESEC_NOACL
		return len(data) == 44
	}
	if count > 128 || 44+24*count != uint64(len(data)) {
		return false
	}
	for i := range count {
		flags := u32(data[44+24*i+16 : 44+24*i+20])
		// Deny entries only reduce access. Reject all granting/unknown ACEs,
		// including inherited grants, rather than guessing principal rights.
		if flags&0xf != 2 {
			return false
		}
	}
	return true
}
