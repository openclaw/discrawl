package cli

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/openclaw/discrawl/internal/config"
)

const (
	lockedMinFreeKiBEnv  = "DISCRAWL_LOCKED_MIN_FREE_KIB"
	lockedMaxWALBytesEnv = "DISCRAWL_LOCKED_MAX_WAL_BYTES"
)

func lockedResourceGuard(rawDBPath string) error {
	rawMinFree := strings.TrimSpace(os.Getenv(lockedMinFreeKiBEnv))
	rawMaxWAL := strings.TrimSpace(os.Getenv(lockedMaxWALBytesEnv))
	if rawMinFree == "" && rawMaxWAL == "" {
		return nil
	}
	if rawMinFree == "" || rawMaxWAL == "" {
		return errors.New("both locked resource guard thresholds are required")
	}
	minFreeKiB, err := strconv.ParseUint(rawMinFree, 10, 64)
	if err != nil || minFreeKiB == 0 {
		return fmt.Errorf("invalid %s value %q", lockedMinFreeKiBEnv, rawMinFree)
	}
	maxWALBytes, err := strconv.ParseUint(rawMaxWAL, 10, 64)
	if err != nil || maxWALBytes == 0 {
		return fmt.Errorf("invalid %s value %q", lockedMaxWALBytesEnv, rawMaxWAL)
	}
	dbPath, err := config.ExpandPath(rawDBPath)
	if err != nil {
		return fmt.Errorf("expand database path: %w", err)
	}
	freeKiB, err := lockedFreeKiB(dbPath)
	if err != nil {
		return fmt.Errorf("probe free space: %w", err)
	}
	walBytes, err := lockedWALBytes(dbPath + "-wal")
	if err != nil {
		return fmt.Errorf("probe WAL: %w", err)
	}
	if freeKiB <= minFreeKiB {
		return fmt.Errorf(
			"free space at or below threshold: free_kib=%d threshold_kib=%d",
			freeKiB,
			minFreeKiB,
		)
	}
	if walBytes >= maxWALBytes {
		return fmt.Errorf(
			"WAL at or above threshold: wal_bytes=%d threshold_bytes=%d",
			walBytes,
			maxWALBytes,
		)
	}
	return nil
}

func lockedWALBytes(path string) (uint64, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return 0, fmt.Errorf("WAL path is not a regular file: %s", path)
	}
	return uint64(info.Size()), nil
}
