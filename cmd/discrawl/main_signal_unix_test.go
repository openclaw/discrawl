//go:build !windows

package main

import (
	"os"
	"os/exec"
	"syscall"
)

func configureShutdownChild(*exec.Cmd) {}

func sendShutdownSignal() error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Signal(syscall.SIGTERM)
}
