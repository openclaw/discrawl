package main

import (
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func configureShutdownChild(cmd *exec.Cmd) {
	// Keep the console event inside the fixture process, away from the CI runner.
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_CONSOLE, HideWindow: true}
}

func sendShutdownSignal() error {
	return windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, 0)
}
