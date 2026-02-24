//go:build !windows

package stream

import (
	"os/exec"
	"syscall"
)

func configureFFmpegCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateFFmpegCommand(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// Kill the entire process group (negative PID) so wrapper-script children
	// that inherited pipe FDs are terminated together with the parent.
	// This prevents cmd.Wait() from blocking on pipe-copy goroutines when
	// orphaned children hold stdout/stderr FDs open.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	// Also kill the direct process in case Setpgid was not effective.
	_ = cmd.Process.Kill()
}
