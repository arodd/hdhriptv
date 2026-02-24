//go:build windows

package stream

import (
	"os/exec"
	"strconv"
	"syscall"
)

func configureFFmpegCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	// Isolate the ffmpeg command in its own process group for best-effort
	// process-tree termination when wrappers spawn child processes.
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func terminateFFmpegCommand(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// Best effort process-tree termination for wrapper-script child processes.
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
	// Always attempt direct process termination as a fallback.
	_ = cmd.Process.Kill()
}
