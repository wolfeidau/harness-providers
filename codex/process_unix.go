//go:build unix

package codex

import (
	"os"
	"os/exec"
	"syscall"
)

// A dedicated process group lets close reach commands Codex spawned.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(p *os.Process) {
	_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
}
