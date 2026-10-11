//go:build unix

package subprocess

import (
	"os"
	"os/exec"
	"syscall"
)

// A dedicated process group lets close reach commands the process spawned.
func SetProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func KillProcessGroup(p *os.Process) {
	_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
}
