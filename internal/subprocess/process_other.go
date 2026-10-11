//go:build !unix

package subprocess

import (
	"os"
	"os/exec"
)

func SetProcessGroup(*exec.Cmd) {}

func KillProcessGroup(p *os.Process) {
	_ = p.Kill()
}
