//go:build !unix

package codex

import (
	"os"
	"os/exec"
)

func setProcessGroup(*exec.Cmd) {}

func killProcessGroup(p *os.Process) {
	_ = p.Kill()
}
