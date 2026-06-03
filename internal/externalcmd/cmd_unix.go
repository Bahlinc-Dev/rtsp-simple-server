//go:build !windows

package externalcmd

import (
    "os/exec"
    "syscall"
    "time"
)

func (e *Cmd) runOSSpecific(env []string) error {
    cmd := exec.Command("sh", "-c", e.cmdstr)
    cmd.Env = env
    
    err := cmd.Start()
    if err != nil {
        return err
    }
    
    done := make(chan error)
    go func() {
        done <- cmd.Wait()
    }()
    
    select {
    case err := <-done:
        return err
        
    case <-e.terminate:
        cmd.Process.Signal(syscall.SIGTERM)
        select {
        case <-done:
        case <-time.After(5 * time.Second):
            cmd.Process.Kill()
            <-done
        }
        return errTerminated
    }
}