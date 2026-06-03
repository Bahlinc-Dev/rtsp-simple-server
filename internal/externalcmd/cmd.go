// Package externalcmd allows launching external commands
package externalcmd

import (
    "context"
    "errors"
    "fmt"
    "os"
    "sync"
    "time"
)

const (
    restartPause = 5 * time.Second
)

var (
    // errTerminated is returned when the command is terminated
    errTerminated = errors.New("terminated")
)

// OnExitFunc is the prototype of onExit
type OnExitFunc func(error)

// Environment is a Cmd environment
type Environment map[string]string

// Cmd is an external command
type Cmd struct {
    pool      *Pool
    cmdstr    string
    restart   bool
    env       Environment
    onExit    func(error)
    terminate chan struct{}
    ctx       context.Context
    cancel    context.CancelFunc
    mu        sync.Mutex
    running   bool
}

// NewCmd allocates a Cmd
func NewCmd(
    pool *Pool,
    cmdstr string,
    restart bool,
    env Environment,
    onExit OnExitFunc,
) *Cmd {
    // Create a context that we can use to manage the lifecycle
    ctx, cancel := context.WithCancel(context.Background())
    
    // Expand environment variables
    cmdstr = os.Expand(cmdstr, func(variable string) string {
        if value, ok := env[variable]; ok {
            return value
        }
        return os.Getenv(variable)
    })
    
    if onExit == nil {
        onExit = func(_ error) {}
    }
    
    e := &Cmd{
        pool:      pool,
        cmdstr:    cmdstr,
        restart:   restart,
        env:       env,
        onExit:    onExit,
        terminate: make(chan struct{}),
        ctx:       ctx,
        cancel:    cancel,
        running:   true,
    }
    
    pool.wg.Add(1)
    go e.run()
    return e
}

// Close closes the command. It doesn't wait for the command to exit.
func (e *Cmd) Close() {
    e.mu.Lock()
    if !e.running {
        e.mu.Unlock()
        return
    }
    e.running = false
    e.mu.Unlock()
    
    e.cancel()  // Cancel the context
    close(e.terminate)
}

func (e *Cmd) run() {
    defer e.pool.wg.Done()
    defer e.cancel()  // Ensure context is cancelled when we exit
    
    env := append([]string(nil), os.Environ()...)
    for key, val := range e.env {
        env = append(env, key+"="+val)
    }
    
    for {
        select {
        case <-e.ctx.Done():
            return
        default:
            err := e.runOSSpecific(env)
            if errors.Is(err, errTerminated) {
                return
            }
            
            if !e.restart {
                if err != nil {
                    e.onExit(err)
                }
                return
            }
            
            if err != nil {
                e.onExit(err)
            } else {
                e.onExit(fmt.Errorf("command exited with code 0"))
            }
            
            select {
            case <-time.After(restartPause):
            case <-e.terminate:
                return
            case <-e.ctx.Done():
                return
            }
        }
    }
}