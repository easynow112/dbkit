package test

import (
	"context"
	"fmt"
	"sync"
)

type lockState struct {
	mu    sync.Mutex
	owner *Connection
	count int
}

var ls = lockState{
	count: 0,
	owner: nil,
}

type Lock struct {
	state *lockState
	conn  *Connection
}

func (lock *Lock) Release(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	lock.state.mu.Lock()
	defer lock.state.mu.Unlock()

	if lock.state.owner != lock.conn {
		return fmt.Errorf("lock is not owned by this connection")
	}

	if lock.state.count <= 1 {
		lock.state.owner = nil
		lock.state.count = 0
	} else {
		lock.state.count--
	}
	return nil
}
