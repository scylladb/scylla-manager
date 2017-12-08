// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

const poolGCInterval = 30 * time.Second

type poolConn struct {
	client   *ssh.Client
	lastUse  time.Time
	refCount uint
}

// DefaultPool is the default instance of Pool it uses DefaultDialer for
// creating the SSH connections.
var DefaultPool = NewPool(DefaultDialer, 0)

// Pool is an SSH connection pool.
type Pool struct {
	dialer      net.Dialer
	idleTimeout time.Duration

	conns map[string]*poolConn // key is network/host:port
	mu    sync.Mutex
	done  chan struct{}
}

func key(network, addr string) string {
	return fmt.Sprint(network, '/', addr)
}

// NewPool creates a new Pool. If idleTimeout > 0 a GC routine is started
// otherwise the SSH client is closed as soon as it's released the last holder.
func NewPool(dialer net.Dialer, idleTimeout time.Duration) *Pool {
	pool := &Pool{
		dialer:      dialer,
		idleTimeout: idleTimeout,

		conns: make(map[string]*poolConn),
		done:  make(chan struct{}),
	}

	if idleTimeout > 0 {
		go pool.loopGC()
	}

	return pool
}

// Dial is a cached ssh.Dial function, the returned client must be relased
// using the Release method.
func (p *Pool) Dial(network, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// cache hit
	if c, ok := p.conns[key(network, addr)]; ok {
		c.lastUse = time.Now()
		c.refCount++
		return c.client, nil
	}

	// cache miss
	client, err := p.dialLocked(network, addr, config)
	if err != nil {
		return nil, err
	}
	p.conns[key(network, addr)] = &poolConn{
		client:   client,
		lastUse:  time.Now(),
		refCount: 1,
	}

	return client, nil
}

func (p *Pool) dialLocked(network, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
	netConn, err := p.dialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(netConn, addr, config)
	if err != nil {
		return nil, err
	}

	return ssh.NewClient(sshConn, chans, reqs), nil
}

// Release marks client as not used.
func (p *Pool) Release(client *ssh.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, conn := range p.conns {
		if conn.client == client {
			conn.refCount--
			if conn.refCount == 0 && p.idleTimeout <= 0 {
				p.releaseLocked(key, conn)
			}
			break
		}
	}
}

func (p *Pool) releaseLocked(key string, conn *poolConn) {
	conn.client.Close()
	delete(p.conns, key)
}

func (p *Pool) loopGC() {
	t := time.NewTicker(poolGCInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.GC()
		case <-p.done:
			return
		}
	}
}

// GC closes not used clients that are idle for more than defaultIdleTimeout.
func (p *Pool) GC() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for key, conn := range p.conns {
		if conn.refCount == 0 && conn.lastUse.Add(p.idleTimeout).Before(now) {
			p.releaseLocked(key, conn)
		}
	}
}

// Close closes all the clients in the pool.
func (p *Pool) Close() error {
	close(p.done)
	return p.close()
}

func (p *Pool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	for _, conn := range p.conns {
		multierr.Append(err, conn.client.Close())
	}
	p.conns = nil

	return err
}
