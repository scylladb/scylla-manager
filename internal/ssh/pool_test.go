// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"time"

	"golang.org/x/crypto/ssh"
)

// Inspect returns poolConn content for testing purposes.
func (p *Pool) Inspect(network, addr string) (client *ssh.Client, lastUse time.Time, refCount uint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.conns[key(network, addr)]
	if v == nil {
		return nil, time.Time{}, 0
	}
	return v.client, v.lastUse, v.refCount
}

// SetLastUseTime sets poolConn lastUse for testing purposes.
func (p *Pool) SetLastUseTime(network, addr string, lastUse time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.conns[key(network, addr)]
	if v != nil {
		v.lastUse = lastUse
	}
}
