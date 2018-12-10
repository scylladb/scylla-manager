// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

// SSH configuration defaults.
var (
	DefaultPort                = 22
	DefaultServerAliveInterval = 15 * time.Second
	DefaultServerAliveCountMax = 3
)

// Config specifies SSH configuration.
type Config struct {
	ssh.ClientConfig
	// Port specifies the port number to connect on the remote host.
	Port int
	// ServerAliveInterval sets an interval in seconds ssh will send a message
	// through the encrypted channel to request a response from the server.
	ServerAliveInterval time.Duration
	// ServerAliveCountMax sets the number of server alive messages which may be
	// sent without receiving any messages back from the server. If this
	// threshold is reached while server alive messages are being sent, ssh will
	// disconnect from the server, terminating the session.
	ServerAliveCountMax int
}

func defaultConfig() Config {
	return Config{
		Port:                DefaultPort,
		ServerAliveInterval: DefaultServerAliveInterval,
		ServerAliveCountMax: DefaultServerAliveCountMax,
	}
}

// NewProductionConfig returns configuration with a key based authentication.
func NewProductionConfig(user string, identityFile []byte) (Config, error) {
	if user == "" {
		return Config{}, errors.New("missing user")
	}

	auth, err := keyPairAuthMethod(identityFile)
	if err != nil {
		return Config{}, errors.Wrap(err, "failed to parse identity file")
	}

	config := defaultConfig()
	config.User = user
	config.Auth = []ssh.AuthMethod{auth}
	config.HostKeyCallback = ssh.InsecureIgnoreHostKey()

	return config, nil
}

func keyPairAuthMethod(pemBytes []byte) (ssh.AuthMethod, error) {
	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, err
	}

	return ssh.PublicKeys(signer), nil
}

// NewDevelopmentConfig returns configuration with a password based authentication.
func NewDevelopmentConfig() Config {
	config := defaultConfig()
	config.User = "root"
	config.Auth = []ssh.AuthMethod{ssh.Password("root")}
	config.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	return config
}
