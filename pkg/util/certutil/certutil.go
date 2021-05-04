// Copyright (C) 2017 ScyllaDB

package certutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

// GenerateSelfSignedCertificate generates a P256 certificate that can be used
// to start an HTTPS server.
// The certificate is valid for a year after it's generated.
func GenerateSelfSignedCertificate(hosts []string) (tls.Certificate, error) {
	var cert tls.Certificate

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return cert, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return cert, errors.Wrap(err, "generate serial number")
	}

	now := timeutc.Now()

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Scylla"},
		},
		NotBefore:             now,
		NotAfter:              now.AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	for i := range hosts {
		h, _, err := net.SplitHostPort(hosts[i])
		if err != nil {
			h = hosts[i]
		}
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return cert, errors.Wrap(err, "generate certificate")
	}

	cert.Certificate = [][]byte{derBytes}
	cert.PrivateKey = priv

	return cert, nil
}
