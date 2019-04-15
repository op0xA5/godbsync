package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

func NewTLSConfig(config *config) (*tls.Config, error) {
	caCert, err := ioutil.ReadFile(config.ClientCA)
	if err != nil {
		return nil, fmt.Errorf("failed load ClientCA: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair(config.Cert, config.CertKey)
	if err != nil {
		return nil, fmt.Errorf("failed load Cert: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		},
		MinVersion: tls.VersionTLS12,
	}, nil
}
