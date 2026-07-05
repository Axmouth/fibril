package fibril

// TLS connection setup: build a *tls.Config from the client's trust settings and
// dial. Trust resolution order: a fingerprint pin if set, else a CA file, else
// the OS roots. An optional client certificate enables mTLS.

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"strings"
)

// TLSOptions configures a TLS connection to the broker.
type TLSOptions struct {
	// CAFile is a PEM file of CA certificate(s) to trust (e.g. the broker's
	// generated <data_dir>/tls/ca.pem). Empty uses the OS trust store.
	CAFile string
	// CAFingerprint pins the broker certificate by its SHA-256 fingerprint (hex,
	// colons optional). Pinning replaces chain verification; the handshake still
	// proves possession of the key. Takes precedence over CAFile.
	CAFingerprint string
	// ServerName is the name verified against the certificate and sent as SNI.
	// Empty defaults to the dial host.
	ServerName string
	// ClientCertFile and ClientKeyFile present a client certificate for
	// tls.client_auth deployments (mTLS). Set both or neither.
	ClientCertFile string
	ClientKeyFile  string
}

func dial(addr string, opts *TLSOptions) (net.Conn, error) {
	if opts == nil {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, &DisconnectionError{Message: "failed to connect to " + addr + ": " + err.Error()}
		}
		return conn, nil
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	cfg, err := buildTLSConfig(opts, host)
	if err != nil {
		return nil, err
	}
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, classifyTLSDialError(err, addr)
	}
	return conn, nil
}

func buildTLSConfig(opts *TLSOptions, host string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: opts.ServerName}
	if cfg.ServerName == "" {
		cfg.ServerName = host
	}

	switch {
	case opts.CAFingerprint != "":
		pin, err := parseFingerprint(opts.CAFingerprint)
		if err != nil {
			return nil, err
		}
		// The pin replaces chain-of-trust verification; the match happens after
		// the handshake, which still proves the certificate key is held.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			for _, cert := range cs.PeerCertificates {
				sum := sha256.Sum256(cert.Raw)
				if bytes.Equal(sum[:], pin) {
					return nil
				}
			}
			return &TlsCertificateUntrustedError{Detail: "no certificate in the presented chain matches the pinned fingerprint"}
		}
	case opts.CAFile != "":
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, &TlsConfigError{Message: "failed to read tls CAFile " + opts.CAFile + ": " + err.Error()}
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, &TlsConfigError{Message: "tls CAFile " + opts.CAFile + " contains no usable certificates"}
		}
		cfg.RootCAs = pool
	}

	if (opts.ClientCertFile == "") != (opts.ClientKeyFile == "") {
		return nil, &TlsConfigError{Message: "client certificate options must be set together: both the certificate and its key"}
	}
	if opts.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.ClientCertFile, opts.ClientKeyFile)
		if err != nil {
			return nil, &TlsConfigError{Message: "failed to load tls client certificate: " + err.Error()}
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func parseFingerprint(raw string) ([]byte, error) {
	hexStr := strings.NewReplacer(":", "", " ", "").Replace(strings.ToLower(raw))
	b, err := hex.DecodeString(hexStr)
	if err != nil || len(b) != sha256.Size {
		return nil, &TlsConfigError{Message: "tls CAFingerprint must be 64 hex digits (SHA-256, colons optional)"}
	}
	return b, nil
}

func classifyTLSDialError(err error, addr string) error {
	var untrusted *TlsCertificateUntrustedError
	if errors.As(err, &untrusted) {
		return untrusted // from our pin check
	}
	var unknownAuthority x509.UnknownAuthorityError
	var invalidCert x509.CertificateInvalidError
	var hostnameErr x509.HostnameError
	if errors.As(err, &unknownAuthority) || errors.As(err, &invalidCert) || errors.As(err, &hostnameErr) {
		return &TlsCertificateUntrustedError{Detail: err.Error()}
	}
	return &TlsHandshakeError{Message: "TLS handshake with " + addr + " failed: " + err.Error()}
}
