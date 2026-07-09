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
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// TLSOptions configures a TLS connection to the broker.
type TLSOptions struct {
	// CAFile is a PEM file of CA certificate(s) to trust (e.g. the broker's
	// generated <data_dir>/tls/ca.pem). Empty uses the OS trust store.
	CAFile string
	// CAFingerprint pins the broker certificate by its SHA-256 fingerprint (hex,
	// colons optional). Pinning replaces the CA trust store: the broker is
	// trusted only when the pinned certificate is its leaf, or is a CA that
	// genuinely signed the presented leaf (so a CA pin survives leaf rotation).
	// Hostname verification is skipped because the pin is the trust root. Takes
	// precedence over CAFile.
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
			if errors.Is(err, syscall.ECONNREFUSED) {
				return nil, &DisconnectionError{Message: "connection refused by " + addr +
					". Is the broker running and reachable there? Clients connect to the broker port " +
					"(default 9876), not the admin API or dashboard port (default 8081)"}
			}
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
		// The pin replaces the CA trust store. Hostname verification is skipped
		// because the pin, not a name, is the trust root; the handshake still
		// proves possession of the leaf key.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			return verifyPinnedChain(cs.PeerCertificates, pin)
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

// verifyPinnedChain enforces a SHA-256 certificate pin against the presented
// chain. A fingerprint can pin either the leaf or an issuer:
//
//   - Leaf pin: the pin matches the presented leaf. The handshake already
//     proved possession of the leaf's key, so the match stands on its own.
//   - CA pin: the pin matches an issuer (which lets the leaf rotate under the
//     same CA). The pinned certificate merely appearing in the chain proves
//     nothing, because the real CA certificate is public and a man-in-the-middle
//     can staple it beside a rogue leaf it controls. So the leaf is
//     path-validated against the pinned certificate as the sole trust root, and
//     accepted only if it is genuinely signed by it.
func verifyPinnedChain(chain []*x509.Certificate, pin []byte) error {
	if len(chain) == 0 {
		return &TlsCertificateUntrustedError{Detail: "the broker presented no certificate"}
	}
	leaf := chain[0]
	if sum := sha256.Sum256(leaf.Raw); bytes.Equal(sum[:], pin) {
		return nil
	}
	for _, cert := range chain[1:] {
		if sum := sha256.Sum256(cert.Raw); !bytes.Equal(sum[:], pin) {
			continue
		}
		roots := x509.NewCertPool()
		roots.AddCert(cert)
		intermediates := x509.NewCertPool()
		for _, inter := range chain[1:] {
			intermediates.AddCert(inter)
		}
		// No DNSName: the pin is the trust root, so hostname is not verified.
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates}); err != nil {
			return &TlsCertificateUntrustedError{
				Detail: "the presented leaf is not signed by the pinned CA certificate: " + err.Error(),
			}
		}
		return nil
	}
	return &TlsCertificateUntrustedError{Detail: "no certificate in the presented chain matches the pinned fingerprint"}
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
	// A handshake that ends early, or non-TLS bytes where the ServerHello should
	// be, means the broker listener is almost certainly speaking plaintext.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "first record does not look like a TLS handshake") {
		return &TlsNotSupportedByBrokerError{Addr: addr}
	}
	return &TlsHandshakeError{Message: "TLS handshake with " + addr + " failed: " + err.Error()}
}

// clientCertRequiredError attributes a HELLO-time TLS death to a missing client
// certificate. Over TLS with no client cert configured, a broker that requires
// one closes the connection with a certificate-required alert, which under TLS
// 1.3 arrives after the handshake completes and can flatten to a bare EOF as the
// client reads the HELLO reply. Returns nil when this does not apply, so the
// caller keeps its original error.
func clientCertRequiredError(tlsOpts *TLSOptions, err error) *TlsClientCertificateRequiredError {
	if tlsOpts == nil || tlsOpts.ClientCertFile != "" || err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "certificate required") ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &TlsClientCertificateRequiredError{}
	}
	return nil
}
