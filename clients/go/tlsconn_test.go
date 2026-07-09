package fibril

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"
)

// selfSignedCert returns a TLS certificate for 127.0.0.1 and its DER bytes (for
// fingerprinting).
func selfSignedCert(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fibril-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, der
}

func startTLSBroker(t *testing.T, cert tls.Certificate) net.Listener {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveOneConn(conn, 2) // n=2: answers HELLO+PUBLISH, does not drop
		}
	}()
	return ln
}

func TestTLSConnectWithFingerprintPin(t *testing.T) {
	cert, der := selfSignedCert(t)
	sum := sha256.Sum256(der)
	ln := startTLSBroker(t, cert)
	defer ln.Close()

	e, err := Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS:               &TLSOptions{CAFingerprint: hex.EncodeToString(sum[:])},
	})
	if err != nil {
		t.Fatalf("TLS connect with correct pin: %v", err)
	}
	defer e.Shutdown()

	off, err := e.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("publish over TLS: %v", err)
	}
	if off != 2 {
		t.Errorf("offset = %d, want 2", off)
	}
}

func TestTLSFingerprintMismatchIsUntrusted(t *testing.T) {
	cert, _ := selfSignedCert(t)
	ln := startTLSBroker(t, cert)
	defer ln.Close()

	// A pin that does not match the presented certificate.
	wrong := hex.EncodeToString(make([]byte, sha256.Size))
	_, err := Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS:               &TLSOptions{CAFingerprint: wrong},
	})
	if err == nil {
		t.Fatal("expected a certificate-untrusted error on a fingerprint mismatch")
	}
	var untrusted *TlsCertificateUntrustedError
	if !errors.As(err, &untrusted) {
		t.Errorf("error = %T (%v), want *TlsCertificateUntrustedError", err, err)
	}
}

func TestTLSBadFingerprintIsConfigError(t *testing.T) {
	_, err := parseFingerprint("not-hex")
	var cfg *TlsConfigError
	if !errors.As(err, &cfg) {
		t.Errorf("error = %T, want *TlsConfigError", err)
	}
}

func TestTLSAgainstPlaintextBrokerIsNotSupported(t *testing.T) {
	// A plaintext TCP listener that accepts and closes: the TLS handshake ends
	// before completing.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	_, err = Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS:               &TLSOptions{},
	})
	if err == nil {
		t.Fatal("expected an error dialing TLS to a plaintext broker")
	}
	var notSupported *TlsNotSupportedByBrokerError
	if !errors.As(err, &notSupported) {
		t.Errorf("error = %T (%v), want *TlsNotSupportedByBrokerError", err, err)
	}
}

// issueCA creates a self-signed CA certificate and its signing key.
func issueCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "Fibril Pin Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return cert, priv
}

// issueLeaf creates a leaf certificate. A nil issuer makes it self-signed, which
// models a leaf whose key an attacker holds but no real CA signed.
func issueLeaf(t *testing.T, cn string, issuer *x509.Certificate, issuerKey *ecdsa.PrivateKey) *x509.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	parent, signerKey := issuer, issuerKey
	if parent == nil {
		parent, signerKey = &tmpl, priv // self-signed
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, parent, &priv.PublicKey, signerKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return cert
}

func pinOf(c *x509.Certificate) []byte {
	sum := sha256.Sum256(c.Raw)
	return sum[:]
}

// The MITM a CA pin must resist: an attacker presents its own leaf (whose key it
// holds, so the handshake signature is valid) stapled next to the genuine,
// public CA certificate. The CA matches the pin but did not sign the rogue leaf.
func TestVerifyPinnedChainCARejectsRogueLeaf(t *testing.T) {
	ca, caKey := issueCA(t)
	_ = issueLeaf(t, "localhost", ca, caKey) // the real leaf exists; the attacker ignores it
	rogue := issueLeaf(t, "localhost", nil, nil)
	err := verifyPinnedChain([]*x509.Certificate{rogue, ca}, pinOf(ca))
	if err == nil {
		t.Fatal("CA pin must reject a rogue leaf stapled to the real CA certificate")
	}
	var untrusted *TlsCertificateUntrustedError
	if !errors.As(err, &untrusted) {
		t.Fatalf("want TlsCertificateUntrustedError, got %T: %v", err, err)
	}
}

func TestVerifyPinnedChainCAAcceptsSignedLeafAndRotation(t *testing.T) {
	ca, caKey := issueCA(t)
	leaf := issueLeaf(t, "localhost", ca, caKey)
	if err := verifyPinnedChain([]*x509.Certificate{leaf, ca}, pinOf(ca)); err != nil {
		t.Fatalf("CA pin must accept a leaf the pinned CA signed: %v", err)
	}
	rotated := issueLeaf(t, "localhost", ca, caKey)
	if err := verifyPinnedChain([]*x509.Certificate{rotated, ca}, pinOf(ca)); err != nil {
		t.Fatalf("CA pin must survive leaf rotation under the same CA: %v", err)
	}
}

func TestVerifyPinnedChainLeafPinIsExact(t *testing.T) {
	ca, caKey := issueCA(t)
	leaf := issueLeaf(t, "localhost", ca, caKey)
	if err := verifyPinnedChain([]*x509.Certificate{leaf, ca}, pinOf(leaf)); err != nil {
		t.Fatalf("leaf pin must accept the pinned leaf: %v", err)
	}
	other := issueLeaf(t, "localhost", ca, caKey)
	if err := verifyPinnedChain([]*x509.Certificate{other, ca}, pinOf(leaf)); err == nil {
		t.Fatal("leaf pin must reject a different leaf, even under the same CA")
	}
}
