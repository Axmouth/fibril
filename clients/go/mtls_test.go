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
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mtlsCA returns a self-signed CA (also usable as the broker's server
// certificate) whose extended key usage permits both server and client auth, so
// it can both serve TLS and be the issuer the broker trusts for client certs.
func mtlsCA(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fibril-mtls-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, der
}

// clientCertSignedBy issues a client certificate signed by the given CA
// and writes the cert and key to temp PEM files, whose
// paths the client feeds to TLSOptions.ClientCertFile / ClientKeyFile.
func clientCertSignedBy(t *testing.T, ca tls.Certificate) (certPath, keyPath string) {
	t.Helper()
	caLeaf, err := x509.ParseCertificate(ca.Certificate[0])
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "fibril-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, caLeaf, &priv.PublicKey, ca.PrivateKey)
	if err != nil {
		t.Fatalf("client cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")
	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "PRIVATE KEY", keyDER)
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// startMTLSBroker starts a TLS listener that requires and verifies a client
// certificate signed by the given CA.
func startMTLSBroker(t *testing.T, serverCert tls.Certificate, clientCA tls.Certificate) net.Listener {
	t.Helper()
	caLeaf, err := x509.ParseCertificate(clientCA.Certificate[0])
	if err != nil {
		t.Fatalf("parse client CA: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caLeaf)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})
	if err != nil {
		t.Fatalf("mtls listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveOneConn(conn, 2)
		}
	}()
	return ln
}

// caAndServerCert returns a self-signed CA (as PEM, to trust via CAFile) and a
// server certificate it signs with a 127.0.0.1 IP SAN so hostname verification
// passes.
func caAndServerCert(t *testing.T) (caPEM []byte, server tls.Certificate) {
	t.Helper()
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fibril-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caPriv.PublicKey, caPriv)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caLeaf, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	srvPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("server key: %v", err)
	}
	srvTmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, &srvTmpl, caLeaf, &srvPriv.PublicKey, caPriv)
	if err != nil {
		t.Fatalf("server cert: %v", err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	server = tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvPriv}
	return caPEM, server
}

// TestTLSCaFileTrustCompletesHandshake checks that trusting a CA via CAFile (not a
// fingerprint pin) verifies a server certificate that chains to it and can publish.
func TestTLSCaFileTrustCompletesHandshake(t *testing.T) {
	caPEM, server := caAndServerCert(t)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	ln := startTLSBroker(t, server)
	defer ln.Close()

	e, err := Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS:               &TLSOptions{CAFile: caPath},
	})
	if err != nil {
		t.Fatalf("TLS connect trusting the CA file: %v", err)
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

// TestMTLSClientCertificatePassesRequiringBroker checks that a client presenting a
// certificate the broker trusts completes the handshake and can publish.
func TestMTLSClientCertificatePassesRequiringBroker(t *testing.T) {
	// One self-signed CA is both the broker's server certificate and the issuer
	// the broker trusts for client certificates.
	ca, serverDER := mtlsCA(t)
	certPath, keyPath := clientCertSignedBy(t, ca)
	ln := startMTLSBroker(t, ca, ca)
	defer ln.Close()

	sum := sha256.Sum256(serverDER)
	e, err := Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS: &TLSOptions{
			CAFingerprint:  hex.EncodeToString(sum[:]),
			ClientCertFile: certPath,
			ClientKeyFile:  keyPath,
		},
	})
	if err != nil {
		t.Fatalf("mTLS connect with a trusted client cert: %v", err)
	}
	defer e.Shutdown()

	off, err := e.PublishConfirmed(context.Background(), Publish{Topic: "t", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("publish over mTLS: %v", err)
	}
	if off != 2 {
		t.Errorf("offset = %d, want 2", off)
	}
}

// TestMTLSCertlessClientRejected checks that a client presenting no certificate
// against a broker that requires one fails to connect.
func TestMTLSCertlessClientRejected(t *testing.T) {
	ca, serverDER := mtlsCA(t)
	ln := startMTLSBroker(t, ca, ca)
	defer ln.Close()

	sum := sha256.Sum256(serverDER)
	_, err := Connect(context.Background(), ln.Addr().String(), EngineOptions{
		ClientName:        "go-test",
		HeartbeatInterval: time.Hour,
		TLS:               &TLSOptions{CAFingerprint: hex.EncodeToString(sum[:])},
	})
	if err == nil {
		t.Fatal("expected a connection error when no client certificate is presented")
	}
	var required *TlsClientCertificateRequiredError
	if !errors.As(err, &required) {
		t.Errorf("error = %T (%v), want *TlsClientCertificateRequiredError", err, err)
	}
}
