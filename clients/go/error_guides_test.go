package fibril

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The shared clients/error_guides.json is the single source of truth for the
// wording every client must carry for the errors it raises itself, the
// error-message analogue of wire_vectors.json.
// mustContain returns the required substrings for a guide case. The file also
// holds a string-valued "_note" key, so entries are parsed individually.
func mustContain(t *testing.T, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "error_guides.json"))
	if err != nil {
		t.Fatalf("read error_guides.json: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse error_guides.json: %v", err)
	}
	var entry struct {
		MustContain []string `json:"must_contain"`
	}
	if err := json.Unmarshal(m[name], &entry); err != nil {
		t.Fatalf("parse guide %q: %v", name, err)
	}
	return entry.MustContain
}

func assertContainsAll(t *testing.T, name, message string, want []string) {
	t.Helper()
	lower := strings.ToLower(message)
	for _, sub := range want {
		if !strings.Contains(lower, strings.ToLower(sub)) {
			t.Errorf("%s message missing %q\n  got: %s", name, sub, message)
		}
	}
}

func TestConnectionRefusedCarriesGuide(t *testing.T) {
	// An address with nothing listening: bind an ephemeral port, then free it.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	_, err = Connect(context.Background(), addr, EngineOptions{ClientName: "go-test", HeartbeatInterval: time.Hour})
	if err == nil {
		t.Fatal("expected a connection-refused error")
	}
	assertContainsAll(t, "connection_refused", err.Error(), mustContain(t, "connection_refused"))
}

func TestHeartbeatTimeoutCarriesGuide(t *testing.T) {
	assertContainsAll(t, "heartbeat_timeout", heartbeatTimeoutMessage, mustContain(t, "heartbeat_timeout"))
}
