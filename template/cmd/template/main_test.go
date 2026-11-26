package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// freeAddress is a loopback address with a port that nothing listens on.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

// revision is the revision that the service reports as applied.
func revision(t *testing.T, base string) (applied struct {
	Revision string `json:"revision"`
	Digest   string `json:"digest"`
}) {
	t.Helper()
	response, err := http.Get(base + "/revision")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&applied); err != nil {
		t.Fatal(err)
	}
	return applied
}

func TestAServiceBuiltFromTheTemplateServesItsHealthAndTheRevisionOfItsConfiguration(t *testing.T) {
	address := freeAddress(t)
	file := filepath.Join(t.TempDir(), "config.yaml")
	// The file as Ansible renders it, with the Git commit of the desired configuration.
	content := "revision: 3f2a9c1\nlisten: \"" + address + "\"\n"
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan int, 1)
	go func() { exited <- run(ctx, []string{"-config", file}, io.Discard) }()

	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(base + "/livez")
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("GET /livez = %d, want 200", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service never answered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	response, err := http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", response.StatusCode)
	}

	sum := sha256.Sum256([]byte(content))
	if applied := revision(t, base); applied.Revision != "3f2a9c1" || applied.Digest != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Errorf("GET /revision = %+v, want the file's revision 3f2a9c1 and the SHA-256 of its bytes", applied)
	}

	// Ansible renders a new revision: the service applies it without a restart.
	renamed := file + ".new"
	if err := os.WriteFile(renamed, []byte("revision: 51d8a37\nlisten: \""+address+"\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(renamed, file); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for revision(t, base).Revision != "51d8a37" {
		if time.Now().After(deadline) {
			t.Fatal("the service did not apply the new revision of its file")
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	select {
	case code := <-exited:
		if code != 0 {
			t.Errorf("exit code = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the service did not stop when its context ended")
	}
}
