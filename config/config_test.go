package config_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/config"
)

// settings stands for a domain service's own configuration. The send keys are among those that
// Notifications applies without a restart; the listening address needs one.
type settings struct {
	Listen string `yaml:"listen"`
	Send   struct {
		Paused   bool `yaml:"paused"`
		Attempts int  `yaml:"attempts"`
	} `yaml:"send"`
}

// live is the allowlist of the tests: the settings applied without a restart.
var live = []string{"send.paused", "send.attempts"}

const validFile = "revision: 3f2a9c1\nlisten: \":8443\"\nsend:\n  paused: false\n  attempts: 5\n"

// writeFile replaces the file that the test's instance reads, as Ansible renders it: it writes a
// temporary file beside it and renames it over the old one, so a reader never sees half a file.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	temporary := path + ".new"
	if err := os.WriteFile(temporary, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}

func configPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "config.yaml")
}

// atLeastOneAttempt is the service's own check of its values.
func atLeastOneAttempt(s settings) error {
	if s.Send.Attempts < 1 {
		return errors.New("send.attempts must be at least 1")
	}
	return nil
}

func TestOpenAppliesTheFileAndReportsItsRevisionAndDigest(t *testing.T) {
	path := configPath(t)
	writeFile(t, path, validFile)

	store, err := config.Open(path, config.Options[settings]{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got := store.Current()
	if got.Revision != "3f2a9c1" {
		t.Errorf("revision = %q, want 3f2a9c1", got.Revision)
	}
	// The SHA-256 of validFile's bytes, computed with sha256sum.
	const digest = "sha256:b7236346d224d7ea298bef67bdb410fa669a85b2cceb4e81495b6c4f2737b5df"
	if got.Digest != digest {
		t.Errorf("digest = %q, want %q", got.Digest, digest)
	}
	if got.Values.Listen != ":8443" || got.Values.Send.Paused || got.Values.Send.Attempts != 5 {
		t.Errorf("values = %+v, want :8443, paused false and 5 attempts", got.Values)
	}
}

func TestAnInvalidFileIsRefusedAndTheSnapshotInForceStays(t *testing.T) {
	invalid := map[string]string{
		"an unknown key":         "revision: b71e0d4\nlisten: \":8443\"\nsend:\n  paused: false\n  attempts: 5\n  pause: true\n",
		"no revision":            "listen: \":8443\"\nsend:\n  paused: false\n  attempts: 5\n",
		"not YAML":               "revision: b71e0d4\nsend: [\n",
		"refused by the service": "revision: b71e0d4\nlisten: \":8443\"\nsend:\n  paused: false\n  attempts: 0\n",
	}
	opts := config.Options[settings]{Validate: atLeastOneAttempt}
	for name, content := range invalid {
		t.Run(name, func(t *testing.T) {
			first := configPath(t)
			writeFile(t, first, content)
			if _, err := config.Open(first, opts); err == nil {
				t.Error("Open accepted the file")
			}

			path := configPath(t)
			writeFile(t, path, validFile)
			store, err := config.Open(path, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			writeFile(t, path, content)
			if err := store.Reload(); err == nil {
				t.Error("Reload accepted the file")
			}
			if got := store.Current(); got.Revision != "3f2a9c1" || got.Values.Send.Attempts != 5 {
				t.Errorf("snapshot in force = %+v, want revision 3f2a9c1 with 5 attempts", got)
			}
		})
	}
}

func TestOnlyAllowlistedSettingsChangeWithoutARestart(t *testing.T) {
	path := configPath(t)
	writeFile(t, path, validFile)
	store, err := config.Open(path, config.Options[settings]{Live: live})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	writeFile(t, path, "revision: b71e0d4\nlisten: \":8443\"\nsend:\n  paused: true\n  attempts: 7\n")
	if err := store.Reload(); err != nil {
		t.Fatalf("Reload of an allowlisted change: %v", err)
	}
	if got := store.Current(); got.Revision != "b71e0d4" || !got.Values.Send.Paused || got.Values.Send.Attempts != 7 {
		t.Errorf("after an allowlisted change = %+v, want revision b71e0d4, paused, 7 attempts", got)
	}

	// A rollback arrives as a new revision, here with the values unchanged.
	writeFile(t, path, "revision: 51d8a37\nlisten: \":8443\"\nsend:\n  paused: true\n  attempts: 7\n")
	if err := store.Reload(); err != nil {
		t.Fatalf("Reload of a new revision: %v", err)
	}
	if got := store.Current(); got.Revision != "51d8a37" {
		t.Errorf("revision after a new revision = %q, want 51d8a37", got.Revision)
	}

	writeFile(t, path, "revision: 9c04e2a\nlisten: \":9443\"\nsend:\n  paused: true\n  attempts: 7\n")
	if err := store.Reload(); !errors.Is(err, config.ErrRestartRequired) {
		t.Errorf("Reload of listen = %v, want ErrRestartRequired", err)
	}
	if got := store.Current(); got.Revision != "51d8a37" || got.Values.Listen != ":8443" {
		t.Errorf("snapshot in force = %+v, want revision 51d8a37 on :8443", got)
	}
}

func TestAWatchedFileIsAppliedWhenItChangesAndARefusalIsReported(t *testing.T) {
	path := configPath(t)
	writeFile(t, path, validFile)
	store, err := config.Open(path, config.Options[settings]{Live: live})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refused := make(chan error, 8)
	go store.Watch(ctx, 10*time.Millisecond, func(err error) { refused <- err })

	writeFile(t, path, "revision: b71e0d4\nlisten: \":8443\"\nsend:\n  paused: true\n  attempts: 5\n")
	deadline := time.Now().Add(2 * time.Second)
	for store.Current().Revision != "b71e0d4" {
		if time.Now().After(deadline) {
			t.Fatalf("revision in force = %q, want the changed file's b71e0d4", store.Current().Revision)
		}
		time.Sleep(5 * time.Millisecond)
	}

	writeFile(t, path, "revision: 9c04e2a\nlisten: \":9443\"\nsend:\n  paused: true\n  attempts: 5\n")
	select {
	case err := <-refused:
		if !errors.Is(err, config.ErrRestartRequired) {
			t.Errorf("reported %v, want ErrRestartRequired", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the change to listen was not reported")
	}
	if got := store.Current().Revision; got != "b71e0d4" {
		t.Errorf("revision in force = %q, want b71e0d4", got)
	}
}
