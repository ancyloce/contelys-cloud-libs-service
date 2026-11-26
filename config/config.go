// Package config loads a domain service's configuration from the file that Ansible renders, and
// reports the revision and digest of the snapshot in force (ADR 0010).
package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.yaml.in/yaml/v3"
)

// ErrRestartRequired is returned by Reload when the file changes a setting that the service
// does not apply while it runs. The snapshot in force stays, and the change takes effect
// with the next start of the instance, in a rolling restart.
var ErrRestartRequired = errors.New("the change needs a restart")

// Snapshot is one applied configuration: the service's values, the revision that Ansible wrote
// into the file, which is the Git commit of the desired configuration, and the digest of the
// file's bytes.
type Snapshot[T any] struct {
	Revision string
	Digest   string
	Values   T

	// settings holds the file's settings by their dotted paths, revision excluded.
	settings map[string]any
}

// Options are what a service adds to the rules of the file.
type Options[T any] struct {
	// Validate is the service's own check of its values. A file whose values it refuses is
	// never applied.
	Validate func(T) error

	// Live lists, by their dotted paths such as "send.paused", the settings that Reload may
	// change while the instance runs. A path covers the settings below it.
	Live []string
}

// Store holds the snapshot in force for one configuration file.
type Store[T any] struct {
	path      string
	opts      Options[T]
	reloading sync.Mutex
	current   atomic.Pointer[Snapshot[T]]
}

// Open reads the file at path and applies it as the first snapshot.
func Open[T any](path string, opts Options[T]) (*Store[T], error) {
	s := &Store[T]{path: path, opts: opts}
	snapshot, err := s.read()
	if err != nil {
		return nil, err
	}
	s.current.Store(snapshot)
	return s, nil
}

// Current returns the snapshot in force.
func (s *Store[T]) Current() Snapshot[T] {
	return *s.current.Load()
}

// Reload reads the file again and applies it. When the file is invalid, or changes a setting
// outside Options.Live, it returns the reason and the snapshot in force stays.
func (s *Store[T]) Reload() error {
	s.reloading.Lock()
	defer s.reloading.Unlock()

	snapshot, err := s.read()
	if err != nil {
		return err
	}
	for _, path := range changed(s.current.Load().settings, snapshot.settings) {
		if !s.live(path) {
			return fmt.Errorf("config: %s: %s: %w", s.path, path, ErrRestartRequired)
		}
	}
	s.current.Store(snapshot)
	return nil
}

// Watch reads the file every interval until ctx ends, and reloads it whenever its bytes change,
// so an allowlisted change takes effect without a restart. It hands each refusal to report: a
// file that cannot be read, an invalid file, or a change that needs a restart. A refused file
// that stays the same is reported once.
func (s *Store[T]) Watch(ctx context.Context, every time.Duration, report func(error)) {
	seen := s.Current().Digest
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		data, err := os.ReadFile(s.path)
		if err != nil {
			if seen != unreadable {
				seen = unreadable
				report(fmt.Errorf("config: %w", err))
			}
			continue
		}
		if digest := digestOf(data); digest != seen {
			seen = digest
			if err := s.Reload(); err != nil {
				report(err)
			}
		}
	}
}

// unreadable stands, in Watch, for a file that could not be read the last time.
const unreadable = "unreadable"

// digestOf is the digest that a snapshot reports for the file's bytes.
func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// live reports whether Options.Live covers the setting at path.
func (s *Store[T]) live(path string) bool {
	return slices.ContainsFunc(s.opts.Live, func(entry string) bool {
		return path == entry || strings.HasPrefix(path, entry+".")
	})
}

// read decodes and checks the file into a snapshot without applying it.
func (s *Store[T]) read() (*Snapshot[T], error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var file struct {
		Revision string `yaml:"revision"`
		Values   T      `yaml:",inline"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("config: %s: %w", s.path, err)
	}
	if file.Revision == "" {
		return nil, fmt.Errorf("config: %s: no revision", s.path)
	}
	if s.opts.Validate != nil {
		if err := s.opts.Validate(file.Values); err != nil {
			return nil, fmt.Errorf("config: %s: %w", s.path, err)
		}
	}
	var tree map[string]any
	if err := yaml.Unmarshal(data, &tree); err != nil {
		return nil, fmt.Errorf("config: %s: %w", s.path, err)
	}
	delete(tree, "revision")
	settings := map[string]any{}
	flatten("", tree, settings)

	return &Snapshot[T]{
		Revision: file.Revision,
		Digest:   digestOf(data),
		Values:   file.Values,
		settings: settings,
	}, nil
}

// flatten records each setting of tree under its dotted path. A mapping is followed into its
// keys, and anything else, a list included, is one setting.
func flatten(prefix string, tree map[string]any, out map[string]any) {
	for key, value := range tree {
		if inner, ok := value.(map[string]any); ok {
			flatten(prefix+key+".", inner, out)
			continue
		}
		out[prefix+key] = value
	}
}

// changed lists, in order, the paths whose setting differs between before and after, or that
// only one of them has.
func changed(before, after map[string]any) []string {
	var paths []string
	for path, value := range after {
		if old, ok := before[path]; !ok || !reflect.DeepEqual(old, value) {
			paths = append(paths, path)
		}
	}
	for path := range before {
		if _, ok := after[path]; !ok {
			paths = append(paths, path)
		}
	}
	slices.Sort(paths)
	return paths
}
