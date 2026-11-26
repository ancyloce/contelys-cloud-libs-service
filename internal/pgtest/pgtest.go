// Package pgtest gives the tests of a library package a PostgreSQL of their own, started once per
// test binary through testcontainers-go, and an empty database for each test. Only tests import
// it.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// image is the PostgreSQL that the tests run, pinned by its digest.
const image = "postgres:18-alpine@sha256:0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71"

var (
	// admin is the address of the container's own database, from which each test creates its own.
	admin     string
	databases atomic.Int64
)

// Main starts PostgreSQL, runs the package's tests and stops it. A package calls it from its
// TestMain.
func Main(m *testing.M) {
	ctx := context.Background()
	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase("library"), postgres.WithUsername("library"), postgres.WithPassword("library"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start PostgreSQL:", err)
		os.Exit(1)
	}
	admin, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "PostgreSQL's address:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(container)
	os.Exit(code)
}

// Database creates an empty database of the test's own, as a domain service's database, and
// returns a pool of it that closes when the test ends.
func Database(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	name := fmt.Sprintf("service_%d", databases.Add(1))
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
