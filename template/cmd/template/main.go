// Command template is the composition root of a Go domain service built from the service template
// of the shared service library (ADR 0034). It loads the service's configuration from the file
// that Ansible renders and applies allowlisted changes without a restart, traces with W3C Trace
// Context and logs with the trace identifier, and serves liveness, readiness and the revision of
// the configuration in force, all through the library. A new domain service copies the template
// into its sub-repository, gives the module and the command its slug, registers the routes of its
// contract in internal/interfaces/httpapi, and adds its other layers under internal/ (the layout
// standard, "Structure of a domain service").
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/template/internal/interfaces/httpapi"
	"github.com/ancyloce/contelys-cloud/libs/service/config"
	"github.com/ancyloce/contelys-cloud/libs/service/telemetry"
)

// slug is the deployable's slug, which names the service in its telemetry, as service.name. A
// copy gives its own.
const slug = "template"

// settings is the service's configuration. A copy adds its own settings, and lists in live the
// dotted paths of those that change without a restart.
type settings struct {
	Listen string `yaml:"listen"`
}

var live []string

func validate(s settings) error {
	if s.Listen == "" {
		return errors.New("listen is required")
	}
	return nil
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout))
}

// run serves until ctx ends or the process receives SIGTERM or SIGINT, and returns the process's
// exit code. It writes its logs to out.
func run(ctx context.Context, args []string, out io.Writer) int {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()
	flags := flag.NewFlagSet(slug, flag.ContinueOnError)
	flags.SetOutput(out)
	path := flags.String("config", "", "the configuration file that Ansible renders")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	// No exporter yet: the spans are dropped until the service sends them to the OpenTelemetry
	// Collector (issue 11 of the phase 1 platform). The trace context still travels, and the logs
	// still carry it.
	logger, shutdown := telemetry.Setup(slug, nil, out)
	defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	if *path == "" {
		logger.Error("no configuration file: pass -config")
		return 2
	}
	store, err := config.Open(*path, config.Options[settings]{Validate: validate, Live: live})
	if err != nil {
		logger.Error("the configuration was refused", "error", err)
		return 1
	}
	go store.Watch(ctx, time.Second, func(err error) {
		if errors.Is(err, config.ErrRestartRequired) {
			logger.Warn("a changed configuration waits for the next restart", "error", err)
			return
		}
		logger.Error("a changed configuration was refused", "error", err)
	})

	// A copy registers the routes of its contract on the router with Handle.
	router := httpapi.New(func() (string, string) {
		snapshot := store.Current()
		return snapshot.Revision, snapshot.Digest
	})

	server := &http.Server{Addr: store.Current().Values.Listen, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	logger.Info("serving", "revision", store.Current().Revision)

	select {
	case err := <-served:
		logger.Error("the server stopped", "error", err)
		return 1
	case <-ctx.Done():
	}
	stopping, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(stopping); err != nil {
		logger.Error("the server did not stop in time", "error", err)
		return 1
	}
	return 0
}
