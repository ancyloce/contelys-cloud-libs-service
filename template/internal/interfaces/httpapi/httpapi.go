// Package httpapi is the service's interface layer over HTTP (the layout standard, "Structure of
// a domain service"): the health endpoints of the shared service library, and the routes of the
// service's contract, each traced under its own route.
package httpapi

import (
	"net/http"

	"github.com/ancyloce/contelys-cloud-libs-service/health"
	"github.com/ancyloce/contelys-cloud-libs-service/telemetry"
)

// Router routes the service's HTTP requests on the standard library's router.
type Router struct {
	mux *http.ServeMux
}

// New returns a router that serves GET /livez, GET /readyz and GET /revision, outside any trace.
// applied reports the revision and digest of the configuration in force, and checks are the
// service's signals of readiness.
func New(applied func() (revision, digest string), checks ...health.Check) *Router {
	probes := health.Handler(applied, checks...)
	mux := http.NewServeMux()
	for _, probe := range []string{"/livez", "/readyz", "/revision"} {
		mux.Handle("GET "+probe, probes)
	}
	return &Router{mux: mux}
}

// Handle registers a route of the service's contract. The route is traced with W3C Trace Context
// where it is registered, so that its span carries pattern as its http.route.
func (r *Router) Handle(pattern string, handler http.Handler) {
	r.mux.Handle(pattern, telemetry.Handler(handler, pattern))
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
