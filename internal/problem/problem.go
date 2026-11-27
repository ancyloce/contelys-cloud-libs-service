// Package problem writes the one error body of the shared contracts for the library's own
// answers: problem details of RFC 9457 with a stable code, the request ID and the correlation ID
// (ADR 0054). A body names no token, no claim and no personal data.
package problem

import (
	"encoding/json"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

// body is the problem body of the shared contracts, openapi/shared/problem.yaml.
type body struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Code          string `json:"code"`
	RequestID     string `json:"requestId"`
	CorrelationID string `json:"correlationId"`
}

// The one title of each code that more than one part of the library answers, which they share.
const (
	UnavailableTitle    = "The service cannot complete the request now."
	TokenInvalidTitle   = "The token is missing, expired, altered, of the wrong kind, or for another audience."
	InvalidRequestTitle = "The request does not match the contract."
)

// idLimit is the most characters that the problem body gives requestId and correlationId.
const idLimit = 128

// Write answers r with status and the problem body of code and title, and of detail when it is not
// empty. A code keeps one title, and detail tells one occurrence from another. requestId is the
// request ID that Edge set in X-Request-Id, and correlationId the request's trace ID, or the
// request ID when the request carries no trace.
func Write(w http.ResponseWriter, r *http.Request, status int, code, title, detail string) {
	answer := body{Type: "about:blank", Title: title, Status: status, Code: code, Detail: detail}
	answer.RequestID = limited(r.Header.Get("X-Request-Id"))
	answer.CorrelationID = answer.RequestID
	if span := trace.SpanContextFromContext(r.Context()); span.HasTraceID() {
		answer.CorrelationID = span.TraceID().String()
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(answer)
}

// limited cuts s to idLimit characters.
func limited(s string) string {
	if runes := []rune(s); len(runes) > idLimit {
		return string(runes[:idLimit])
	}
	return s
}
