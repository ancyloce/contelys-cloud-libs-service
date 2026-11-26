package problem_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
)

func TestAProblemBodyKeepsTheRequestIDWithinItsLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Request-Id", strings.Repeat("r", 200))
	response := httptest.NewRecorder()

	problem.Write(response, request, http.StatusUnauthorized, "token_invalid", "The token is missing.", "")

	var body struct {
		RequestID     string `json:"requestId"`
		CorrelationID string `json:"correlationId"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", response.Body.String(), err)
	}
	// problem.yaml limits requestId and correlationId to 128 characters.
	if body.RequestID != strings.Repeat("r", 128) || body.CorrelationID != strings.Repeat("r", 128) {
		t.Errorf("requestId %d and correlationId %d characters, want 128 each", len(body.RequestID), len(body.CorrelationID))
	}
}
