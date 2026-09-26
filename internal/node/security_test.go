package node

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"forger/internal/config"
)

func TestInternalEndpointsRequireConfiguredToken(t *testing.T) {
	s, err := New(config.Config{Mode: "node", NodeID: "n1", DataDir: t.TempDir(), CapacityBytes: 1024, InternalToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		token string
		want  int
	}{{"", http.StatusUnauthorized}, {"wrong", http.StatusUnauthorized}, {"secret", http.StatusOK}} {
		req := httptest.NewRequest(http.MethodGet, "/internal/inventory", nil)
		req.Header.Set("X-Forger-Internal-Token", test.token)
		res := httptest.NewRecorder()
		s.Handler().ServeHTTP(res, req)
		if res.Code != test.want {
			t.Fatalf("token %q status=%d want=%d", test.token, res.Code, test.want)
		}
	}
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/health", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("health status=%d", res.Code)
	}
}
