package testutil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func NewEphemeralHTTPServer(t testing.TB, handler func(http.ResponseWriter, *http.Request)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server
}
