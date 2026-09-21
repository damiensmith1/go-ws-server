package connection

import (
	"testing"

	"github.com/damiensmith1/go-ws-server/handler"
)

// Conn must satisfy handler.Responder, or a custom verb has no way to
// write frames back.
func TestConnIsResponder(t *testing.T) {
	var _ handler.Responder = (*Conn)(nil)
}
