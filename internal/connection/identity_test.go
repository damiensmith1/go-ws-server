package connection

import (
	"testing"

	"github.com/damiensmith1/go-ws-server/bus"
)

// Conn must satisfy bus.Identified, or a Judge can never filter it and
// every judged publish silently delivers to everyone.
func TestConnIsIdentified(t *testing.T) {
	var _ bus.Identified = (*Conn)(nil)

	a := newTestConn(t, nil, ConnConfig{})
	b := newTestConn(t, nil, ConnConfig{})
	if a.SubscriberID() == "" {
		t.Fatal("SubscriberID is empty")
	}
	if a.SubscriberID() == b.SubscriberID() {
		t.Fatal("two connections share a SubscriberID; a Judge could not tell them apart")
	}
}
