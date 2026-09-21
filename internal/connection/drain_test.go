package connection

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/damiensmith1/go-ws-server/metrics"
)

// TestWriterDrainsBeforeClose is the regression test for graceful
// shutdown: everything already queued must reach the client before the
// close frame, and WaitDrained must not return until it has.
//
// It uses a real socket because the ordering guarantee lives in the
// interaction between Close queueing its frame at the tail and runWriter
// consuming the channel in order — neither is observable without a peer
// actually reading the wire.
func TestWriterDrainsBeforeClose(t *testing.T) {
	const queued = 50

	serverConn := make(chan *Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		c := NewConn(ws, ConnConfig{
			UserKey:          "alice",
			SendChanCapacity: queued + 8,
			MaxBufferedBytes: 1 << 20,
			WriteWait:        5 * time.Second,
			Metrics:          metrics.New(),
		}, quietLogger())
		go c.runWriter()
		serverConn <- c
	}))
	defer srv.Close()

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	c := <-serverConn

	// Queue a backlog, then close on top of it — the shutdown sequence.
	for i := 0; i < queued; i++ {
		c.Send([]byte(fmt.Sprintf(`{"n":%d}`, i)))
	}
	c.Close(websocket.CloseGoingAway, "server shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if !c.WaitDrained(ctx) {
		t.Fatal("writer never finished draining")
	}

	// Every queued frame must arrive, in order, before the close.
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < queued; i++ {
		_, msg, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("frame %d of %d was lost on shutdown: %v", i, queued, err)
		}
		if want := fmt.Sprintf(`{"n":%d}`, i); string(msg) != want {
			t.Fatalf("frame %d = %s, want %s", i, msg, want)
		}
	}

	// ...and the close frame comes last, carrying the shutdown reason.
	_, _, err = client.ReadMessage()
	var ce *websocket.CloseError
	if !websocket.IsCloseError(err, websocket.CloseGoingAway) {
		t.Fatalf("want a GoingAway close after the backlog, got %v", err)
	}
	if ce, _ = err.(*websocket.CloseError); ce != nil && ce.Text != "server shutting down" {
		t.Fatalf("close reason = %q", ce.Text)
	}
}
