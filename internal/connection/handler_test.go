package connection

import (
	"testing"

	"github.com/damiensmith1/go-ws-server/internal/protocol"
)

// env.Type comes straight from the client, so it must never reach a label
// unsanitised: unbounded label values mean unbounded time series.
func TestFrameTypeLabel_BoundsCardinality(t *testing.T) {
	for _, known := range []string{
		protocol.TypeSubscribe,
		protocol.TypePublish,
		protocol.TypeScheduleJob,
		protocol.TypeBroadcast,
	} {
		if got := frameTypeLabel(known); got != known {
			t.Fatalf("known type %q became %q", known, got)
		}
	}
	for _, junk := range []string{"", "SUBSCRIBE", "../../etc/passwd", "a-random-string"} {
		if got := frameTypeLabel(junk); got != "unknown" {
			t.Fatalf("junk type %q became %q, want \"unknown\"", junk, got)
		}
	}
}
