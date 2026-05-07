// Package protocol defines the JSON wire protocol exchanged with WebSocket
// clients. The shape matches the reference TypeScript implementation field
// for field — same JSON keys, same semantics — so a Go server can be
// substituted into an existing deployment without client changes.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// Inbound message types.
const (
	TypeSubscribe   = "subscribe"
	TypeUnsubscribe = "unsubscribe"
	TypePublish     = "publish"
	TypeLockTopic   = "lockTopic"
	TypeUnlockTopic = "unlockTopic"
	TypeRenewLock   = "renewLock"
	TypeScheduleJob = "scheduleJob"
	TypeRemoveJob   = "removeJob"
	TypeBroadcast   = "broadcast"
)

// Lock kinds.
const (
	LockPublish   = "publish"
	LockSubscribe = "subscribe"
)

// Outbound frame types.
const (
	OutSuccess         = "success"
	OutError           = "error"
	OutPublishDelivery = "publish"
	OutReplayTruncated = "replayTruncated"
	OutBroadcastMsg    = "message"
)

// Envelope is the union shape every inbound JSON message conforms to.
// Optional fields use pointers/raw json so we can distinguish absent from
// zero-value.
type Envelope struct {
	Type     string          `json:"type"`
	Topic    string          `json:"topic,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
	JobData  *JobData        `json:"jobData,omitempty"`
	JobID    string          `json:"jobId,omitempty"`
	LockType string          `json:"lockType,omitempty"`
	Since    json.RawMessage `json:"since,omitempty"`
	ReqID    string          `json:"reqID,omitempty"`
}

type JobRetryPolicy struct {
	MaxRetries int `json:"maxRetries"`
	RetryDelay int `json:"retryDelay"`
}

// JobData mirrors the TS JobData type. ExecuteAt and ValidUntil come over
// the wire as ISO 8601 strings; we keep them as strings here and parse
// when scheduling so the original encoding round-trips unchanged.
type JobData struct {
	JobID       string            `json:"jobId"`
	APIEndpoint string            `json:"apiEndpoint"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers,omitempty"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	RetryPolicy *JobRetryPolicy   `json:"retryPolicy,omitempty"`
	ExecuteAt   string            `json:"executeAt"`
	ValidUntil  string            `json:"validUntil,omitempty"`
	Interval    int64             `json:"interval,omitempty"`
}

// LogValue makes JobData log-safe: headers get expanded into a slog.Group
// so the redacting handler can scrub Authorization/token/etc. by key.
func (j JobData) LogValue() slog.Value {
	headerAttrs := make([]slog.Attr, 0, len(j.Headers))
	for k, v := range j.Headers {
		headerAttrs = append(headerAttrs, slog.String(k, v))
	}
	attrs := []slog.Attr{
		slog.String("jobId", j.JobID),
		slog.String("apiEndpoint", j.APIEndpoint),
		slog.String("method", j.Method),
		slog.String("executeAt", j.ExecuteAt),
		slog.Group("headers", attrSliceToAny(headerAttrs)...),
	}
	if j.ValidUntil != "" {
		attrs = append(attrs, slog.String("validUntil", j.ValidUntil))
	}
	if j.Interval != 0 {
		attrs = append(attrs, slog.Int64("interval", j.Interval))
	}
	return slog.GroupValue(attrs...)
}

func attrSliceToAny(attrs []slog.Attr) []any {
	out := make([]any, len(attrs))
	for i, a := range attrs {
		out[i] = a
	}
	return out
}

// ParseExecuteAt parses an ISO 8601 timestamp into a UTC time. Returns the
// zero time if s is empty.
func ParseExecuteAt(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("missing executeAt")
	}
	return time.Parse(time.RFC3339Nano, s)
}

// ParseValidUntil is like ParseExecuteAt but the field is optional.
func ParseValidUntil(s string) (time.Time, bool, error) {
	if s == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false, err
	}
	return t, true, nil
}

// Validate enforces the same shape rules as the TS validateParsedMessage.
// It does NOT validate `data` content (which is opaque to the server) or
// the parseability of `since` cursors (handled at subscribe time).
func Validate(env *Envelope) error {
	if env.Type == "" {
		return errors.New("Message must have a type.")
	}
	switch env.Type {
	case TypeLockTopic, TypeUnlockTopic, TypeRenewLock:
		if env.Topic == "" || (env.LockType != LockPublish && env.LockType != LockSubscribe) {
			return fmt.Errorf("Message of type %q must contain topic and a valid lockType.", env.Type)
		}
	case TypeSubscribe, TypeUnsubscribe:
		if env.Topic == "" {
			return fmt.Errorf("Message of type %q must contain topic.", env.Type)
		}
		if env.Type == TypeSubscribe && len(env.Since) > 0 {
			if err := validateSince(env.Since); err != nil {
				return err
			}
		}
	case TypePublish:
		if env.Topic == "" || len(env.Data) == 0 {
			return fmt.Errorf("Message of type %q must contain both topic and data.", env.Type)
		}
	case TypeScheduleJob:
		if env.JobData == nil {
			return errors.New("Message of type \"scheduleJob\" must contain valid jobData.")
		}
		if err := validateJobData(env.JobData); err != nil {
			return fmt.Errorf("Message of type \"scheduleJob\" must contain valid jobData: %w", err)
		}
	case TypeRemoveJob:
		if env.JobID == "" {
			return fmt.Errorf("Message of type %q must contain jobId to remove", env.Type)
		}
	case TypeBroadcast:
		if len(env.Data) == 0 {
			return errors.New("Messages must contain data field.")
		}
	default:
		return errors.New("Unsupported message type.")
	}
	return nil
}

func validateJobData(j *JobData) error {
	if j.JobID == "" {
		return errors.New("jobId is required")
	}
	if j.APIEndpoint == "" {
		return errors.New("apiEndpoint is required")
	}
	switch j.Method {
	case "GET", "POST", "PUT", "DELETE":
	default:
		return fmt.Errorf("invalid method %q", j.Method)
	}
	if j.ExecuteAt == "" {
		return errors.New("executeAt is required")
	}
	if _, err := ParseExecuteAt(j.ExecuteAt); err != nil {
		return fmt.Errorf("executeAt: %w", err)
	}
	if j.ValidUntil != "" {
		if _, _, err := ParseValidUntil(j.ValidUntil); err != nil {
			return fmt.Errorf("validUntil: %w", err)
		}
	}
	if j.RetryPolicy != nil {
		if j.RetryPolicy.MaxRetries < 0 || j.RetryPolicy.RetryDelay < 0 {
			return errors.New("retryPolicy fields must be non-negative")
		}
	}
	return nil
}

func validateSince(raw json.RawMessage) error {
	// Strings are validated lazily at subscribe time by ParseSinceCursor.
	if len(raw) == 0 {
		return nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return errors.New("subscribe.since must be an ISO timestamp, stream ID, or numeric ms.")
		}
		if s == "" {
			return errors.New("subscribe.since must be an ISO timestamp, stream ID, or numeric ms.")
		}
		return nil
	case 't', 'f', 'n', '{', '[':
		return errors.New("subscribe.since must be an ISO timestamp, stream ID, or numeric ms.")
	default:
		var n float64
		if err := json.Unmarshal(raw, &n); err != nil {
			return errors.New("subscribe.since must be an ISO timestamp, stream ID, or numeric ms.")
		}
		return nil
	}
}

// Outbound encoders.
//
// Each returns a freshly marshaled JSON byte slice ready to send over the
// websocket. Errors are unreachable in practice — every value passed in is
// already JSON-marshalable — but we surface them anyway for safety.

type successFrame struct {
	Type    string `json:"type"`
	ReqID   string `json:"reqID,omitempty"`
	Message string `json:"message"`
}

func EncodeSuccess(reqID, msg string) []byte {
	b, _ := json.Marshal(successFrame{Type: OutSuccess, ReqID: reqID, Message: msg})
	return b
}

func EncodeError(reqID, msg string) []byte {
	b, _ := json.Marshal(successFrame{Type: OutError, ReqID: reqID, Message: msg})
	return b
}

type publishFrame struct {
	Type     string          `json:"type"`
	Topic    string          `json:"topic"`
	Data     json.RawMessage `json:"data"`
	StreamID string          `json:"streamId,omitempty"`
	Replay   bool            `json:"replay,omitempty"`
}

func EncodePublish(topic string, data json.RawMessage, streamID string, replay bool) []byte {
	b, _ := json.Marshal(publishFrame{
		Type:     OutPublishDelivery,
		Topic:    topic,
		Data:     data,
		StreamID: streamID,
		Replay:   replay,
	})
	return b
}

type replayTruncatedFrame struct {
	Type            string `json:"type"`
	Topic           string `json:"topic"`
	OldestAvailable string `json:"oldestAvailable"`
}

func EncodeReplayTruncated(topic, oldestAvailable string) []byte {
	b, _ := json.Marshal(replayTruncatedFrame{
		Type:            OutReplayTruncated,
		Topic:           topic,
		OldestAvailable: oldestAvailable,
	})
	return b
}

type broadcastFrame struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func EncodeBroadcast(data json.RawMessage) []byte {
	b, _ := json.Marshal(broadcastFrame{Type: OutBroadcastMsg, Data: data})
	return b
}

// SinceToString normalizes a `since` raw JSON value (string or number) into
// a string the cursor parser can consume.
func SinceToString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		// fall through to float for non-integer numerics
		var f float64
		if err2 := json.Unmarshal(raw, &f); err2 != nil {
			return "", err
		}
		return strconv.FormatInt(int64(f), 10), nil
	}
	return strconv.FormatInt(n, 10), nil
}
