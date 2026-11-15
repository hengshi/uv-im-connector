package uvim

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EventLog struct {
	Path string

	mu      sync.Mutex
	nextSeq int64
	seen    map[string]struct{}
}

func NewEventLog(path string) (*EventLog, error) {
	log := &EventLog{Path: path, seen: map[string]struct{}{}, nextSeq: 1}
	if path == "" {
		return log, nil
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, event := range events {
		if event.Sequence >= log.nextSeq {
			log.nextSeq = event.Sequence + 1
		}
		if key := event.DedupeKey(); key != "" {
			log.seen[key] = struct{}{}
		}
	}
	return log, nil
}

func (l *EventLog) Append(ctx context.Context, event Event) (Event, bool, error) {
	return l.AppendForAudience(ctx, event, "")
}

// AppendForAudience persists one normalized event and its private caller
// audience. The public Event JSON is unchanged: client_id is an additive log
// field which old Event readers ignore, and is never exposed by HTTP or WS.
func (l *EventLog) AppendForAudience(ctx context.Context, event Event, audience string) (Event, bool, error) {
	if l == nil {
		return event, true, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := event.DedupeKey()
	if key != "" {
		if _, ok := l.seen[key]; ok {
			return event, false, nil
		}
	}
	if event.ID == "" {
		event.ID = NewID("evt")
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	event.Audience = audience
	event.Sequence = l.nextSeq
	l.nextSeq++
	if l.Path == "" {
		if key != "" {
			l.seen[key] = struct{}{}
		}
		return event, true, nil
	}
	if err := os.MkdirAll(filepath.Dir(l.Path), 0o700); err != nil {
		return event, false, err
	}
	file, err := os.OpenFile(l.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return event, false, err
	}
	defer file.Close()
	persisted := event.Sanitized()
	persisted.Audience = audience
	raw, err := json.Marshal(struct {
		Event
		Audience string `json:"client_id,omitempty"`
	}{Event: persisted, Audience: audience})
	if err != nil {
		return event, false, err
	}
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return event, false, err
	}
	if key != "" {
		l.seen[key] = struct{}{}
	}
	select {
	case <-ctx.Done():
		return event, false, ctx.Err()
	default:
		return event, true, nil
	}
}

// ReadForAudience returns only events durably assigned to audience. Empty
// legacy audience rows are deliberately not exposed to a scoped caller.
func (l *EventLog) ReadForAudience(ctx context.Context, audience string, sequence int64) ([]Event, error) {
	events, err := l.ReadAfter(ctx, sequence)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(events))
	for _, event := range events {
		if event.Audience == audience {
			out = append(out, event)
		}
	}
	return out, nil
}

func (l *EventLog) ReadAfter(ctx context.Context, sequence int64) ([]Event, error) {
	if l == nil || l.Path == "" {
		return nil, nil
	}
	file, err := os.Open(l.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var out []Event
	reader := bufio.NewReader(file)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		raw, readErr := reader.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}
		if len(raw) == 0 && errors.Is(readErr, io.EOF) {
			return out, nil
		}
		var persisted struct {
			Event
			Audience string `json:"client_id,omitempty"`
		}
		if err := json.Unmarshal(raw, &persisted); err != nil {
			return nil, err
		}
		event := persisted.Event
		event.Audience = persisted.Audience
		if event.Sequence > sequence {
			out = append(out, event)
		}
	}
}
