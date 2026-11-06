package uvim

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	conversationWindowLogBytes   int64 = 64 << 20
	conversationWindowLogRecords       = 10_000
)

var errConversationWindowTriggerUnavailable = errors.New("conversation window trigger unavailable")

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
	raw, err := json.Marshal(event.Sanitized())
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
		var event Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		if event.Sequence > sequence {
			out = append(out, event)
		}
	}
}

// ConversationWindowTrigger returns one retained, addressed group-message
// trigger for a native conversation lookup. It reads a fixed tail of the event
// log solely to prove the inbound trigger; it never selects conversation
// material from the log.
func (l *EventLog) ConversationWindowTrigger(ctx context.Context, request ConversationWindowRequest) (Event, error) {
	if l == nil || strings.TrimSpace(l.Path) == "" || strings.TrimSpace(request.Provider) == "" || strings.TrimSpace(request.Connector) == "" || strings.TrimSpace(request.TriggerEventID) == "" || strings.TrimSpace(request.TriggerMessageID) == "" {
		return Event{}, errConversationWindowTriggerUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}

	l.mu.Lock()
	match, err := l.conversationWindowTailMatch(ctx, request)
	l.mu.Unlock()
	if err != nil {
		return Event{}, errConversationWindowTriggerUnavailable
	}
	if match.Type != EventMessageCreate || canonicalKeyPart(match.Channel.Type) != ChannelGroup || !match.Addressed || canonicalKeyPart(match.User.Kind) == UserKindBot || match.Sequence <= 0 || strings.TrimSpace(match.Channel.ID) == "" || strings.TrimSpace(match.Message.ID) == "" {
		return Event{}, errConversationWindowTriggerUnavailable
	}
	return match, nil
}

// conversationWindowTailMatch runs under EventLog.mu. The frozen file extent
// keeps concurrent Append calls out of the read and bounds the retained trigger
// scan. It retains only the final exact match rather than event-log bodies.
func (l *EventLog) conversationWindowTailMatch(ctx context.Context, request ConversationWindowRequest) (Event, error) {
	file, err := os.Open(l.Path)
	if err != nil {
		return Event{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 {
		if err != nil {
			return Event{}, err
		}
		return Event{}, errConversationWindowTriggerUnavailable
	}
	offset := max(int64(0), info.Size()-conversationWindowLogBytes)
	reader := bufio.NewReader(io.NewSectionReader(file, offset, info.Size()-offset))
	if offset > 0 {
		_, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return Event{}, errConversationWindowTriggerUnavailable
			}
			return Event{}, readErr
		}
	}

	var lastMatch Event
	recordCount, lastMatchOrdinal, priorMatchOrdinal := 0, 0, 0
	for {
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		default:
		}
		raw, readErr := reader.ReadBytes('\n')
		if len(raw) == 0 && errors.Is(readErr, io.EOF) {
			if lastMatchOrdinal == 0 || recordCount-lastMatchOrdinal >= conversationWindowLogRecords || (priorMatchOrdinal != 0 && recordCount-priorMatchOrdinal < conversationWindowLogRecords) {
				return Event{}, errConversationWindowTriggerUnavailable
			}
			return lastMatch, nil
		}
		if readErr != nil {
			// A partial final JSON record is a corrupt tail, not historical
			// evidence that may be recovered by scanning elsewhere.
			if errors.Is(readErr, io.EOF) {
				return Event{}, errConversationWindowTriggerUnavailable
			}
			return Event{}, readErr
		}
		var identity struct {
			ID        string `json:"id"`
			Provider  string `json:"provider"`
			Connector string `json:"connector"`
			Message   struct {
				ID string `json:"id"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			return Event{}, errConversationWindowTriggerUnavailable
		}
		recordCount++
		if canonicalKeyPart(identity.Provider) == canonicalKeyPart(request.Provider) && canonicalKeyPart(identity.Connector) == canonicalKeyPart(request.Connector) && strings.TrimSpace(identity.ID) == strings.TrimSpace(request.TriggerEventID) && strings.TrimSpace(identity.Message.ID) == strings.TrimSpace(request.TriggerMessageID) {
			var event Event
			if err := json.Unmarshal(raw, &event); err != nil {
				return Event{}, errConversationWindowTriggerUnavailable
			}
			priorMatchOrdinal = lastMatchOrdinal
			lastMatchOrdinal = recordCount
			lastMatch = event
		}
	}
}
