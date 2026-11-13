package uvim

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	conversationWindowLogBytes   int64 = 64 << 20
	conversationWindowLogRecords       = 10_000
)

var errConversationWindowTriggerUnavailable = errors.New("conversation window trigger unavailable")

var errConversationContextUnavailable = errors.New("conversation context unavailable")

// ConversationContextCursor is a persisted, metadata-only position in the
// bounded EventLog tail. It contains file offsets and sequence numbers, never
// a second copy of message bodies or provider-private material. FileIdentity,
// generation and EOF make an append-only fast path fail closed on replacement,
// truncation or an unexpected file object.
type ConversationContextCursor struct {
	Version            int                         `json:"version"`
	Generation         string                      `json:"generation,omitempty"`
	FileIdentity       string                      `json:"file_identity,omitempty"`
	EOFOffset          int64                       `json:"eof_offset,omitempty"`
	LowerBoundSequence int64                       `json:"lower_bound_sequence,omitempty"`
	Records            []ConversationContextRecord `json:"records,omitempty"`
}

// ConversationContextRecord identifies one complete sanitized EventLog line.
// Offset and EndOffset are byte offsets in the exact file identified by the
// cursor; they are revalidated before a selected body is read.
type ConversationContextRecord struct {
	Sequence  int64 `json:"sequence"`
	Offset    int64 `json:"offset"`
	EndOffset int64 `json:"end_offset"`
}

// ConversationContextDelta returns either appended records since Cursor or a
// rebuilt tail after an identity/generation/truncation guard failed. Events are
// returned only for the new/rebuilt tail; callers persist their own sanitized
// metadata projection and read selected bodies separately.
type ConversationContextDelta struct {
	Cursor ConversationContextCursor
	Events []Event
	Reset  bool
}

type EventLog struct {
	Path string

	mu        sync.Mutex
	nextSeq   int64
	seen      map[string]struct{}
	appendErr error // An uncertain interaction append cannot be ACKed/read as durable.
}

func NewEventLog(path string) (*EventLog, error) {
	log := &EventLog{Path: path, seen: map[string]struct{}{}, nextSeq: 1}
	if path == "" {
		return log, nil
	}
	if err := log.recoverUnterminatedTail(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
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
	if err := ValidateInteractionEvent(event); err != nil {
		return event, false, err
	}
	if event.Type == EventInteractionAction && (l == nil || l.Path == "") {
		return event, false, errors.New("interaction event requires durable EventLog")
	}
	if l == nil {
		return event, true, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.appendErr != nil {
		return event, false, l.appendErr
	}
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
		if event.Type == EventInteractionAction {
			l.appendErr = err
		}
		return event, false, err
	}
	if event.Type == EventInteractionAction {
		if err := file.Sync(); err != nil {
			l.appendErr = err
			return event, false, err
		}
		directory, err := os.Open(filepath.Dir(l.Path))
		if err == nil {
			err = directory.Sync()
			closeErr := directory.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			l.appendErr = err
			return event, false, err
		}
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
	if ctx == nil {
		ctx = context.Background()
	}
	// A polling/reconnecting reader must not observe an action between its
	// write and durable commit, or bypass a latched uncertain append.
	l.mu.Lock()
	if l.appendErr != nil {
		l.mu.Unlock()
		return nil, l.appendErr
	}
	file, err := os.Open(l.Path)
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}
	info, err := file.Stat()
	l.mu.Unlock()
	defer file.Close()
	if err != nil {
		return nil, err
	}
	var out []Event
	reader := bufio.NewReader(io.LimitReader(file, info.Size()))
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
		if errors.Is(readErr, io.EOF) {
			return out, nil
		}
		var event Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		if err := ValidateInteractionEvent(event); err != nil {
			return nil, err
		}
		if event.Sequence > sequence {
			out = append(out, event)
		}
	}
}

// ConversationContextAdvance reads only complete records appended after a
// previously persisted EOF cursor. A changed inode, first-record generation,
// invalid prior EOF, or truncation returns a complete bounded rebuild with
// Reset=true. This keeps ordinary received-context queries from reparsing the
// full EventLog while retaining a fail-closed recovery path.
func (l *EventLog) ConversationContextAdvance(ctx context.Context, cursor ConversationContextCursor) (ConversationContextDelta, error) {
	if l == nil || strings.TrimSpace(l.Path) == "" {
		return ConversationContextDelta{}, errConversationContextUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	file, err := os.Open(l.Path)
	if err != nil {
		return ConversationContextDelta{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 {
		if err != nil {
			return ConversationContextDelta{}, err
		}
		return ConversationContextDelta{}, errConversationContextUnavailable
	}
	generation, identity, err := conversationContextFileProof(file, info, l.Path)
	if err != nil {
		return ConversationContextDelta{}, errConversationContextUnavailable
	}
	if !validConversationContextCursor(cursor) || cursor.Generation != generation || cursor.FileIdentity != identity || info.Size() < cursor.EOFOffset || !conversationContextCursorEOFValid(cursor) {
		next, events, err := conversationContextTailLocked(ctx, file, info, l.Path, generation, identity)
		if err != nil {
			return ConversationContextDelta{}, err
		}
		return ConversationContextDelta{Cursor: next, Events: events, Reset: true}, nil
	}
	if info.Size() == cursor.EOFOffset {
		return ConversationContextDelta{Cursor: cloneConversationContextCursor(cursor)}, nil
	}
	// A catch-up is itself bounded.  Do not decode an arbitrarily large append
	// just because the cursor's file identity remains valid: a projection can
	// rebuild its retained tail without treating every missed body as metadata.
	if info.Size()-cursor.EOFOffset > conversationWindowLogBytes {
		next, events, tailErr := conversationContextTailLocked(ctx, file, info, l.Path, generation, identity)
		if tailErr != nil {
			return ConversationContextDelta{}, tailErr
		}
		return ConversationContextDelta{Cursor: next, Events: events, Reset: true}, nil
	}
	next, events, overrun, err := conversationContextAppendLocked(ctx, file, info.Size(), cursor)
	if err != nil {
		return ConversationContextDelta{}, err
	}
	if overrun {
		next, events, tailErr := conversationContextTailLocked(ctx, file, info, l.Path, generation, identity)
		if tailErr != nil {
			return ConversationContextDelta{}, tailErr
		}
		return ConversationContextDelta{Cursor: next, Events: events, Reset: true}, nil
	}
	return ConversationContextDelta{Cursor: next, Events: events}, nil
}

// ConversationContextSelected reads and revalidates only the caller-selected
// EventLog bodies under a previously proved cursor. It does not traverse the
// retained tail or copy its bodies into the received-context index.
func (l *EventLog) ConversationContextSelected(ctx context.Context, cursor ConversationContextCursor, records []ConversationContextRecord) (map[int64]Event, error) {
	if l == nil || strings.TrimSpace(l.Path) == "" || len(records) == 0 {
		return nil, errConversationContextUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	file, err := os.Open(l.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() < cursor.EOFOffset {
		return nil, errConversationContextUnavailable
	}
	generation, identity, err := conversationContextFileProof(file, info, l.Path)
	if err != nil || !validConversationContextCursor(cursor) || cursor.Generation != generation || cursor.FileIdentity != identity || !conversationContextCursorEOFValid(cursor) {
		return nil, errConversationContextUnavailable
	}
	known := make(map[int64]ConversationContextRecord, len(cursor.Records))
	for _, record := range cursor.Records {
		known[record.Sequence] = record
	}
	selected := make(map[int64]Event, len(records))
	for _, record := range records {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		knownRecord, ok := known[record.Sequence]
		if !ok || knownRecord.Offset != record.Offset || knownRecord.EndOffset != record.EndOffset || record.Offset < 0 || record.EndOffset <= record.Offset || record.EndOffset > cursor.EOFOffset || record.EndOffset-record.Offset > conversationWindowLogBytes {
			return nil, errConversationContextUnavailable
		}
		raw := make([]byte, record.EndOffset-record.Offset)
		if _, err := file.ReadAt(raw, record.Offset); err != nil {
			return nil, errConversationContextUnavailable
		}
		if len(raw) == 0 || raw[len(raw)-1] != '\n' {
			return nil, errConversationContextUnavailable
		}
		var event Event
		if err := json.Unmarshal(raw, &event); err != nil || event.Sequence != record.Sequence {
			return nil, errConversationContextUnavailable
		}
		selected[event.Sequence] = event
	}
	return selected, nil
}

func conversationContextTailLocked(ctx context.Context, file *os.File, info os.FileInfo, path, generation, identity string) (ConversationContextCursor, []Event, error) {
	offset := max(int64(0), info.Size()-conversationWindowLogBytes)
	// Select a bounded raw tail first.  JSON decoding message bodies before
	// trimming would turn a 64 MiB EventLog window into unbounded projection
	// allocation when it contains many small lines.
	rawRecords, _, err := conversationContextRawRecords(ctx, file, offset, info.Size(), offset > 0)
	if err != nil {
		return ConversationContextCursor{}, nil, err
	}
	events, records, err := conversationContextDecodeRawRecords(rawRecords, 0)
	if err != nil {
		return ConversationContextCursor{}, nil, err
	}
	if len(events) == 0 || len(records) != len(events) {
		return ConversationContextCursor{}, nil, errConversationContextUnavailable
	}
	cursor := ConversationContextCursor{Version: 1, Generation: generation, FileIdentity: identity, EOFOffset: info.Size(), LowerBoundSequence: records[0].Sequence, Records: records}
	return cursor, events, nil
}

func conversationContextAppendLocked(ctx context.Context, file *os.File, eof int64, cursor ConversationContextCursor) (ConversationContextCursor, []Event, bool, error) {
	rawRecords, overrun, err := conversationContextRawRecords(ctx, file, cursor.EOFOffset, eof, false)
	if err != nil {
		return ConversationContextCursor{}, nil, false, err
	}
	if overrun {
		return ConversationContextCursor{}, nil, true, nil
	}
	lastSequence := cursor.Records[len(cursor.Records)-1].Sequence
	events, records, err := conversationContextDecodeRawRecords(rawRecords, lastSequence)
	if err != nil {
		return ConversationContextCursor{}, nil, false, err
	}
	next := cloneConversationContextCursor(cursor)
	next.Records = append(next.Records, records...)
	next.EOFOffset = eof
	for len(next.Records) > conversationWindowLogRecords || len(next.Records) > 1 && next.Records[len(next.Records)-1].EndOffset-next.Records[0].Offset > conversationWindowLogBytes {
		next.Records = next.Records[1:]
	}
	if len(next.Records) == 0 {
		return ConversationContextCursor{}, nil, false, errConversationContextUnavailable
	}
	next.LowerBoundSequence = next.Records[0].Sequence
	return next, events, false, nil
}

type conversationContextRawRecord struct {
	raw    []byte
	offset int64
	end    int64
}

// conversationContextRawRecords retains at most the reader's final 10,000
// complete records, before JSON decoding any message body.  start may be in
// the middle of a record only for a tail rebuild; append readers always begin
// at a proved line boundary.
func conversationContextRawRecords(ctx context.Context, file *os.File, start, end int64, skipFirstPartial bool) ([]conversationContextRawRecord, bool, error) {
	if start < 0 || end <= start {
		return nil, false, errConversationContextUnavailable
	}
	reader := bufio.NewReader(io.NewSectionReader(file, start, end-start))
	position := start
	if skipFirstPartial {
		partial, err := reader.ReadBytes('\n')
		position += int64(len(partial))
		if err != nil {
			return nil, false, errConversationContextUnavailable
		}
	}
	ring := make([]conversationContextRawRecord, conversationWindowLogRecords)
	count, next, overrun := 0, 0, false
	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		default:
		}
		raw, readErr := reader.ReadBytes('\n')
		if len(raw) == 0 && errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil || len(raw) == 0 || raw[len(raw)-1] != '\n' {
			return nil, false, errConversationContextUnavailable
		}
		if count == conversationWindowLogRecords {
			overrun = true
		} else {
			count++
		}
		ring[next] = conversationContextRawRecord{raw: append([]byte(nil), raw...), offset: position, end: position + int64(len(raw))}
		next = (next + 1) % conversationWindowLogRecords
		position += int64(len(raw))
	}
	if position != end || count == 0 {
		return nil, false, errConversationContextUnavailable
	}
	result := make([]conversationContextRawRecord, count)
	first := 0
	if overrun {
		first = next
	}
	for index := range result {
		result[index] = ring[(first+index)%conversationWindowLogRecords]
	}
	return result, overrun, nil
}

func conversationContextDecodeRawRecords(rawRecords []conversationContextRawRecord, previousSequence int64) ([]Event, []ConversationContextRecord, error) {
	events := make([]Event, 0, len(rawRecords))
	records := make([]ConversationContextRecord, 0, len(rawRecords))
	for _, raw := range rawRecords {
		var event Event
		if err := json.Unmarshal(raw.raw, &event); err != nil || event.Sequence <= previousSequence {
			return nil, nil, errConversationContextUnavailable
		}
		previousSequence = event.Sequence
		events = append(events, event)
		records = append(records, ConversationContextRecord{Sequence: event.Sequence, Offset: raw.offset, EndOffset: raw.end})
	}
	return events, records, nil
}

func conversationContextFileProof(file *os.File, info os.FileInfo, path string) (string, string, error) {
	identity := conversationContextFileIdentity(info)
	if identity == "" {
		return "", "", errConversationContextUnavailable
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", "", err
	}
	first, err := bufio.NewReader(file).ReadBytes('\n')
	if err != nil || len(first) == 0 || !json.Valid(first) {
		return "", "", errConversationContextUnavailable
	}
	// Generation is source evidence, not merely the first event digest.  Bind
	// it to the file object as well so an inode replacement that repeats the
	// trigger bytes invalidates received descriptors and opaque continuations.
	sum := sha256.Sum256(append([]byte(path+"\x00"+identity+"\x00"), first...))
	return hex.EncodeToString(sum[:]), identity, nil
}

func conversationContextFileIdentity(info os.FileInfo) string {
	value := reflect.ValueOf(info.Sys())
	if !value.IsValid() {
		return ""
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ""
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return ""
	}
	dev, inode := value.FieldByName("Dev"), value.FieldByName("Ino")
	devValue, devOK := conversationContextNumericField(dev)
	inodeValue, inodeOK := conversationContextNumericField(inode)
	if !devOK || !inodeOK {
		return ""
	}
	return fmt.Sprintf("%x:%x", devValue, inodeValue)
}

func conversationContextNumericField(value reflect.Value) (uint64, bool) {
	if !value.IsValid() {
		return 0, false
	}
	if value.CanUint() {
		return value.Uint(), true
	}
	if value.CanInt() && value.Int() >= 0 {
		return uint64(value.Int()), true
	}
	return 0, false
}

func validConversationContextCursor(cursor ConversationContextCursor) bool {
	return cursor.Version == 1 && cursor.Generation != "" && cursor.FileIdentity != "" && cursor.EOFOffset > 0 && cursor.LowerBoundSequence > 0 && len(cursor.Records) > 0 && cursor.Records[0].Sequence == cursor.LowerBoundSequence
}

func conversationContextCursorEOFValid(cursor ConversationContextCursor) bool {
	if !validConversationContextCursor(cursor) || cursor.Records[len(cursor.Records)-1].EndOffset != cursor.EOFOffset {
		return false
	}
	var previous ConversationContextRecord
	for index, record := range cursor.Records {
		if record.Sequence <= 0 || record.Offset < 0 || record.EndOffset <= record.Offset || record.EndOffset > cursor.EOFOffset || index > 0 && (record.Sequence <= previous.Sequence || record.Offset < previous.EndOffset) {
			return false
		}
		previous = record
	}
	return true
}

func cloneConversationContextCursor(cursor ConversationContextCursor) ConversationContextCursor {
	cursor.Records = append([]ConversationContextRecord(nil), cursor.Records...)
	return cursor
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
