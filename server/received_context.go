package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	receivedContextResourceCapacity = 20_000
	receivedContextResourceBytes    = 8 << 20
)

// receivedContextIndex is a bounded EventLog projection. It deliberately does
// not store message text: each read rechecks the retained EventLog body and its
// digest. Provider-private resource references are encrypted and survive only
// while their source EventLog record and generation remain valid.
type receivedContextIndex struct {
	eventLog *uvim.EventLog
	path     string
	mu       sync.Mutex
	loaded   bool
	state    receivedContextState
}

type receivedContextState struct {
	Version    int64  `json:"version"`
	Generation string `json:"generation,omitempty"`
	LowerBound int64  `json:"lower_bound_sequence,omitempty"`
	// LogCursor is EventLog-owned append/rebuild evidence. It persists only
	// bounded record offsets/sequence metadata, never message bodies.
	LogCursor uvim.ConversationContextCursor `json:"log_cursor,omitempty"`
	Entries   map[int64]receivedContextEntry `json:"entries"`
	// ScopeOrder is a persisted metadata-only seek index ordered by
	// (created_at desc, sequence desc) per exact provider/connector/chat/thread.
	// Query bodies are still read and revalidated from EventLog only for the
	// selected 1--10 candidates in each bounded page.
	ScopeOrder map[string][]int64                `json:"scope_order"`
	Pending    map[string]receivedContextPending `json:"pending"`
	Withdrawn  map[string]time.Time              `json:"withdrawn,omitempty"`
	// WithdrawnSequences ties each deletion tombstone to retained EventLog
	// evidence.  The timestamp is useful display metadata; the sequence is the
	// authority that lets the bounded projection prune tombstones safely.
	WithdrawnSequences map[string]int64 `json:"withdrawn_sequences,omitempty"`
}

type receivedContextEntry struct {
	Sequence       int64                     `json:"sequence"`
	EventID        string                    `json:"event_id"`
	EntryDigest    string                    `json:"entry_digest"`
	Provider       string                    `json:"provider"`
	Connector      string                    `json:"connector"`
	ConversationID string                    `json:"conversation_id"`
	ThreadID       string                    `json:"thread_id,omitempty"`
	MessageID      string                    `json:"message_id"`
	CreatedAt      time.Time                 `json:"created_at"`
	SenderID       string                    `json:"sender_id,omitempty"`
	SenderKind     string                    `json:"sender_kind,omitempty"`
	RecordOffset   int64                     `json:"record_offset"`
	RecordEnd      int64                     `json:"record_end"`
	Resources      []receivedContextResource `json:"resources,omitempty"`
}

type receivedContextResource struct {
	Ordinal        int                  `json:"ordinal"`
	Descriptor     uvim.ContextResource `json:"descriptor"`
	ResourceCipher string               `json:"resource_cipher,omitempty"`
	State          string               `json:"state"`
}

type receivedContextPending struct {
	ID             string                    `json:"id"`
	EventID        string                    `json:"event_id"`
	Provider       string                    `json:"provider"`
	Connector      string                    `json:"connector"`
	ConversationID string                    `json:"conversation_id"`
	ThreadID       string                    `json:"thread_id,omitempty"`
	MessageID      string                    `json:"message_id"`
	CreatedAt      time.Time                 `json:"created_at"`
	Resources      []receivedContextResource `json:"resources,omitempty"`
	PreparedAt     time.Time                 `json:"prepared_at"`
}

type receivedContextQuery struct {
	Generation         string
	TriggerDigest      string
	LowerBound         int64
	CandidateCount     int
	PageCount          int
	MatchCount         int
	ScannedOldest      *time.Time
	ScannedNewest      *time.Time
	Items              []uvim.ContextMessage
	HasMore            bool
	NextBeforeTime     time.Time
	NextBeforeSequence int64
	Reason             string
}

func newReceivedContextIndex(eventLog *uvim.EventLog) *receivedContextIndex {
	path := ""
	if eventLog != nil && strings.TrimSpace(eventLog.Path) != "" {
		path = filepath.Join(filepath.Dir(eventLog.Path), "state", "conversation-context", "received-index.v1.json")
	}
	return &receivedContextIndex{eventLog: eventLog, path: path, state: receivedContextState{Version: 1, Entries: map[int64]receivedContextEntry{}, ScopeOrder: map[string][]int64{}, Pending: map[string]receivedContextPending{}, Withdrawn: map[string]time.Time{}, WithdrawnSequences: map[string]int64{}}}
}

// prepare is called before EventLog append. A later duplicate or append failure
// is explicitly discarded; a restart discards all pending records rather than
// treating uncommitted provider references as received evidence.
func (r *receivedContextIndex) prepare(authToken string, event uvim.Event) (string, error) {
	if r == nil || strings.TrimSpace(authToken) == "" || !isReceivedAmbientEvent(event) || len(event.Message.Resources) == 0 {
		return "", nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(); err != nil {
		return "", err
	}
	sourceTime, ok := uvim.EventSourceTime(event)
	if !ok {
		return "", nil
	}
	id, err := conversationMaterialRandomID()
	if err != nil {
		return "", err
	}
	pending := receivedContextPending{
		ID: id, EventID: event.ID, Provider: event.Provider, Connector: event.Connector,
		ConversationID: event.Channel.ID, ThreadID: event.Referrer.ThreadID, MessageID: event.Message.ID,
		CreatedAt: sourceTime, PreparedAt: time.Now().UTC(),
	}
	ambiguousIDs := conflictingResourceIDs(event.Message.Resources)
	for ordinal, resource := range event.Message.Resources {
		resource.Provider = uvim.FirstNonEmpty(resource.Provider, event.Provider)
		resource.Connector = uvim.FirstNonEmpty(resource.Connector, event.Connector)
		if ambiguousIDs[strings.TrimSpace(resource.ID)] {
			// Match Hub conflict semantics before encrypting a private lazy ref.
			// The ingress ID is only a relation key, so it cannot choose a
			// ResourceStore path or physical-byte cache for distinct provider
			// references.  Key/secret/private selectors remain unchanged.
			resource.ID = uvim.NewID("res")
		}
		resource.SourceMessageID = uvim.FirstNonEmpty(resource.SourceMessageID, event.Message.ID)
		if resource.Metadata == nil {
			resource.Metadata = map[string]string{}
		}
		resource.Metadata["message_id"] = resource.SourceMessageID
		ciphertext, err := conversationMaterialEncrypt(authToken, resource)
		if err != nil {
			return "", err
		}
		pending.Resources = append(pending.Resources, receivedContextResource{
			Ordinal: ordinal, Descriptor: uvim.ContextResource{Source: "received", SourceMessageID: resource.SourceMessageID, Kind: resource.Kind, Name: resource.Name, MIME: resource.MIME, SizeBytes: resource.SizeBytes, SHA256: resource.SHA256, Availability: "available"},
			ResourceCipher: ciphertext, State: "pending",
		})
	}
	if len(pending.Resources) == 0 {
		return "", nil
	}
	r.state.Pending[id] = pending
	r.enforcePendingCapacityLocked()
	if _, retained := r.state.Pending[id]; !retained {
		return "", nil
	}
	if err := r.persistLocked(); err != nil {
		return "", err
	}
	return id, nil
}

func (r *receivedContextIndex) discard(pendingID string) {
	if r == nil || pendingID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loadLocked() == nil {
		delete(r.state.Pending, pendingID)
		_ = r.persistLocked()
	}
}

func (r *receivedContextIndex) commit(ctx context.Context, pendingID, authToken string, written uvim.Event) error {
	if r == nil || pendingID == "" || strings.TrimSpace(authToken) == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(); err != nil {
		return err
	}
	changed, cursor, err := r.syncTailLocked(ctx)
	if err != nil {
		return err
	}
	pending, ok := r.state.Pending[pendingID]
	if !ok || pending.EventID != written.ID || pending.Provider != written.Provider || pending.Connector != written.Connector || pending.MessageID != written.Message.ID || written.Sequence <= 0 {
		delete(r.state.Pending, pendingID)
		if changed {
			return r.persistLocked()
		}
		return nil
	}
	digest, err := uvim.ConversationEventDigest(written)
	if err != nil {
		return err
	}
	sourceTime, ok := uvim.EventSourceTime(written)
	if !ok {
		return nil
	}
	position, ok := receivedContextRecordFromCursor(cursor, written.Sequence)
	if !ok {
		return errors.New("received context EventLog record unavailable")
	}
	entry := receivedContextEntry{
		Sequence: written.Sequence, EventID: written.ID, EntryDigest: digest, Provider: written.Provider, Connector: written.Connector,
		ConversationID: written.Channel.ID, ThreadID: written.Referrer.ThreadID, MessageID: written.Message.ID, CreatedAt: sourceTime,
		SenderID: written.User.ID, SenderKind: written.User.Kind, RecordOffset: position.Offset, RecordEnd: position.EndOffset, Resources: pending.Resources,
	}
	for index := range entry.Resources {
		entry.Resources[index].State = "active"
		entry.Resources[index].Descriptor.SourceGeneration = cursor.Generation
		entry.Resources[index].Descriptor.SourceEntryDigest = digest
		entry.Resources[index].Descriptor.SourceSequence = written.Sequence
		entry.Resources[index].Descriptor.SourceOrdinal = entry.Resources[index].Ordinal
	}
	r.state.Entries[entry.Sequence] = entry
	delete(r.state.Pending, pendingID)
	r.state.Generation = cursor.Generation
	if r.state.LowerBound < cursor.LowerBoundSequence {
		r.state.LowerBound = cursor.LowerBoundSequence
	}
	r.enforceCapacityLocked()
	r.rebuildScopeOrderLocked()
	return r.persistLocked()
}

func (r *receivedContextIndex) observe(ctx context.Context, written uvim.Event) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loadLocked() != nil {
		return
	}
	changed, _, err := r.syncTailLocked(ctx)
	if err != nil {
		return
	}
	if changed {
		_ = r.persistLocked()
	}
}

func (r *receivedContextIndex) query(ctx context.Context, authToken string, trigger uvim.Event, filter conversationSearchFilter, limit int, before time.Time, beforeSequence int64) (receivedContextQuery, error) {
	if r == nil || strings.TrimSpace(authToken) == "" {
		return receivedContextQuery{Reason: "received_unavailable"}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(); err != nil {
		return receivedContextQuery{}, err
	}
	changed, cursor, err := r.syncTailLocked(ctx)
	if err != nil {
		return receivedContextQuery{Reason: "index_rebuild_limited"}, nil
	}
	if changed {
		if err := r.persistLocked(); err != nil {
			return receivedContextQuery{}, err
		}
	}
	triggerDigest, err := uvim.ConversationEventDigest(trigger)
	if err != nil {
		return receivedContextQuery{}, err
	}
	result := receivedContextQuery{Generation: cursor.Generation, TriggerDigest: triggerDigest, LowerBound: r.state.LowerBound}
	if trigger.Sequence < r.state.LowerBound {
		result.Reason = "index_capacity_limited"
		return result, nil
	}
	order := r.state.ScopeOrder[receivedContextScopeKey(trigger.Provider, trigger.Connector, trigger.Channel.ID, trigger.Referrer.ThreadID)]
	position := 0
	for pages := 0; pages < conversationSearchMaxPages; pages++ {
		page := make([]receivedContextEntry, 0, limit)
		for position < len(order) && len(page) < limit {
			sequence := order[position]
			position++
			entry, ok := r.state.Entries[sequence]
			if !ok || !eligibleReceivedContextEntry(entry, trigger, before, beforeSequence) {
				continue
			}
			page = append(page, entry)
		}
		// An empty first page is an honest metadata attempt. Later empty pages
		// mean remaining scope metadata was outside the strict sequence/cursor
		// boundary and no EventLog body was selected.
		if len(page) == 0 {
			if result.PageCount == 0 {
				result.PageCount = 1
			}
			return result, nil
		}
		result.PageCount++
		records := make([]uvim.ConversationContextRecord, 0, len(page))
		for _, entry := range page {
			result.CandidateCount++
			observed := entry.CreatedAt.UTC()
			if result.ScannedOldest == nil || observed.Before(*result.ScannedOldest) {
				result.ScannedOldest = &observed
			}
			if result.ScannedNewest == nil || observed.After(*result.ScannedNewest) {
				result.ScannedNewest = &observed
			}
			records = append(records, uvim.ConversationContextRecord{Sequence: entry.Sequence, Offset: entry.RecordOffset, EndOffset: entry.RecordEnd})
		}
		selected, selectErr := r.eventLog.ConversationContextSelected(ctx, cursor, records)
		if selectErr != nil || len(selected) != len(records) {
			return receivedContextQuery{}, errors.New("received context selected EventLog body unavailable")
		}
		matches := make([]struct {
			entry receivedContextEntry
			event uvim.Event
		}, 0, len(page))
		for _, entry := range page {
			event, ok := selected[entry.Sequence]
			createdAt, sourceTimeOK := uvim.EventSourceTime(event)
			if !ok || !sourceTimeOK || !createdAt.Equal(entry.CreatedAt) || !sameReceivedContextEntry(entry, event) {
				return receivedContextQuery{}, errors.New("received context selected EventLog proof unavailable")
			}
			message := receivedContextMessage(event)
			if len(filterConversationSearchCandidates([]uvim.ConversationSearchCandidate{{Message: message, Readable: strings.TrimSpace(message.Text) != ""}}, filter)) != 0 {
				matches = append(matches, struct {
					entry receivedContextEntry
					event uvim.Event
				}{entry: entry, event: event})
			}
		}
		last := page[len(page)-1]
		hasMore := receivedContextOrderHasEligible(order, position, r.state.Entries, trigger, before, beforeSequence)
		if len(matches) > 0 {
			result.MatchCount, result.Items = len(matches), make([]uvim.ContextMessage, 0, len(matches))
			for _, match := range matches {
				result.Items = append(result.Items, r.receivedContextMessage(authToken, match.entry, match.event))
			}
			sort.SliceStable(result.Items, func(i, j int) bool {
				if result.Items[i].CreatedAt.Equal(result.Items[j].CreatedAt) {
					return result.Items[i].Sequence < result.Items[j].Sequence
				}
				return result.Items[i].CreatedAt.Before(result.Items[j].CreatedAt)
			})
			result.HasMore = hasMore
			if hasMore {
				result.NextBeforeTime, result.NextBeforeSequence = last.CreatedAt, last.Sequence
			}
			return result, nil
		}
		if !hasMore {
			return result, nil
		}
		if pages+1 == conversationSearchMaxPages {
			result.HasMore, result.Reason = true, "search_scan_limited"
			result.NextBeforeTime, result.NextBeforeSequence = last.CreatedAt, last.Sequence
			return result, nil
		}
	}
	return result, nil
}

func eligibleReceivedContextEntry(entry receivedContextEntry, trigger uvim.Event, before time.Time, beforeSequence int64) bool {
	return entry.Sequence < trigger.Sequence && entry.Provider == trigger.Provider && entry.Connector == trigger.Connector && entry.ConversationID == trigger.Channel.ID && entry.ThreadID == trigger.Referrer.ThreadID && (before.IsZero() || entry.CreatedAt.Before(before) || entry.CreatedAt.Equal(before) && entry.Sequence < beforeSequence)
}

func receivedContextOrderHasEligible(order []int64, start int, entries map[int64]receivedContextEntry, trigger uvim.Event, before time.Time, beforeSequence int64) bool {
	for _, sequence := range order[start:] {
		if entry, ok := entries[sequence]; ok && eligibleReceivedContextEntry(entry, trigger, before, beforeSequence) {
			return true
		}
	}
	return false
}

func receivedContextMessage(event uvim.Event) uvim.ContextMessage {
	createdAt, _ := uvim.EventSourceTime(event)
	return uvim.ContextMessage{MessageID: event.Message.ID, Sequence: event.Sequence, SenderID: event.User.ID, SenderIDType: event.User.Kind, SenderType: event.User.Kind, CreatedAt: createdAt, Text: event.Message.Text}
}

func (r *receivedContextIndex) receivedContextMessage(authToken string, entry receivedContextEntry, event uvim.Event) uvim.ContextMessage {
	message := receivedContextMessage(event)
	for _, stored := range entry.Resources {
		descriptor := stored.Descriptor
		descriptor.Resource = uvim.ResourceRef{}
		if stored.State == "active" {
			private, decryptErr := conversationMaterialDecrypt(authToken, stored.ResourceCipher)
			if decryptErr == nil {
				descriptor.Resource = private
			} else {
				descriptor.Availability = "unrecoverable"
			}
		} else {
			descriptor.Availability = "unrecoverable"
		}
		message.Resources = append(message.Resources, descriptor)
	}
	return message
}

func (r *receivedContextIndex) materializeEvent(ctx context.Context, record conversationMaterialRecord, trigger uvim.Event) (uvim.Event, error) {
	if r == nil || record.Descriptor.Source != "received" {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(); err != nil {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	changed, cursor, err := r.syncTailLocked(ctx)
	if err != nil || record.Descriptor.SourceGeneration != cursor.Generation || record.Descriptor.SourceSequence <= 0 || record.Descriptor.SourceSequence >= trigger.Sequence {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	if changed {
		if err := r.persistLocked(); err != nil {
			return uvim.Event{}, uvim.ErrConversationResourceUnavailable
		}
	}
	entry, ok := r.state.Entries[record.Descriptor.SourceSequence]
	if !ok || entry.EntryDigest != record.Descriptor.SourceEntryDigest || entry.Provider != trigger.Provider || entry.Connector != trigger.Connector || entry.ConversationID != trigger.Channel.ID || entry.ThreadID != trigger.Referrer.ThreadID {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	ordinalActive := false
	for _, resource := range entry.Resources {
		if resource.Ordinal == record.Descriptor.SourceOrdinal && resource.State == "active" && resource.Descriptor.SourceGeneration == record.Descriptor.SourceGeneration && resource.Descriptor.SourceEntryDigest == record.Descriptor.SourceEntryDigest {
			ordinalActive = true
			break
		}
	}
	if !ordinalActive {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	selected, selectErr := r.eventLog.ConversationContextSelected(ctx, cursor, []uvim.ConversationContextRecord{{Sequence: entry.Sequence, Offset: entry.RecordOffset, EndOffset: entry.RecordEnd}})
	event, ok := selected[entry.Sequence]
	if selectErr != nil || !ok || !sameReceivedContextEntry(entry, event) {
		return uvim.Event{}, uvim.ErrConversationResourceUnavailable
	}
	return event, nil
}

func (r *receivedContextIndex) loadLocked() error {
	if r.loaded {
		return nil
	}
	r.loaded = true
	r.state = receivedContextState{Version: 1, Entries: map[int64]receivedContextEntry{}, ScopeOrder: map[string][]int64{}, Pending: map[string]receivedContextPending{}, Withdrawn: map[string]time.Time{}, WithdrawnSequences: map[string]int64{}}
	if r.path == "" {
		return errors.New("received context index unavailable")
	}
	raw, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || json.Unmarshal(raw, &r.state) != nil || r.state.Version != 1 {
		return errors.New("received context index invalid")
	}
	if r.state.Entries == nil {
		r.state.Entries = map[int64]receivedContextEntry{}
	}
	if r.state.ScopeOrder == nil {
		r.state.ScopeOrder = map[string][]int64{}
		r.rebuildScopeOrderLocked()
	}
	if r.state.Pending == nil {
		r.state.Pending = map[string]receivedContextPending{}
	}
	changed := false
	if r.state.Withdrawn == nil {
		r.state.Withdrawn = map[string]time.Time{}
	}
	if r.state.WithdrawnSequences == nil {
		// Older v1 projections have tombstone timestamps but no retained source
		// sequence.  Rebuild rather than letting an unbounded old tombstone map
		// silently outlive its EventLog evidence.
		r.state.WithdrawnSequences = map[string]int64{}
		if len(r.state.Withdrawn) > 0 {
			r.state.Generation, r.state.LowerBound = "", 0
			r.state.LogCursor = uvim.ConversationContextCursor{}
			r.state.Entries = map[int64]receivedContextEntry{}
			r.state.ScopeOrder = map[string][]int64{}
			r.state.Withdrawn = map[string]time.Time{}
		}
		changed = true
	}
	if len(r.state.Pending) > 0 {
		r.state.Pending = map[string]receivedContextPending{}
		changed = true
	}
	if changed {
		return r.persistLocked()
	}
	return nil
}

func (r *receivedContextIndex) persistLocked() error {
	if r.path == "" {
		return errors.New("received context index unavailable")
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(r.state)
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(r.path)); err == nil {
		defer dir.Close()
		return dir.Sync()
	}
	return nil
}

func (r *receivedContextIndex) syncTailLocked(ctx context.Context) (bool, uvim.ConversationContextCursor, error) {
	delta, err := r.eventLog.ConversationContextAdvance(ctx, r.state.LogCursor)
	if err != nil {
		return false, uvim.ConversationContextCursor{}, err
	}
	cursor := delta.Cursor
	changed := delta.Reset || r.state.Generation != cursor.Generation || r.state.LogCursor.EOFOffset != cursor.EOFOffset || r.state.LogCursor.FileIdentity != cursor.FileIdentity
	if delta.Reset || r.state.Generation != cursor.Generation {
		r.state.Generation, r.state.LowerBound = cursor.Generation, cursor.LowerBoundSequence
		r.state.Entries = map[int64]receivedContextEntry{}
		r.state.ScopeOrder = map[string][]int64{}
		r.state.Withdrawn = map[string]time.Time{}
		r.state.WithdrawnSequences = map[string]int64{}
	}
	r.state.LogCursor = cursor
	if r.state.LowerBound < cursor.LowerBoundSequence {
		r.state.LowerBound = cursor.LowerBoundSequence
		changed = true
	}
	if r.pruneRetainedLocked(r.state.LowerBound) {
		changed = true
	}
	for _, event := range delta.Events {
		// The index may have advanced its own capacity floor beyond the current
		// EventLog tail.  Never reintroduce metadata or deletion evidence that
		// is already outside that bounded projection.
		if event.Sequence < r.state.LowerBound {
			continue
		}
		if event.Type == uvim.EventMessageDelete && event.Message.ID != "" {
			key := receivedContextMessageKey(event.Provider, event.Connector, event.Channel.ID, event.Message.ID)
			if sequence := r.state.WithdrawnSequences[key]; sequence < event.Sequence {
				r.state.Withdrawn[key] = event.Time.UTC()
				r.state.WithdrawnSequences[key] = event.Sequence
				changed = true
			}
			for sequence, entry := range r.state.Entries {
				if entry.Provider == event.Provider && entry.Connector == event.Connector && entry.ConversationID == event.Channel.ID && entry.MessageID == event.Message.ID {
					delete(r.state.Entries, sequence)
					changed = true
				}
			}
			continue
		}
		if !isReceivedHistoryEvent(event) {
			continue
		}
		if _, withdrawn := r.state.Withdrawn[receivedContextMessageKey(event.Provider, event.Connector, event.Channel.ID, event.Message.ID)]; withdrawn {
			continue
		}
		if _, exists := r.state.Entries[event.Sequence]; exists {
			continue
		}
		entry, ok := receivedContextEntryFromEvent(event, cursor, nil)
		if !ok {
			continue
		}
		r.state.Entries[event.Sequence] = entry
		changed = true
	}
	if r.enforceCapacityLocked() {
		changed = true
	}
	if changed {
		r.rebuildScopeOrderLocked()
	}
	return changed, cursor, nil
}

func receivedContextEntryFromEvent(event uvim.Event, cursor uvim.ConversationContextCursor, resources []receivedContextResource) (receivedContextEntry, bool) {
	digest, digestErr := uvim.ConversationEventDigest(event)
	createdAt, sourceTimeOK := uvim.EventSourceTime(event)
	position, positionOK := receivedContextRecordFromCursor(cursor, event.Sequence)
	if digestErr != nil || !sourceTimeOK || !positionOK {
		return receivedContextEntry{}, false
	}
	entry := receivedContextEntry{Sequence: event.Sequence, EventID: event.ID, EntryDigest: digest, Provider: event.Provider, Connector: event.Connector, ConversationID: event.Channel.ID, ThreadID: event.Referrer.ThreadID, MessageID: event.Message.ID, CreatedAt: createdAt, SenderID: event.User.ID, SenderKind: event.User.Kind, RecordOffset: position.Offset, RecordEnd: position.EndOffset, Resources: resources}
	if resources != nil {
		return entry, true
	}
	for ordinal, resource := range event.Message.Resources {
		entry.Resources = append(entry.Resources, receivedContextResource{Ordinal: ordinal, Descriptor: uvim.ContextResource{Source: "received", SourceMessageID: event.Message.ID, SourceGeneration: cursor.Generation, SourceEntryDigest: digest, SourceSequence: event.Sequence, SourceOrdinal: ordinal, Kind: resource.Kind, Name: resource.Name, MIME: resource.MIME, SizeBytes: resource.SizeBytes, SHA256: resource.SHA256, Availability: "unrecoverable"}, State: "unrecoverable"})
	}
	return entry, true
}

func (r *receivedContextIndex) enforceCapacityLocked() bool {
	type candidate struct {
		sequence  int64
		resources int
		bytes     int
		withdrawn string
	}
	candidates := make([]candidate, 0, len(r.state.Entries)+len(r.state.WithdrawnSequences))
	resources := 0
	for sequence, entry := range r.state.Entries {
		raw, _ := json.Marshal(entry)
		candidate := candidate{sequence: sequence, resources: len(entry.Resources), bytes: len(raw)}
		candidates = append(candidates, candidate)
		resources += candidate.resources
	}
	for key, sequence := range r.state.WithdrawnSequences {
		if sequence <= 0 {
			continue
		}
		stamp, _ := json.Marshal(r.state.Withdrawn[key])
		encodedKey, _ := json.Marshal(key)
		candidate := candidate{sequence: sequence, bytes: 2*len(encodedKey) + len(stamp) + len(strconv.FormatInt(sequence, 10)) + 4, withdrawn: key}
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].sequence == candidates[j].sequence {
			return candidates[i].withdrawn < candidates[j].withdrawn
		}
		return candidates[i].sequence < candidates[j].sequence
	})
	changed := false
	evidence := len(r.state.Entries) + len(r.state.WithdrawnSequences)
	// Count actual JSON, including escaped/doubled keys and scope-order/map
	// overhead. Candidate sizes only amortize eviction; they cannot decide the
	// hard bound without a final encoded-size check.
	r.rebuildScopeOrderLocked()
	bytes := receivedContextMetadataBytes(r.state)
	for len(candidates) > 0 {
		if resources <= receivedContextResourceCapacity && evidence <= receivedContextResourceCapacity && bytes <= receivedContextResourceBytes {
			r.rebuildScopeOrderLocked()
			bytes = receivedContextMetadataBytes(r.state)
			if bytes <= receivedContextResourceBytes {
				break
			}
		}
		oldest := candidates[0]
		candidates = candidates[1:]
		if oldest.withdrawn != "" {
			if r.state.WithdrawnSequences[oldest.withdrawn] != oldest.sequence {
				continue
			}
			delete(r.state.WithdrawnSequences, oldest.withdrawn)
			delete(r.state.Withdrawn, oldest.withdrawn)
		} else {
			if _, exists := r.state.Entries[oldest.sequence]; !exists {
				continue
			}
			delete(r.state.Entries, oldest.sequence)
			resources -= oldest.resources
		}
		evidence--
		bytes -= oldest.bytes
		if r.state.LowerBound <= oldest.sequence {
			r.state.LowerBound = oldest.sequence + 1
		}
		changed = true
	}
	if r.pruneRetainedLocked(r.state.LowerBound) {
		changed = true
	}
	r.rebuildScopeOrderLocked()
	return changed
}

func receivedContextMetadataBytes(state receivedContextState) int {
	raw, err := json.Marshal(state)
	if err != nil {
		return receivedContextResourceBytes + 1
	}
	return len(raw)
}

func (r *receivedContextIndex) enforcePendingCapacityLocked() {
	type candidate struct {
		id        string
		prepared  time.Time
		resources int
		bytes     int
	}
	candidates := make([]candidate, 0, len(r.state.Pending))
	resources := 0
	for _, entry := range r.state.Entries {
		resources += len(entry.Resources)
	}
	for id, pending := range r.state.Pending {
		raw, _ := json.Marshal(pending)
		candidate := candidate{id: id, prepared: pending.PreparedAt, resources: len(pending.Resources), bytes: len(raw)}
		candidates = append(candidates, candidate)
		resources += candidate.resources
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].prepared.Before(candidates[j].prepared) })
	bytes := receivedContextMetadataBytes(r.state)
	for len(candidates) > 0 {
		if resources <= receivedContextResourceCapacity && bytes <= receivedContextResourceBytes {
			bytes = receivedContextMetadataBytes(r.state)
			if bytes <= receivedContextResourceBytes {
				break
			}
		}
		oldest := candidates[0]
		candidates = candidates[1:]
		delete(r.state.Pending, oldest.id)
		resources -= oldest.resources
		bytes -= oldest.bytes
	}
}

func isReceivedHistoryEvent(event uvim.Event) bool {
	_, hasSourceTime := uvim.EventSourceTime(event)
	return event.Type == uvim.EventMessageCreate && event.Channel.Type == uvim.ChannelGroup && event.Provider != "" && event.Connector != "" && event.Channel.ID != "" && event.Message.ID != "" && hasSourceTime && event.User.Kind != uvim.UserKindBot
}

// Ambient ingress is the narrow subset allowed to retain encrypted provider
// references for lazy materialization.  Received history is deliberately
// broader: prior addressed normal group messages remain searchable context,
// even though their resources took the eager ingress path.
func isReceivedAmbientEvent(event uvim.Event) bool {
	return isReceivedHistoryEvent(event) && !event.Addressed
}

func (r *receivedContextIndex) pruneRetainedLocked(lowerBound int64) bool {
	if lowerBound <= 0 {
		return false
	}
	changed := false
	for sequence := range r.state.Entries {
		if sequence < lowerBound {
			delete(r.state.Entries, sequence)
			changed = true
		}
	}
	for key, sequence := range r.state.WithdrawnSequences {
		if sequence < lowerBound {
			delete(r.state.WithdrawnSequences, key)
			delete(r.state.Withdrawn, key)
			changed = true
		}
	}
	for key := range r.state.Withdrawn {
		if _, retained := r.state.WithdrawnSequences[key]; !retained {
			delete(r.state.Withdrawn, key)
			changed = true
		}
	}
	return changed
}

func receivedContextMessageKey(provider, connector, conversationID, messageID string) string {
	return provider + "\x00" + connector + "\x00" + conversationID + "\x00" + messageID
}

func receivedContextScopeKey(provider, connector, conversationID, threadID string) string {
	return provider + "\x00" + connector + "\x00" + conversationID + "\x00" + threadID
}

// rebuildScopeOrderLocked stores only sequence metadata in deterministic
// per-scope descending display order. The actual message body stays in the
// EventLog and is revalidated only when a bounded query page selects it.
func (r *receivedContextIndex) rebuildScopeOrderLocked() {
	if r == nil {
		return
	}
	orders := make(map[string][]int64)
	for sequence, entry := range r.state.Entries {
		key := receivedContextScopeKey(entry.Provider, entry.Connector, entry.ConversationID, entry.ThreadID)
		orders[key] = append(orders[key], sequence)
	}
	for key, sequences := range orders {
		sort.SliceStable(sequences, func(i, j int) bool {
			left, right := r.state.Entries[sequences[i]], r.state.Entries[sequences[j]]
			if left.CreatedAt.Equal(right.CreatedAt) {
				return left.Sequence > right.Sequence
			}
			return left.CreatedAt.After(right.CreatedAt)
		})
		orders[key] = sequences
	}
	r.state.ScopeOrder = orders
}

func receivedContextRecordFromCursor(cursor uvim.ConversationContextCursor, sequence int64) (uvim.ConversationContextRecord, bool) {
	for _, record := range cursor.Records {
		if record.Sequence == sequence {
			return record, true
		}
	}
	return uvim.ConversationContextRecord{}, false
}

func sameReceivedContextEntry(entry receivedContextEntry, event uvim.Event) bool {
	digest, err := uvim.ConversationEventDigest(event)
	return err == nil && entry.Sequence == event.Sequence && entry.EventID == event.ID && entry.EntryDigest == digest && entry.Provider == event.Provider && entry.Connector == event.Connector && entry.ConversationID == event.Channel.ID && entry.ThreadID == event.Referrer.ThreadID && entry.MessageID == event.Message.ID
}
