package server

import (
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
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
	conversationMaterializeTTL      = conversationSearchCursorTTL
	conversationMaterializeCapacity = 20_000
	conversationMaterializeBytes    = 8 << 20
)

// conversationMaterialRegistry is a Hub-private, durable presentation journal.
// The EventLog remains the source of trigger evidence; this journal contains
// only short-lived encrypted provider references and exact-one-attempt state.
// A record is never reconstructed from a public descriptor after it expires.
type conversationMaterialRegistry struct {
	path string

	mu     sync.Mutex
	loaded bool
	state  conversationMaterialState
}

type conversationMaterialState struct {
	Version int                                   `json:"version"`
	Records map[string]conversationMaterialRecord `json:"records"`
	// Cursors share this encrypted, 0600 Hub-private journal with material
	// plans. They are continuation state, not public query output or a new
	// Task/Run authority owner.
	Cursors map[string]conversationSearchCursorRecord `json:"cursors"`
}

type conversationMaterialRecord struct {
	Version          int                          `json:"version"`
	Handle           string                       `json:"handle"`
	SourceEpoch      string                       `json:"source_epoch"`
	IssuedAt         time.Time                    `json:"issued_at"`
	ExpiresAt        time.Time                    `json:"expires_at"`
	Provider         string                       `json:"provider"`
	Connector        string                       `json:"connector"`
	TriggerEventID   string                       `json:"trigger_event_id"`
	TriggerMessageID string                       `json:"trigger_message_id"`
	EventDigest      string                       `json:"event_digest"`
	AudienceDigest   string                       `json:"audience_digest"`
	ScopeHash        string                       `json:"scope_hash"`
	Scope            uvim.ConversationSearchScope `json:"scope"`
	Source           string                       `json:"source"`
	SourceMessageID  string                       `json:"source_message_id"`
	// PhysicalCacheKey is an opaque hash of the private provider resource
	// identity. It may only reuse bytes after this record's own source has been
	// revalidated; it never merges handles or provenance records.
	PhysicalCacheKey string               `json:"physical_cache_key,omitempty"`
	Descriptor       uvim.ContextResource `json:"descriptor"`
	ResourceCipher   string               `json:"resource_cipher"`
	State            string               `json:"state"`
	AttemptedAt      *time.Time           `json:"attempted_at,omitempty"`
	SettledAt        *time.Time           `json:"settled_at,omitempty"`
	Reason           string               `json:"reason,omitempty"`
	Resource         uvim.ResourceRef     `json:"resource,omitempty"`
}

func newConversationMaterialRegistry(eventLog *uvim.EventLog) *conversationMaterialRegistry {
	path := ""
	if eventLog != nil && strings.TrimSpace(eventLog.Path) != "" {
		path = filepath.Join(filepath.Dir(eventLog.Path), "state", "conversation-context", "materializations.v1.json")
	}
	return &conversationMaterialRegistry{path: path, state: conversationMaterialState{Version: 1, Records: map[string]conversationMaterialRecord{}, Cursors: map[string]conversationSearchCursorRecord{}}}
}

// present mints a fresh opaque source epoch/handle for each selected resource.
// The handle is a presentation identity, not a provider resource identity.
func (r *conversationMaterialRegistry) present(authToken string, input uvim.ConversationSearchRequest, trigger uvim.Event, result *uvim.ConversationSearchResult) error {
	if r == nil || result == nil || len(result.Items) == 0 || strings.TrimSpace(authToken) == "" {
		return errors.New("conversation material registry unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return err
	}
	now := time.Now().UTC()
	changed := r.purgeLocked(now)
	scope := uvim.ConversationSearchScope{
		Provider: result.Provider, Connector: result.Connector, ConversationID: result.ConversationID,
		ThreadID: result.ThreadID, TriggerMessageID: result.TriggerMessageID, AnchorTime: result.AnchorTime,
	}
	scopeHash := conversationMaterialScopeHash(scope, trigger)
	seen := map[string]string{}
	for itemIndex := range result.Items {
		item := &result.Items[itemIndex]
		for resourceIndex := range item.Resources {
			descriptor := &item.Resources[resourceIndex]
			private := descriptor.Resource
			if strings.TrimSpace(private.Key) == "" && len(private.Private) == 0 {
				descriptor.Resource = uvim.ResourceRef{}
				if descriptor.Availability == "" {
					descriptor.Availability = "unavailable"
				}
				continue
			}
			private.Provider = uvim.FirstNonEmpty(private.Provider, input.Provider)
			private.Connector = uvim.FirstNonEmpty(private.Connector, input.Connector)
			private.SourceMessageID = uvim.FirstNonEmpty(private.SourceMessageID, item.MessageID)
			if private.Metadata == nil {
				private.Metadata = map[string]string{}
			}
			private.Metadata["message_id"] = private.SourceMessageID
			descriptor.Source = uvim.FirstNonEmpty(descriptor.Source, result.Source, "native")
			descriptor.SourceMessageID = uvim.FirstNonEmpty(descriptor.SourceMessageID, private.SourceMessageID, item.MessageID)
			key := conversationMaterialPresentationKey(*descriptor, private)
			handle := seen[key]
			var err error
			if handle == "" {
				handle, err = conversationMaterialRandomID()
				if err != nil {
					return err
				}
				epoch, err := conversationMaterialRandomID()
				if err != nil {
					return err
				}
				ciphertext, err := conversationMaterialEncrypt(authToken, private)
				if err != nil {
					return err
				}
				safe := *descriptor
				safe.MaterializeHandle, safe.SourceEpoch = "", ""
				safe.Source = descriptor.Source
				safe.SourceMessageID = descriptor.SourceMessageID
				safe.Availability = "available"
				safe.Resource = uvim.ResourceRef{}
				r.state.Records[handle] = conversationMaterialRecord{
					Version: 1, Handle: handle, SourceEpoch: epoch, IssuedAt: now, ExpiresAt: now.Add(conversationMaterializeTTL),
					Provider: input.Provider, Connector: input.Connector, TriggerEventID: input.TriggerEventID, TriggerMessageID: input.TriggerMessageID,
					EventDigest: input.ExpectedEventDigest, AudienceDigest: input.AudienceDigest, ScopeHash: scopeHash,
					Scope: scope, Source: safe.Source, SourceMessageID: safe.SourceMessageID, PhysicalCacheKey: conversationMaterialPhysicalCacheKey(private), Descriptor: safe, ResourceCipher: ciphertext, State: "active",
				}
				seen[key] = handle
				changed = true
			}
			descriptor.MaterializeHandle = handle
			descriptor.SourceEpoch = r.state.Records[handle].SourceEpoch
			descriptor.Availability = "available"
			descriptor.Resource = uvim.ResourceRef{}
		}
	}
	if r.enforceCapacityLocked() {
		changed = true
	}
	if changed {
		return r.persistLocked()
	}
	return nil
}

func (r *conversationMaterialRegistry) reserve(authToken string, input uvim.ConversationMaterializeRequest) (conversationMaterialRecord, uvim.ResourceRef, bool, error) {
	if r == nil || strings.TrimSpace(authToken) == "" {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, false, errors.New("conversation material registry unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, false, err
	}
	now := time.Now().UTC()
	changed := r.purgeLocked(now)
	record, ok := r.state.Records[input.MaterializeHandle]
	if !ok || !sameConversationMaterialIdentity(record, input) {
		if changed {
			_ = r.persistLocked()
		}
		return conversationMaterialRecord{}, uvim.ResourceRef{}, false, nil
	}
	if record.State == "effect_started" {
		record.State, record.Reason = "unknown", "attempt_interrupted"
		settled := now
		record.SettledAt = &settled
		r.state.Records[record.Handle] = record
		if err := r.persistLocked(); err != nil {
			return conversationMaterialRecord{}, uvim.ResourceRef{}, false, err
		}
		return record, uvim.ResourceRef{}, false, nil
	}
	if record.State != "active" {
		if changed {
			if err := r.persistLocked(); err != nil {
				return conversationMaterialRecord{}, uvim.ResourceRef{}, false, err
			}
		}
		return record, uvim.ResourceRef{}, false, nil
	}
	resource, err := conversationMaterialDecrypt(authToken, record.ResourceCipher)
	if err != nil {
		record.State, record.Reason = "unavailable", "handle_not_recoverable"
		settled := now
		record.SettledAt = &settled
		r.state.Records[record.Handle] = record
		if persistErr := r.persistLocked(); persistErr != nil {
			return conversationMaterialRecord{}, uvim.ResourceRef{}, false, persistErr
		}
		return record, uvim.ResourceRef{}, false, nil
	}
	record.State, record.Reason = "effect_started", ""
	attempted := now
	record.AttemptedAt = &attempted
	r.state.Records[record.Handle] = record
	if err := r.persistLocked(); err != nil {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, false, err
	}
	return record, resource, true, nil
}

func (r *conversationMaterialRegistry) settle(authToken, handle, state, reason string, resource uvim.ResourceRef) (conversationMaterialRecord, error) {
	if r == nil || strings.TrimSpace(authToken) == "" {
		return conversationMaterialRecord{}, errors.New("conversation material registry unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationMaterialRecord{}, err
	}
	record, ok := r.state.Records[handle]
	if !ok {
		return conversationMaterialRecord{}, errors.New("conversation material handle unavailable")
	}
	if record.State != "effect_started" {
		return record, nil
	}
	record.State, record.Reason = state, reason
	settled := time.Now().UTC()
	record.SettledAt = &settled
	if state == "ready" {
		record.Resource = resource.Sanitized()
	}
	r.state.Records[handle] = record
	if err := r.persistLocked(); err != nil {
		return conversationMaterialRecord{}, err
	}
	return record, nil
}

// invalidateReady records that a formerly ready local result no longer has
// current authority.  It deliberately never returns a record to active: an
// opaque materialization handle has exactly one provider attempt.
func (r *conversationMaterialRegistry) invalidateReady(authToken, handle, state, reason string) (conversationMaterialRecord, error) {
	if r == nil || strings.TrimSpace(authToken) == "" {
		return conversationMaterialRecord{}, errors.New("conversation material registry unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationMaterialRecord{}, err
	}
	record, ok := r.state.Records[handle]
	if !ok {
		return conversationMaterialRecord{}, errors.New("conversation material handle unavailable")
	}
	if record.State != "ready" {
		return record, nil
	}
	record.State, record.Reason, record.Resource = state, reason, uvim.ResourceRef{}
	settled := time.Now().UTC()
	record.SettledAt = &settled
	r.state.Records[handle] = record
	if err := r.persistLocked(); err != nil {
		return conversationMaterialRecord{}, err
	}
	return record, nil
}

// readyResource returns a prior, ready physical resource only for a different
// provenance record with the same opaque physical fingerprint. Callers must
// validate their own record's trigger/source before using it; this function is
// never a substitute for source authority.
func (r *conversationMaterialRegistry) readyResource(authToken string, record conversationMaterialRecord) (uvim.ResourceRef, bool, error) {
	if r == nil || strings.TrimSpace(authToken) == "" || record.PhysicalCacheKey == "" {
		return uvim.ResourceRef{}, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return uvim.ResourceRef{}, false, err
	}
	changed := r.purgeLocked(time.Now().UTC())
	if changed {
		if err := r.persistLocked(); err != nil {
			return uvim.ResourceRef{}, false, err
		}
	}
	knownCacheLoss := false
	for handle, candidate := range r.state.Records {
		if handle != record.Handle && candidate.PhysicalCacheKey == record.PhysicalCacheKey && candidate.State == "unavailable" && candidate.Reason == "cached_resource_unavailable" {
			knownCacheLoss = true
		}
		if handle == record.Handle || candidate.State != "ready" || candidate.PhysicalCacheKey != record.PhysicalCacheKey || strings.TrimSpace(candidate.Resource.InternalURL) == "" {
			continue
		}
		return candidate.Resource.Sanitized(), true, nil
	}
	if knownCacheLoss {
		return uvim.ResourceRef{}, false, errConversationMaterialCacheUnavailable
	}
	return uvim.ResourceRef{}, false, nil
}

func (r *conversationMaterialRegistry) loadLocked(authToken string) error {
	if r.loaded {
		return nil
	}
	r.loaded = true
	r.state = conversationMaterialState{Version: 1, Records: map[string]conversationMaterialRecord{}, Cursors: map[string]conversationSearchCursorRecord{}}
	if strings.TrimSpace(r.path) == "" {
		return errors.New("conversation material journal unavailable")
	}
	raw, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &r.state); err != nil || r.state.Version != 1 {
		return errors.New("conversation material journal invalid")
	}
	if r.state.Records == nil {
		r.state.Records = map[string]conversationMaterialRecord{}
	}
	if r.state.Cursors == nil {
		r.state.Cursors = map[string]conversationSearchCursorRecord{}
	}
	changed := false
	for handle, record := range r.state.Records {
		if record.Handle != handle || record.Version != 1 || record.State == "" {
			delete(r.state.Records, handle)
			changed = true
			continue
		}
		if record.State == "effect_started" {
			now := time.Now().UTC()
			record.State, record.Reason, record.SettledAt = "unknown", "attempt_interrupted", &now
			r.state.Records[handle] = record
			changed = true
		}
	}
	for id, cursor := range r.state.Cursors {
		if !validConversationSearchCursorRecord(cursor, id) {
			delete(r.state.Cursors, id)
			changed = true
		}
	}
	if changed {
		return r.persistLocked()
	}
	return nil
}

func (r *conversationMaterialRegistry) persistLocked() error {
	if strings.TrimSpace(r.path) == "" {
		return errors.New("conversation material journal unavailable")
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

func (r *conversationMaterialRegistry) purgeLocked(now time.Time) bool {
	changed := false
	for handle, record := range r.state.Records {
		if !record.ExpiresAt.After(now) {
			delete(r.state.Records, handle)
			changed = true
		}
	}
	for id, cursor := range r.state.Cursors {
		if !cursor.ExpiresAt.After(now) {
			delete(r.state.Cursors, id)
			changed = true
		}
	}
	return changed
}

func (r *conversationMaterialRegistry) enforceCapacityLocked() bool {
	if len(r.state.Records) == 0 {
		return false
	}
	type candidate struct {
		handle string
		issued time.Time
	}
	candidates := make([]candidate, 0, len(r.state.Records))
	bytes := 0
	for handle, record := range r.state.Records {
		candidates = append(candidates, candidate{handle: handle, issued: record.IssuedAt})
		raw, _ := json.Marshal(record)
		bytes += len(raw)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].issued.Before(candidates[j].issued) })
	changed := false
	for len(candidates) > conversationMaterializeCapacity || bytes > conversationMaterializeBytes {
		oldest := candidates[0]
		candidates = candidates[1:]
		raw, _ := json.Marshal(r.state.Records[oldest.handle])
		bytes -= len(raw)
		delete(r.state.Records, oldest.handle)
		changed = true
	}
	return changed
}

func sameConversationMaterialIdentity(record conversationMaterialRecord, input uvim.ConversationMaterializeRequest) bool {
	return record.Provider == input.Provider && record.Connector == input.Connector &&
		record.TriggerEventID == input.TriggerEventID && record.TriggerMessageID == input.TriggerMessageID &&
		record.EventDigest == input.ExpectedEventDigest && record.AudienceDigest == input.AudienceDigest &&
		record.SourceEpoch == input.SourceEpoch && record.Handle == input.MaterializeHandle
}

func conversationMaterialPhysicalCacheKey(ref uvim.ResourceRef) string {
	// A correlation ID is an ingress relation, not byte authority.  In
	// particular, conflicting IDs are split after received-context prepares its
	// encrypted provider reference.  Never let an original ID with an unknown
	// seal join two private resources in the physical cache.
	identity := ""
	if strings.TrimSpace(ref.SHA256) != "" && ref.SizeBytes > 0 {
		identity = "sha256:" + strings.ToLower(strings.TrimSpace(ref.SHA256)) + "\x00" + strconv.FormatInt(ref.SizeBytes, 10)
	} else if strings.TrimSpace(ref.Key) != "" || strings.TrimSpace(ref.Secret) != "" || len(ref.Private) != 0 {
		// Key, secret, and provider-private selectors together identify the
		// provider object.  json.Marshal orders string map keys, giving the
		// private map a stable, non-public cache fingerprint without making a
		// filename, URL, or correlation ID authoritative.
		private, err := json.Marshal(struct {
			Key     string            `json:"key,omitempty"`
			Secret  string            `json:"secret,omitempty"`
			Private map[string]string `json:"private,omitempty"`
		}{Key: ref.Key, Secret: ref.Secret, Private: ref.Private})
		if err == nil {
			identity = "private:" + ref.Provider + "\x00" + ref.Connector + "\x00" + string(private)
		}
	}
	if identity == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

// conversationMaterialPresentationKey keeps handle/epoch identity one-to-one
// with a descriptor's source record. It deliberately includes received
// generation/digest/sequence/ordinal and source-message identity; physical
// byte identity may be reused only later, after this record revalidates.
func conversationMaterialPresentationKey(descriptor uvim.ContextResource, private uvim.ResourceRef) string {
	return strings.Join([]string{
		descriptor.Source,
		descriptor.SourceMessageID,
		descriptor.SourceGeneration,
		descriptor.SourceEntryDigest,
		strconv.FormatInt(descriptor.SourceSequence, 10),
		strconv.Itoa(descriptor.SourceOrdinal),
		conversationMaterialPhysicalCacheKey(private),
	}, "\x00")
}

func conversationMaterialScopeHash(scope uvim.ConversationSearchScope, trigger uvim.Event) string {
	value := strings.Join([]string{scope.Provider, scope.Connector, scope.ConversationID, scope.ThreadID, scope.TriggerMessageID, trigger.ID, trigger.Message.ID, conversationSearchAnchor(trigger).Format(time.RFC3339Nano)}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func conversationMaterialRandomID() (string, error) {
	var random [32]byte
	if _, err := io.ReadFull(crand.Reader, random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}

func conversationMaterialEncrypt(authToken string, resource uvim.ResourceRef) (string, error) {
	private := conversationMaterialPrivateResource{Resource: resource, Secret: resource.Secret, Private: resource.Private}
	private.Resource.Secret, private.Resource.Private = "", nil
	raw, err := json.Marshal(private)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(conversationMaterialKey(authToken))
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(crand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := aead.Seal(nil, nonce, raw, []byte("uv-im-conversation-material-v1"))
	return base64.RawURLEncoding.EncodeToString(append(nonce, ciphertext...)), nil
}

func conversationMaterialDecrypt(authToken, encoded string) (uvim.ResourceRef, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return uvim.ResourceRef{}, err
	}
	block, err := aes.NewCipher(conversationMaterialKey(authToken))
	if err != nil {
		return uvim.ResourceRef{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(raw) < aead.NonceSize() {
		return uvim.ResourceRef{}, errors.New("invalid conversation material cipher")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("uv-im-conversation-material-v1"))
	if err != nil {
		return uvim.ResourceRef{}, err
	}
	var private conversationMaterialPrivateResource
	if err := json.Unmarshal(plain, &private); err != nil {
		return uvim.ResourceRef{}, err
	}
	private.Resource.Secret, private.Resource.Private = private.Secret, private.Private
	return private.Resource, nil
}

func conversationMaterialEncryptCursorToken(authToken, token string) (string, error) {
	if strings.TrimSpace(token) == "" {
		return "", errors.New("conversation search continuation is required")
	}
	block, err := aes.NewCipher(conversationMaterialKey(authToken))
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(crand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := aead.Seal(nil, nonce, []byte(token), []byte("uv-im-conversation-search-cursor-v1"))
	return base64.RawURLEncoding.EncodeToString(append(nonce, ciphertext...)), nil
}

func conversationMaterialDecryptCursorToken(authToken, encoded string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(conversationMaterialKey(authToken))
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(raw) < aead.NonceSize() {
		return "", errors.New("invalid conversation search cursor cipher")
	}
	plain, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("uv-im-conversation-search-cursor-v1"))
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

type conversationMaterialPrivateResource struct {
	Resource uvim.ResourceRef  `json:"resource"`
	Secret   string            `json:"secret,omitempty"`
	Private  map[string]string `json:"private,omitempty"`
}

func conversationMaterialKey(authToken string) []byte {
	sum := sha256.Sum256([]byte("uv-im-conversation-material-v1\x00" + authToken))
	return sum[:]
}
