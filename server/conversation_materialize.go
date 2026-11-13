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

// conversationMaterialRegistry holds short-lived encrypted resource references
// and completed local files. The EventLog remains the source of trigger proof;
// interrupted reads create no delivery or lifecycle state.
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
	// Cursors share this encrypted, 0600 Hub-private file with resource
	// handles. They are continuation state, not public query output or a new
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
	Descriptor       uvim.ContextResource         `json:"descriptor"`
	ResourceCipher   string                       `json:"resource_cipher"`
	Resource         uvim.ResourceRef             `json:"resource,omitempty"`
	// Prior v1 cache-loss and revocation markers remain read-only. They must
	// not become new reads after the original completed reference was cleared.
	LegacyState  string `json:"state,omitempty"`
	LegacyReason string `json:"reason,omitempty"`
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
			handle, err := conversationMaterialRandomID()
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
				Scope: scope, Source: safe.Source, SourceMessageID: safe.SourceMessageID, Descriptor: safe, ResourceCipher: ciphertext,
			}
			changed = true
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

// resolve rechecks the exact presentation. An incomplete read can be retried;
// only a complete resource is cached, never an attempt or failure.
func (r *conversationMaterialRegistry) resolve(authToken string, input uvim.ConversationMaterializeRequest) (conversationMaterialRecord, uvim.ResourceRef, error) {
	if r == nil || strings.TrimSpace(authToken) == "" {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, errors.New("conversation material registry unavailable")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, err
	}
	if r.purgeLocked(time.Now().UTC()) {
		if err := r.persistLocked(); err != nil {
			return conversationMaterialRecord{}, uvim.ResourceRef{}, err
		}
	}
	record, ok := r.state.Records[input.MaterializeHandle]
	if !ok || !sameConversationMaterialIdentity(record, input) {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, errors.New("conversation material handle unavailable")
	}
	if record.LegacyState == "revoked" || record.LegacyState == "unavailable" && record.LegacyReason == "cached_resource_unavailable" {
		return conversationMaterialRecord{}, uvim.ResourceRef{}, errors.New("completed conversation material unavailable")
	}
	resource, err := conversationMaterialDecrypt(authToken, record.ResourceCipher)
	return record, resource, err
}

// complete preserves the first verified complete value if readers race.
func (r *conversationMaterialRegistry) complete(authToken, handle string, resource uvim.ResourceRef) (conversationMaterialRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(authToken); err != nil {
		return conversationMaterialRecord{}, err
	}
	record, ok := r.state.Records[handle]
	if !ok || !time.Now().Before(record.ExpiresAt) {
		return conversationMaterialRecord{}, errors.New("conversation material handle unavailable")
	}
	if record.Resource.InternalURL != "" {
		return record, nil
	}
	previous := record
	record.Resource = resource.Sanitized()
	record.LegacyState, record.LegacyReason = "", ""
	r.state.Records[handle] = record
	if err := r.persistLocked(); err != nil {
		r.state.Records[handle] = previous
		return conversationMaterialRecord{}, err
	}
	return record, nil
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
		if record.Handle != handle || record.Version != 1 {
			delete(r.state.Records, handle)
			changed = true
			continue
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
		(input.SourceEpoch == "" || record.SourceEpoch == input.SourceEpoch) && record.Handle == input.MaterializeHandle
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
