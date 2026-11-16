package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	uvim "github.com/hengshi/uv-im-connector"
)

// ClientRegistry is the operator-owned, startup-only boundary used when one
// connector serves several independent applications. It deliberately has no
// HTTP mutation API: a caller cannot broaden its own audience or routes.
type ClientRegistry struct {
	clients map[string]scopedClient
	byRoute map[string]string
}

type scopedClientsFile struct {
	Version int            `json:"version"`
	Clients []scopedClient `json:"clients"`
}

type scopedClient struct {
	ID     string        `json:"id"`
	Token  string        `json:"token"`
	Routes []ClientRoute `json:"routes"`
}

// ClientRoute is an exact native inbound conversation selector. Proactive
// targets are separate because a direct channel ID is not generally a user
// address, and a group/channel target is not safely inferred from its string.
type ClientRoute struct {
	Provider         string                `json:"provider"`
	Connector        string                `json:"connector"`
	ConversationID   string                `json:"conversation_id"`
	ProactiveTargets []uvim.OutboundTarget `json:"proactive_targets,omitempty"`
}

// LoadClientRegistry parses the private shared-service manifest. The caller
// must keep this file outside application images and logs.
func LoadClientRegistry(path string) (*ClientRegistry, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("client registry requires an absolute path")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm()&0o077 != 0 || pathInfo.Size() > 1<<20 {
		return nil, errors.New("client registry must be a private bounded regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(pathInfo, info) {
		return nil, errors.New("client registry changed while opening")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var input scopedClientsFile
	if err := decoder.Decode(&input); err != nil {
		return nil, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("client registry must contain one JSON object")
	}
	if input.Version != 1 || len(input.Clients) == 0 {
		return nil, errors.New("client registry requires version 1 and at least one client")
	}
	registry := &ClientRegistry{clients: make(map[string]scopedClient, len(input.Clients)), byRoute: map[string]string{}}
	seenTokens := make(map[string]struct{}, len(input.Clients))
	for index, client := range input.Clients {
		if err := validateScopedClient(client); err != nil {
			return nil, fmt.Errorf("client %d: %w", index, err)
		}
		if _, exists := registry.clients[client.ID]; exists {
			return nil, fmt.Errorf("client %d repeats id", index)
		}
		if _, exists := seenTokens[client.Token]; exists {
			return nil, fmt.Errorf("client %d repeats token", index)
		}
		seenTokens[client.Token] = struct{}{}
		for _, route := range client.Routes {
			key := clientRouteKey(route.Provider, route.Connector, route.ConversationID)
			if owner, exists := registry.byRoute[key]; exists {
				return nil, fmt.Errorf("client %d route already belongs to %q", index, owner)
			}
			registry.byRoute[key] = client.ID
		}
		registry.clients[client.ID] = client
	}
	return registry, nil
}

func validateScopedClient(client scopedClient) error {
	if !safeConfigAtom(client.ID, 128) || !safeConfigAtom(client.Token, 4096) {
		return errors.New("id and token are required without surrounding whitespace")
	}
	if len(client.Routes) == 0 {
		return errors.New("at least one route is required")
	}
	for _, route := range client.Routes {
		if !safeConfigAtom(route.Provider, 128) || !safeConfigAtom(route.Connector, 128) || !safeConfigAtom(route.ConversationID, 4096) {
			return errors.New("route requires provider, connector and conversation_id without surrounding whitespace")
		}
		for _, target := range route.ProactiveTargets {
			if !validConfiguredTarget(target) {
				return errors.New("route has an invalid proactive target")
			}
		}
	}
	return nil
}

func safeConfigAtom(value string, max int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= max
}

func validConfiguredTarget(target uvim.OutboundTarget) bool {
	if !safeConfigAtom(target.ID, 4096) || !safeConfigAtom(target.Kind, 64) {
		return false
	}
	switch strings.ToLower(target.Kind) {
	case uvim.TargetUser, uvim.TargetGroup, uvim.TargetChannel, uvim.TargetConversation:
		return target.IDType == strings.TrimSpace(target.IDType) && len(target.IDType) <= 256
	default:
		return false
	}
}

func clientRouteKey(provider, connector, conversationID string) string {
	return uvim.ProviderKey(provider, connector) + "\x00" + conversationID
}

func (r *ClientRegistry) Authenticate(req *http.Request) (string, bool) {
	if r == nil || req == nil {
		return "", false
	}
	value := strings.TrimSpace(req.Header.Get("Authorization"))
	if len(value) < len("Bearer ") || !strings.EqualFold(value[:len("Bearer ")], "Bearer ") {
		return "", false
	}
	token := strings.TrimSpace(value[len("Bearer "):])
	if token == "" {
		return "", false
	}
	for id, client := range r.clients {
		if subtle.ConstantTimeCompare([]byte(token), []byte(client.Token)) == 1 {
			return id, true
		}
	}
	return "", false
}

func (r *ClientRegistry) Owner(event uvim.Event) string {
	if r == nil || event.Channel.ID == "" {
		return ""
	}
	return r.byRoute[clientRouteKey(event.Provider, event.Connector, event.Channel.ID)]
}

func (r *ClientRegistry) OwnsEvent(clientID string, event uvim.Event) bool {
	// The audience is stamped while the event enters the shared Hub. It is
	// durable authority: later operator route changes select future ingress,
	// but must never transfer (or erase) an existing caller's history, reply
	// proof, resources, or identity evidence.
	return r != nil && event.Audience != "" && event.Audience == clientID
}

func (r *ClientRegistry) AllowsProvider(clientID, provider, connector string) bool {
	if r == nil {
		return false
	}
	client, exists := r.clients[clientID]
	if !exists {
		return false
	}
	for _, route := range client.Routes {
		if uvim.ProviderKey(route.Provider, route.Connector) == uvim.ProviderKey(provider, connector) {
			return true
		}
	}
	return false
}

func (r *ClientRegistry) AllowsProactive(clientID, provider, connector string, target uvim.OutboundTarget) bool {
	if r == nil {
		return false
	}
	client, exists := r.clients[clientID]
	if !exists {
		return false
	}
	target = normalizedTarget(target)
	for _, route := range client.Routes {
		if uvim.ProviderKey(route.Provider, route.Connector) != uvim.ProviderKey(provider, connector) {
			continue
		}
		for _, allowed := range route.ProactiveTargets {
			if normalizedTarget(allowed) == target {
				return true
			}
		}
	}
	return false
}

func normalizedTarget(target uvim.OutboundTarget) uvim.OutboundTarget {
	target.ID = strings.TrimSpace(target.ID)
	target.Kind = strings.ToLower(strings.TrimSpace(target.Kind))
	target.IDType = strings.TrimSpace(target.IDType)
	return target
}

type clientPrincipalKey struct{}

func withClientPrincipal(req *http.Request, clientID string) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), clientPrincipalKey{}, clientID))
}

func clientPrincipal(req *http.Request) string {
	if req == nil {
		return ""
	}
	return clientIDFromContext(req.Context())
}

type resourceScope struct {
	ClientID       string `json:"client_id"`
	Provider       string `json:"provider,omitempty"`
	Connector      string `json:"connector,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	EventID        string `json:"event_id,omitempty"`
}

// outboundReceiptScope binds a native provider receipt to the caller that
// caused the send. It is intentionally colocated with resource ownership: both
// are durable opaque IDs in the shared provider store and both must survive a
// Hub restart without becoming bearer capabilities for another caller.
type outboundReceiptScope struct {
	ClientID  string `json:"client_id"`
	Provider  string `json:"provider"`
	Connector string `json:"connector"`
	MessageID string `json:"message_id"`
}

const resourceScopeStateMaxBytes int64 = 8 << 20

// resourceScopes is a small durable authorization index. Resource bytes remain
// in the existing provider store, while this index prevents an internal:// ID
// from becoming a bearer capability for another scoped caller.
type resourceScopes struct {
	mu     sync.Mutex
	path   string
	values map[string]resourceScope
	err    error
}

func openResourceScopes(path string) (*resourceScopes, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("resource scopes require an absolute store path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	// Persist the receipt directory before accepting any native sends. Receipts
	// grow independently of the bounded resource index and are never evicted.
	if err := os.MkdirAll(path+".receipts", 0o700); err != nil {
		return nil, err
	}
	for _, directory := range []string{path + ".receipts", filepath.Dir(path)} {
		dir, err := os.Open(directory)
		if err != nil {
			return nil, err
		}
		err = dir.Sync()
		dir.Close()
		if err != nil {
			return nil, err
		}
	}
	store := &resourceScopes{path: path, values: map[string]resourceScope{}}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > resourceScopeStateMaxBytes {
		return nil, errors.New("resource scope state is not a bounded regular file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, resourceScopeStateMaxBytes))
	decoder.DisallowUnknownFields()
	var state struct {
		Version  int                             `json:"version"`
		Values   map[string]resourceScope        `json:"values"`
		Receipts map[string]outboundReceiptScope `json:"receipts,omitempty"`
	}
	if err := decoder.Decode(&state); err != nil || state.Version != 1 || state.Values == nil {
		return nil, errors.New("resource scope state cannot be loaded")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid resource scope state trailer")
	}
	for id, scope := range state.Values {
		if !safeConfigAtom(id, 4096) || !safeConfigAtom(scope.ClientID, 128) {
			return nil, errors.New("invalid persisted resource scope")
		}
	}
	for key, receipt := range state.Receipts {
		if !validOutboundReceipt(receipt) || key != outboundReceiptKey(receipt.Provider, receipt.Connector, receipt.MessageID) {
			return nil, errors.New("invalid persisted outbound receipt scope")
		}
	}
	store.values = state.Values
	// Migrate only after validating the entire old index. A failed or interrupted
	// migration leaves the old JSON intact; identical files can be replayed.
	for _, receipt := range state.Receipts {
		if err := store.claimOutboundReceipts(receipt.ClientID, receipt.Provider, receipt.Connector, uvim.SendResult{MessageID: receipt.MessageID}); err != nil {
			return nil, err
		}
	}
	if len(state.Receipts) > 0 {
		if err := store.save(state.Values); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *resourceScopes) claim(id string, scope resourceScope) error {
	return s.claimResources([]string{id}, scope)
}

func (s *resourceScopes) claimResources(ids []string, scope resourceScope) error {
	if s == nil || !safeConfigAtom(scope.ClientID, 128) {
		return errors.New("invalid resource scope")
	}
	unique := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !safeConfigAtom(id, 4096) {
			return errors.New("invalid resource scope")
		}
		unique[id] = struct{}{}
	}
	if len(unique) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return errors.New("resource scope state requires restart")
	}
	next := make(map[string]resourceScope, len(s.values)+len(unique))
	for key, value := range s.values {
		next[key] = value
	}
	changed := false
	for id := range unique {
		if current, exists := next[id]; exists {
			// A provider can expose the same stored attachment in more than one
			// event. It remains private to the original client; a later event must
			// not turn that ordinary reuse into a delivery failure or transfer it.
			if current.ClientID != scope.ClientID {
				return errors.New("resource is already owned by another scope")
			}
			continue
		}
		next[id] = scope
		changed = true
	}
	if !changed {
		return nil
	}
	if err := s.save(next); err != nil {
		return err
	}
	s.values = next
	return nil
}

func (s *resourceScopes) claimOutboundReceipts(clientID, provider, connector string, result uvim.SendResult) error {
	if s == nil || !safeConfigAtom(clientID, 128) || !safeConfigAtom(provider, 128) || !safeConfigAtom(connector, 128) {
		return errors.New("invalid outbound receipt scope")
	}
	ids := map[string]struct{}{}
	if id := strings.TrimSpace(result.MessageID); id != "" {
		ids[id] = struct{}{}
	}
	for _, id := range result.MessageIDs {
		if id = strings.TrimSpace(id); id != "" {
			ids[id] = struct{}{}
		}
	}
	// Some provider send capabilities do not yield a native message ID. There is
	// nothing that can later be edited, so no receipt is recorded for that send.
	if len(ids) == 0 {
		return nil
	}
	for id := range ids {
		if !safeConfigAtom(id, 4096) {
			return errors.New("invalid outbound receipt")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return errors.New("resource scope state requires restart")
	}
	var pending []outboundReceiptScope
	for id := range ids {
		key := outboundReceiptKey(provider, connector, id)
		want := outboundReceiptScope{ClientID: clientID, Provider: provider, Connector: connector, MessageID: id}
		current, err := s.readOutboundReceipt(key)
		if err == nil {
			if current.ClientID != clientID {
				return errors.New("outbound receipt is already owned by another scope")
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		pending = append(pending, want)
	}
	// Check all conflicts first. On an I/O failure, only the exact receipts
	// already persisted may be used; the send remains non-retryable/unknown.
	for _, receipt := range pending {
		raw, err := json.Marshal(receipt)
		if err != nil {
			return err
		}
		key := outboundReceiptKey(receipt.Provider, receipt.Connector, receipt.MessageID)
		if err := s.saveFile(s.receiptPath(key), raw); err != nil {
			return err
		}
	}
	return nil
}

func (s *resourceScopes) ownsOutboundReceipt(clientID, provider, connector, messageID string) bool {
	if s == nil {
		return false
	}
	key := outboundReceiptKey(provider, connector, messageID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false
	}
	receipt, err := s.readOutboundReceipt(key)
	return err == nil && receipt.ClientID == clientID
}

func outboundReceiptKey(provider, connector, messageID string) string {
	return uvim.ProviderKey(provider, connector) + "\x00" + strings.TrimSpace(messageID)
}

func (s *resourceScopes) owns(id, clientID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err == nil && s.values[id].ClientID == clientID
}

func (s *resourceScopes) receiptPath(key string) string {
	return filepath.Join(s.path+".receipts", fmt.Sprintf("%x.json", sha256.Sum256([]byte(key))))
}

func validOutboundReceipt(receipt outboundReceiptScope) bool {
	return safeConfigAtom(receipt.ClientID, 128) && safeConfigAtom(receipt.Provider, 128) && safeConfigAtom(receipt.Connector, 128) && safeConfigAtom(receipt.MessageID, 4096)
}

func (s *resourceScopes) readOutboundReceipt(key string) (outboundReceiptScope, error) {
	var receipt outboundReceiptScope
	path := s.receiptPath(key)
	info, err := os.Lstat(path)
	if err != nil {
		return receipt, err
	}
	// Includes worst-case JSON escaping of all bounded fields. The hash is only
	// a filename; the complete persisted identity must match before granting use.
	const maxBytes = 32 << 10
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return receipt, errors.New("invalid outbound receipt file")
	}
	file, err := os.Open(path)
	if err != nil {
		return receipt, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	if decoder.Decode(new(any)) != io.EOF || !validOutboundReceipt(receipt) || key != outboundReceiptKey(receipt.Provider, receipt.Connector, receipt.MessageID) {
		return receipt, errors.New("invalid outbound receipt identity")
	}
	return receipt, nil
}

func (s *resourceScopes) save(values map[string]resourceScope) error {
	raw, err := json.Marshal(struct {
		Version int                      `json:"version"`
		Values  map[string]resourceScope `json:"values"`
	}{Version: 1, Values: values})
	if err != nil {
		return err
	}
	if int64(len(raw)+1) > resourceScopeStateMaxBytes {
		return errors.New("resource scope state exceeds its restartable size limit")
	}
	return s.saveFile(s.path, raw)
}

func (s *resourceScopes) saveFile(path string, raw []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".resource-scopes-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(append(raw, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		s.err = err
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		s.err = err
		return err
	}
	return nil
}

func clientIDFromContext(ctx context.Context) string {
	clientID, _ := ctx.Value(clientPrincipalKey{}).(string)
	return clientID
}
