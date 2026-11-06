package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

type identityLinks struct {
	mu    sync.Mutex
	path  string
	lock  *os.File
	links map[string]uvim.IdentityLink
	err   error // A failed directory sync after rename has an uncertain durable outcome.
}

// EnableIdentityLinks opens the single-writer store belonging to this Hub.
// Existing static identity mappings and event history are never rewritten.
func (h *Hub) EnableIdentityLinks(path string) error {
	if h.identityLinks != nil || !filepath.IsAbs(path) {
		return errors.New("identity links require one absolute store path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return errors.New("identity link state is already owned by another process")
	}
	store := &identityLinks{path: path, lock: lock, links: map[string]uvim.IdentityLink{}}
	file, err := os.Open(path)
	if err == nil {
		defer file.Close()
		if info, statErr := file.Stat(); statErr != nil || !info.Mode().IsRegular() || info.Size() >= 8<<20 {
			lock.Close()
			return errors.New("identity link state is not a bounded regular file")
		}
		var state struct {
			Version int                          `json:"version"`
			Links   map[string]uvim.IdentityLink `json:"links"`
		}
		decoder := json.NewDecoder(io.LimitReader(file, 8<<20))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&state)
		if err == nil && state.Version != 1 {
			err = errors.New("unsupported identity link state version")
		}
		if err == nil {
			var extra any
			if decoder.Decode(&extra) != io.EOF {
				err = errors.New("invalid identity link state trailer")
			}
		}
		if err == nil {
			for key, link := range state.Links {
				if !validIdentity(link.Identity) || key != identityLinkKey(link.Identity, link.Provider, link.Connector) || link.ID != key || link.Revision == "" || link.UserID == "" || link.VerifiedAt.IsZero() || link.Target.ID == "" || link.Target.Kind == "" || link.Origin.Provider != link.Provider || link.Origin.Connector != link.Connector || link.Origin.EventID == "" || link.Proof == "" {
					err = errors.New("invalid persisted identity link")
					break
				}
				if link.Account != nil && (!validIdentity(*link.Account) || link.Account.Scope != link.Identity.Scope) {
					err = errors.New("invalid persisted account alias")
					break
				}
				if !link.Revoked && link.Account != nil {
					if other, found := store.find(*link.Account, link.Provider, link.Connector); found && !other.Revoked && other.ID != link.ID {
						err = errors.New("ambiguous persisted account alias")
						break
					}
				}
				if !link.Revoked {
					if other, found := store.find(link.Identity, link.Provider, link.Connector); found && !other.Revoked && other.ID != link.ID {
						err = errors.New("ambiguous persisted business identity")
						break
					}
				}
				store.links[key] = link
			}
		}
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		lock.Close()
		return fmt.Errorf("identity link state cannot be loaded: %w", err)
	}
	h.identityLinks = store
	return nil
}

func (h *Hub) CloseIdentityLinks() error {
	if h.identityLinks == nil {
		return nil
	}
	return h.identityLinks.lock.Close()
}

func validIdentity(identity uvim.UserIdentity) bool {
	return strings.TrimSpace(identity.Type) != "" && strings.TrimSpace(identity.Scope) != "" && strings.TrimSpace(identity.ID) != "" && len(identity.Type)+len(identity.Scope)+len(identity.ID) <= 4096
}

func identityLinkKey(identity uvim.UserIdentity, provider, connector string) string {
	data, _ := json.Marshal([]string{identity.Type, identity.Scope, identity.ID, provider, connector})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sameIdentity(a, b uvim.UserIdentity) bool {
	return a.Type == b.Type && a.Scope == b.Scope && a.ID == b.ID
}

// A verified login is an alias of the stable native ID, never a display name.
func (s *identityLinks) find(identity uvim.UserIdentity, provider, connector string) (uvim.IdentityLink, bool) {
	var revoked uvim.IdentityLink
	foundRevoked := false
	for _, link := range s.links {
		if link.Provider != provider || link.Connector != connector || (!sameIdentity(link.Identity, identity) && (link.Account == nil || !sameIdentity(*link.Account, identity))) {
			continue
		}
		if !link.Revoked {
			return link, true
		}
		revoked, foundRevoked = link, true
	}
	return revoked, foundRevoked
}

func (s *identityLinks) save(next map[string]uvim.IdentityLink) error {
	data, err := json.Marshal(struct {
		Version int                          `json:"version"`
		Links   map[string]uvim.IdentityLink `json:"links"`
	}{1, next})
	if err != nil {
		return err
	}
	if len(data) >= 8<<20 {
		return errors.New("identity link state size limit reached")
	}
	file, err := os.CreateTemp(filepath.Dir(s.path), ".identity-links-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), s.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		s.err = err
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		s.err = err
		return err
	}
	s.links = next
	return nil
}

func (h *Hub) identityEvent(ctx context.Context, ref uvim.IdentityEventRef) (uvim.Event, uvim.OutboundTarget, error) {
	if ref.Provider == "" || ref.Connector == "" || ref.EventID == "" {
		return uvim.Event{}, uvim.OutboundTarget{}, errors.New("native IM event reference is required")
	}
	provider := h.registry.Get(ref.Provider, ref.Connector)
	if provider == nil {
		return uvim.Event{}, uvim.OutboundTarget{}, errors.New("provider_not_found")
	}
	events, err := h.eventLog.ReadAfter(ctx, 0)
	if err != nil {
		return uvim.Event{}, uvim.OutboundTarget{}, err
	}
	for _, event := range events {
		if event.ID != ref.EventID || event.Provider != ref.Provider || event.Connector != ref.Connector {
			continue
		}
		if !personalIdentityEvent(event) {
			return uvim.Event{}, uvim.OutboundTarget{}, errors.New("personal IM message event is required")
		}
		if event.User.ID == "" || (event.User.Kind != "" && event.User.Kind != uvim.UserKindUser) {
			return uvim.Event{}, uvim.OutboundTarget{}, errors.New("personal IM account is required")
		}
		if !provider.Capabilities().ProactiveDirect {
			return uvim.Event{}, uvim.OutboundTarget{}, errors.New("proactive private messages are not supported")
		}
		var target uvim.OutboundTarget
		if private, ok := provider.(uvim.PrivateTargetResolver); ok {
			var err error
			target, err = private.PrivateTarget(ctx, event)
			if err != nil {
				return uvim.Event{}, uvim.OutboundTarget{}, err
			}
		} else if event.Type != uvim.EventMessageCreate || event.Channel.Type != uvim.ChannelDirect || event.Referrer.Target == nil {
			return uvim.Event{}, uvim.OutboundTarget{}, errors.New("verified private IM target is required")
		} else {
			target = *event.Referrer.Target
		}
		if err := uvim.ValidateOutboundTarget(uvim.OutboundMessage{Provider: ref.Provider, Connector: ref.Connector, Target: &target, ChannelType: uvim.ChannelDirect}, provider.Capabilities()); err != nil {
			return uvim.Event{}, uvim.OutboundTarget{}, err
		}
		return event, target, nil
	}
	return uvim.Event{}, uvim.OutboundTarget{}, errors.New("native IM event was not found")
}

func (h *Hub) handleIdentity(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if h.authToken == "" || h.identityLinks == nil {
		writeError(w, http.StatusNotImplemented, "identity_links_unavailable")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 16384))
	decoder.DisallowUnknownFields()
	if req.URL.Path == "/v1/identity.resolve" {
		var input uvim.IdentityResolveRequest
		if decoder.Decode(&input) != nil || !validIdentity(input.Identity) || input.Provider == "" || input.Connector == "" {
			writeError(w, http.StatusBadRequest, "invalid_identity_query")
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid_identity_query")
			return
		}
		if input.Event != nil {
			if input.Event.Provider != input.Provider || input.Event.Connector != input.Connector {
				writeError(w, http.StatusBadRequest, "identity_event_connector_mismatch")
				return
			}
			if _, _, err := h.identityEvent(req.Context(), *input.Event); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		h.identityLinks.mu.Lock()
		link, exists := h.identityLinks.find(input.Identity, input.Provider, input.Connector)
		storeErr := h.identityLinks.err
		h.identityLinks.mu.Unlock()
		if storeErr != nil {
			writeError(w, http.StatusServiceUnavailable, "identity_link_state_requires_restart")
			return
		}
		result := uvim.IdentityResolveResult{Found: exists && !link.Revoked, Revoked: exists && link.Revoked}
		if exists {
			result.Link = &link
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	var input uvim.IdentityLinkRequest
	if decoder.Decode(&input) != nil || !validIdentity(input.Identity) || (input.Account != nil && (!validIdentity(*input.Account) || input.Account.Scope != input.Identity.Scope)) {
		writeError(w, http.StatusBadRequest, "invalid_identity_link")
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_identity_link")
		return
	}
	event, target, err := h.identityEvent(req.Context(), input.Event)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if age := time.Since(event.Time); age > 15*time.Minute || age < -time.Minute {
		writeError(w, http.StatusBadRequest, "recent_native_IM_event_required")
		return
	}
	store := h.identityLinks
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.err != nil {
		writeError(w, http.StatusServiceUnavailable, "identity_link_state_requires_restart")
		return
	}
	key := identityLinkKey(input.Identity, event.Provider, event.Connector)
	current, exists := store.links[key]
	if other, found := store.find(input.Identity, event.Provider, event.Connector); found && !other.Revoked && other.ID != key {
		writeError(w, http.StatusConflict, "identity_is_an_alias_of_another_link")
		return
	}
	if input.Account != nil {
		if alias, found := store.find(*input.Account, event.Provider, event.Connector); found && !alias.Revoked && alias.ID != key {
			writeError(w, http.StatusConflict, "account_already_linked_to_another_identity")
			return
		}
	}
	if exists && !current.Revoked && current.UserID != event.User.ID {
		writeError(w, http.StatusConflict, "identity_already_linked_to_another_user")
		return
	}
	unlink := req.URL.Path == "/v1/identity.unlink"
	if unlink && (!exists || current.Revoked) {
		writeJSON(w, http.StatusOK, uvim.IdentityResolveResult{Revoked: exists})
		return
	}
	if !unlink && (strings.TrimSpace(input.Proof) == "" || len(input.Proof) > 4096) {
		writeError(w, http.StatusBadRequest, "verified_external_event_reference_required")
		return
	}
	if !unlink && exists && !current.Revoked && current.Target == target && ((current.Account == nil && input.Account == nil) || (current.Account != nil && input.Account != nil && sameIdentity(*current.Account, *input.Account))) {
		writeJSON(w, http.StatusOK, uvim.IdentityResolveResult{Found: true, Link: &current})
		return
	}
	link := uvim.IdentityLink{IdentityLinkRef: uvim.IdentityLinkRef{ID: key, Revision: uvim.NewID("link")},
		Identity: input.Identity, Account: input.Account, Provider: event.Provider, Connector: event.Connector, UserID: event.User.ID,
		Target: target, Origin: input.Event, Proof: input.Proof, VerifiedAt: time.Now().UTC()}
	if unlink {
		if current.UserID != event.User.ID {
			writeError(w, http.StatusForbidden, "identity_link_owner_mismatch")
			return
		}
		link = current
		link.Revoked = true
	}
	next := make(map[string]uvim.IdentityLink, len(store.links)+1)
	for id, value := range store.links {
		next[id] = value
	}
	next[key] = link
	if err := store.save(next); err != nil {
		writeError(w, http.StatusInternalServerError, "identity_link_save_failed")
		return
	}
	writeJSON(w, http.StatusOK, uvim.IdentityResolveResult{Found: !link.Revoked, Revoked: link.Revoked, Link: &link})
}

func (h *Hub) validateIdentityLink(ctx context.Context, msg uvim.OutboundMessage) error {
	if msg.IdentityLink == nil {
		return nil
	}
	if h.identityLinks == nil || h.authToken == "" {
		return errors.New("identity_links_unavailable")
	}
	h.identityLinks.mu.Lock()
	link, exists := h.identityLinks.links[msg.IdentityLink.ID]
	storeErr := h.identityLinks.err
	h.identityLinks.mu.Unlock()
	if storeErr != nil {
		return errors.New("identity_link_state_requires_restart")
	}
	if !exists || link.Revoked || link.IdentityLinkRef != *msg.IdentityLink || link.Provider != msg.Provider || link.Connector != msg.Connector || link.Target != msg.ResolvedTarget() {
		return errors.New("identity_link_is_stale")
	}
	// Native room audience can change after linking. Reuse the same adapter
	// proof before allowing another send, rather than trusting the old room ID.
	if provider := h.registry.Get(link.Provider, link.Connector); provider != nil {
		if _, ok := provider.(uvim.PrivateTargetResolver); ok {
			_, target, err := h.identityEvent(ctx, link.Origin)
			if err != nil || target != link.Target {
				return errors.New("identity_link_private_target_changed")
			}
		}
	}
	return nil
}
