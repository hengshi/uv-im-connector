package server

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"

	uvim "github.com/hengshi/uv-im-connector"
)

// handleInboundIdentity shares the existing authenticated identity owner.
// It never accepts a caller-selected native actor or requested business ID.
func (h *Hub) handleInboundIdentity(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if h.authToken == "" || h.identityLinks == nil || h.eventLog == nil || h.eventLog.Path == "" {
		writeError(w, http.StatusNotImplemented, "identity_links_unavailable")
		return
	}
	var input uvim.IdentityInboundResolveRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 16384))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF || input.Provider == "" || input.Connector == "" || input.EventID == "" {
		writeError(w, http.StatusBadRequest, "invalid_identity_event")
		return
	}
	provider := h.registry.Get(input.Provider, input.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	events, err := h.eventLog.ReadAfter(req.Context(), 0)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "identity_event_unavailable")
		return
	}
	var event uvim.Event
	found := false
	for _, candidate := range events {
		if candidate.ID != input.EventID || candidate.Provider != input.Provider || candidate.Connector != input.Connector {
			continue
		}
		if found {
			writeError(w, http.StatusServiceUnavailable, "identity_event_ambiguous")
			return
		}
		event, found = candidate, true
	}
	// Released normal-user adapters omit Kind; this is the same personal
	// native account contract used by identityEvent, not a display-name guess.
	if !found || event.Sequence <= 0 || !personalIdentityEvent(event) || event.User.ID == "" || (event.User.Kind != "" && event.User.Kind != uvim.UserKindUser) {
		writeError(w, http.StatusUnprocessableEntity, "personal_im_event_unavailable")
		return
	}
	result := uvim.IdentityInboundResolveResult{
		Event:       uvim.IdentityEventRef{Provider: event.Provider, Connector: event.Connector, EventID: event.ID},
		NativeActor: uvim.IdentityNativeActor{Provider: event.Provider, Connector: event.Connector, UserID: event.User.ID, Kind: uvim.UserKindUser},
	}
	result.EventDigest, err = uvim.ConversationEventDigest(event)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "identity_event_unavailable")
		return
	}
	// Direct channels require current private-participant proof. Room adapters
	// can prove a private room without pretending its target is a user ID.
	if event.Channel.Type == uvim.ChannelDirect || event.Channel.Type == uvim.ChannelRoom {
		_, target, proofErr := h.identityEvent(req.Context(), result.Event)
		if proofErr != nil {
			writeError(w, http.StatusUnprocessableEntity, "private_target_unavailable")
			return
		}
		result.DirectTarget = &target
	} else if event.Type == uvim.EventInteractionAction {
		// Callback chat classification is not supplied by the caller. The
		// provider may prove a current private target; otherwise the complete
		// actor/link set stays useful without granting direct delivery.
		if _, target, proofErr := h.identityEvent(req.Context(), result.Event); proofErr == nil {
			result.DirectTarget = &target
		}
	}
	h.identityLinks.mu.Lock()
	if h.identityLinks.err != nil {
		h.identityLinks.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "identity_link_state_requires_restart")
		return
	}
	for _, link := range h.identityLinks.links {
		if link.Revoked || link.Provider != event.Provider || link.Connector != event.Connector || link.UserID != event.User.ID {
			continue
		}
		// Identity membership belongs to the native actor. A business link may
		// have been proven from another private room; its target is not a filter
		// on the complete current identity set. Current-event delivery scope is
		// proved separately above and never inferred from a preferred link.
		result.Links = append(result.Links, uvim.IdentityInboundLink{Identity: link.Identity, Link: link.IdentityLinkRef, Target: link.Target})
	}
	h.identityLinks.mu.Unlock()
	if len(result.Links) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "identity_link_unavailable")
		return
	}
	sort.Slice(result.Links, func(i, j int) bool { return result.Links[i].Link.ID < result.Links[j].Link.ID })
	writeJSON(w, http.StatusOK, result)
}

func personalIdentityEvent(event uvim.Event) bool {
	if event.Type == uvim.EventMessageCreate {
		return event.Message.ID != ""
	}
	return event.Type == uvim.EventInteractionAction && uvim.ValidateInteractionEvent(event) == nil
}
