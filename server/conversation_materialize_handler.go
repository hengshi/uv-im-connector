package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

var errConversationMaterialCacheUnavailable = errors.New("conversation material cache unavailable")

func (h *Hub) handleConversationMaterialize(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if !h.conversationWindowAuthOK(req) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var input uvim.ConversationMaterializeRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_conversation_materialize_request")
		return
	}
	trimConversationMaterializeRequest(&input)
	if !validConversationMaterializeRequest(input) {
		writeError(w, http.StatusBadRequest, "invalid_conversation_materialize_request")
		return
	}
	provider := h.registry.Get(input.Provider, input.Connector)
	if provider == nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("unsupported"))
		return
	}
	searchInput := uvim.ConversationSearchRequest{
		Provider: input.Provider, Connector: input.Connector, TriggerEventID: input.TriggerEventID, TriggerMessageID: input.TriggerMessageID,
		ExpectedEventDigest: input.ExpectedEventDigest, AudienceDigest: input.AudienceDigest,
	}
	trigger, digest, reason := h.conversationSearchTrigger(req.Context(), searchInput)
	if reason != "" || digest != input.ExpectedEventDigest {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("trigger_unavailable"))
		return
	}
	record, resource, err := h.materialResources.resolve(h.authToken, input)
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
		return
	}
	if record.ScopeHash != conversationMaterialScopeHash(record.Scope, trigger) || record.Scope.Provider != trigger.Provider || record.Scope.Connector != trigger.Connector || record.Scope.ConversationID != trigger.Channel.ID || record.Scope.TriggerMessageID != trigger.Message.ID || !record.Scope.AnchorTime.Equal(conversationSearchAnchor(trigger)) {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("scope_unavailable"))
		return
	}
	var downloaded uvim.ResourceRef
	// Each read gets its own storage object. A repeated provider reference or
	// concurrent attempt must not truncate a previously completed cached file.
	resource.ID = uvim.NewID("res")
	if record.Source == "received" {
		source, sourceErr := h.receivedContext.materializeEvent(req.Context(), record, trigger)
		if sourceErr != nil {
			err = sourceErr
		} else if !h.receivedMaterializeAvailable(provider) {
			err = uvim.ErrConversationResourceUnavailable
		} else if record.Resource.InternalURL == "" {
			downloaded, err = provider.Download(req.Context(), uvim.ResourceDownloadRequest{Resource: resource, Event: source, Message: source.Message, Dir: h.resources.Dir})
		}
	} else if record.Resource.InternalURL == "" {
		materializer, ok := provider.(uvim.ConversationResourceMaterializer)
		if !ok || provider.Capabilities().ConversationMaterializeVersion != uvim.ConversationMaterializeVersionV1 {
			err = uvim.ErrConversationResourceUnavailable
		} else {
			downloaded, err = materializer.MaterializeConversationResource(req.Context(), uvim.ConversationResourceMaterializeRequest{
				Trigger: trigger, Scope: record.Scope, Source: record.Source, SourceMessage: record.SourceMessageID, SourceOrdinal: record.Descriptor.SourceOrdinal,
				Resource: resource, Dir: h.resources.Dir,
			})
		}
	}
	if err != nil {
		state, reason := materializeFailureState(req.Context(), err)
		writeJSON(w, http.StatusOK, uvim.ConversationMaterializeResult{Version: uvim.ConversationMaterializeVersionV1, State: state, Reason: reason})
		return
	}
	if record.Resource.InternalURL != "" {
		downloaded = record.Resource
	}
	downloaded, err = verifyConversationMaterialCache(h.resources, downloaded, record.Descriptor)
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("cached_resource_unavailable"))
		return
	}
	if record.Source == "received" {
		if _, err := h.receivedContext.materializeEvent(req.Context(), record, trigger); err != nil {
			writeJSON(w, http.StatusOK, unavailableConversationMaterialize("source_unavailable"))
			return
		}
	}
	_, latest, reason := h.conversationSearchTrigger(req.Context(), searchInput)
	if reason != "" || latest != digest {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("trigger_unavailable"))
		return
	}
	completed, err := h.materialResources.complete(h.authToken, record.Handle, downloaded)
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
		return
	}
	writeJSON(w, http.StatusOK, uvim.ConversationMaterializeResult{Version: uvim.ConversationMaterializeVersionV1, State: "ready", Resource: completed.Resource.Sanitized()})
}

func trimConversationMaterializeRequest(input *uvim.ConversationMaterializeRequest) {
	input.Provider = strings.TrimSpace(input.Provider)
	input.Connector = strings.TrimSpace(input.Connector)
	input.TriggerEventID = strings.TrimSpace(input.TriggerEventID)
	input.TriggerMessageID = strings.TrimSpace(input.TriggerMessageID)
	input.ExpectedEventDigest = strings.TrimSpace(input.ExpectedEventDigest)
	input.AudienceDigest = strings.TrimSpace(input.AudienceDigest)
	input.SourceEpoch = strings.TrimSpace(input.SourceEpoch)
	input.MaterializeHandle = strings.TrimSpace(input.MaterializeHandle)
}

func validConversationMaterializeRequest(input uvim.ConversationMaterializeRequest) bool {
	return input.Provider != "" && input.Connector != "" && input.TriggerEventID != "" && input.TriggerMessageID != "" && input.ExpectedEventDigest != "" && input.AudienceDigest != "" && input.MaterializeHandle != ""
}

func unavailableConversationMaterialize(reason string) uvim.ConversationMaterializeResult {
	return uvim.ConversationMaterializeResult{Version: uvim.ConversationMaterializeVersionV1, State: "unavailable", Reason: reason}
}

func materializeFailureState(ctx context.Context, err error) (string, string) {
	if errors.Is(err, errConversationMaterialCacheUnavailable) {
		return "unavailable", "cached_resource_unavailable"
	}
	if errors.Is(err, uvim.ErrConversationResourceUnavailable) {
		return "revoked", "source_unavailable"
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "unavailable", "provider_materialize_unavailable"
	}
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		switch failure.Category {
		case uvim.SendFailurePermission, uvim.SendFailureAuthentication, uvim.SendFailureInvalidRequest, uvim.SendFailureProviderRejected, uvim.SendFailurePayloadTooLarge:
			return "definite_failed", "provider_materialize_failed"
		}
	}
	return "unavailable", "provider_materialize_unavailable"
}

// verifyConversationMaterialCache deliberately hashes one selected cached
// resource after the caller's source proof and outside every registry/index
// lock. A missing, changed or truncated local value is not an authorization to
// retry a provider download under a different provenance record.
func verifyConversationMaterialCache(store *uvim.ResourceStore, cached uvim.ResourceRef, descriptor uvim.ContextResource) (uvim.ResourceRef, error) {
	if store == nil || cached.InternalURL == "" {
		return uvim.ResourceRef{}, errConversationMaterialCacheUnavailable
	}
	file, _, err := store.Open(cached.InternalURL)
	if err != nil {
		return uvim.ResourceRef{}, errConversationMaterialCacheUnavailable
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return uvim.ResourceRef{}, errConversationMaterialCacheUnavailable
	}
	actualSHA := hex.EncodeToString(hash.Sum(nil))
	if cached.SizeBytes != size || cached.SHA256 == "" || cached.SHA256 != actualSHA || descriptor.SizeBytes > 0 && descriptor.SizeBytes != size || descriptor.SHA256 != "" && descriptor.SHA256 != actualSHA {
		return uvim.ResourceRef{}, errConversationMaterialCacheUnavailable
	}
	return cached.Sanitized(), nil
}
