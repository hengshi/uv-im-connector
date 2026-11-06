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
	record, resource, started, err := h.materialResources.reserve(h.authToken, input)
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
		return
	}
	if record.ScopeHash != conversationMaterialScopeHash(record.Scope, trigger) || record.Scope.Provider != trigger.Provider || record.Scope.Connector != trigger.Connector || record.Scope.ConversationID != trigger.Channel.ID || record.Scope.TriggerMessageID != trigger.Message.ID || !record.Scope.AnchorTime.Equal(conversationSearchAnchor(trigger)) {
		if started {
			settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "revoked", "scope_unavailable", uvim.ResourceRef{})
			if settleErr != nil {
				writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
				return
			}
			writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
			return
		}
		writeJSON(w, http.StatusOK, h.invalidateConversationMaterialReady(record, "revoked", "scope_unavailable"))
		return
	}
	if !started {
		writeJSON(w, http.StatusOK, h.revalidateSettledConversationMaterial(req.Context(), record, trigger, searchInput, digest))
		return
	}
	if record.Source == "received" && !h.receivedMaterializeAvailable(provider) {
		settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "definite_failed", "unsupported", uvim.ResourceRef{})
		if settleErr != nil {
			writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
			return
		}
		writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
		return
	}
	if record.Source != "received" && provider.Capabilities().ConversationMaterializeVersion != uvim.ConversationMaterializeVersionV1 {
		settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "definite_failed", "unsupported", uvim.ResourceRef{})
		if settleErr != nil {
			writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
			return
		}
		writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
		return
	}
	var downloaded uvim.ResourceRef
	if record.Source == "received" {
		source, sourceErr := h.receivedContext.materializeEvent(req.Context(), record, trigger)
		if sourceErr != nil {
			err = sourceErr
		} else {
			cached, cachedOK, cacheErr := h.materialResources.readyResource(h.authToken, record)
			if cacheErr != nil {
				err = cacheErr
			} else if cachedOK {
				downloaded, err = verifyConversationMaterialCache(h.resources, cached, record.Descriptor)
				if err == nil {
					// Bytes may be shared only after this second record's current
					// source proof still holds; do not let cache presence replace it.
					if _, sourceErr := h.receivedContext.materializeEvent(req.Context(), record, trigger); sourceErr != nil {
						err = sourceErr
					} else {
						downloaded = rebindConversationMaterialRelation(downloaded, resource)
					}
				}
			} else {
				downloaded, err = provider.Download(req.Context(), uvim.ResourceDownloadRequest{Resource: resource, Event: source, Message: source.Message, Dir: h.resources.Dir})
			}
		}
	} else {
		materializer, ok := provider.(uvim.ConversationResourceMaterializer)
		if !ok {
			settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "definite_failed", "unsupported", uvim.ResourceRef{})
			if settleErr != nil {
				writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
				return
			}
			writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
			return
		}
		downloaded, err = materializer.MaterializeConversationResource(req.Context(), uvim.ConversationResourceMaterializeRequest{
			Trigger: trigger, Scope: record.Scope, Source: record.Source, SourceMessage: record.SourceMessageID, SourceOrdinal: record.Descriptor.SourceOrdinal,
			Resource: resource, Dir: h.resources.Dir,
		})
	}
	if err != nil {
		state, failureReason := materializeFailureState(req.Context(), err)
		settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, state, failureReason, uvim.ResourceRef{})
		if settleErr != nil {
			writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
			return
		}
		writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
		return
	}
	if record.Source == "received" {
		if _, sourceErr := h.receivedContext.materializeEvent(req.Context(), record, trigger); sourceErr != nil {
			settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "revoked", "source_unavailable", uvim.ResourceRef{})
			if settleErr != nil {
				writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
				return
			}
			writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
			return
		}
	}
	_, latest, finalReason := h.conversationSearchTrigger(req.Context(), searchInput)
	if finalReason != "" || latest != digest {
		settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "revoked", "trigger_unavailable", uvim.ResourceRef{})
		if settleErr != nil {
			writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
			return
		}
		writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
		return
	}
	settled, settleErr := h.materialResources.settle(h.authToken, record.Handle, "ready", "", downloaded)
	if settleErr != nil {
		writeJSON(w, http.StatusOK, unavailableConversationMaterialize("handle_not_recoverable"))
		return
	}
	writeJSON(w, http.StatusOK, conversationMaterializeRecordResult(settled))
}

// revalidateSettledConversationMaterial makes a replayed ready handle prove
// the same things as the first successful attempt: current trigger/scope,
// received source authority where applicable, and the sealed bytes in the
// local ResourceStore.  It never invokes a provider, so cache loss or source
// withdrawal cannot become a second download attempt.
func (h *Hub) revalidateSettledConversationMaterial(ctx context.Context, record conversationMaterialRecord, trigger uvim.Event, searchInput uvim.ConversationSearchRequest, digest string) uvim.ConversationMaterializeResult {
	if record.State != "ready" {
		return conversationMaterializeRecordResult(record)
	}
	if record.Source == "received" {
		if _, err := h.receivedContext.materializeEvent(ctx, record, trigger); err != nil {
			return h.invalidateConversationMaterialReady(record, "revoked", "source_unavailable")
		}
	}
	if _, err := verifyConversationMaterialCache(h.resources, record.Resource, record.Descriptor); err != nil {
		return h.invalidateConversationMaterialReady(record, "unavailable", "cached_resource_unavailable")
	}
	if record.Source == "received" {
		// Source authority is checked again after hashing outside index locks so a
		// deletion cannot race a ready replay into a stale success.
		if _, err := h.receivedContext.materializeEvent(ctx, record, trigger); err != nil {
			return h.invalidateConversationMaterialReady(record, "revoked", "source_unavailable")
		}
	}
	_, latest, reason := h.conversationSearchTrigger(ctx, searchInput)
	if reason != "" || latest != digest {
		return h.invalidateConversationMaterialReady(record, "revoked", "trigger_unavailable")
	}
	return conversationMaterializeRecordResult(record)
}

func (h *Hub) invalidateConversationMaterialReady(record conversationMaterialRecord, state, reason string) uvim.ConversationMaterializeResult {
	settled, err := h.materialResources.invalidateReady(h.authToken, record.Handle, state, reason)
	if err != nil {
		return unavailableConversationMaterialize("handle_not_recoverable")
	}
	return conversationMaterializeRecordResult(settled)
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
	return input.Provider != "" && input.Connector != "" && input.TriggerEventID != "" && input.TriggerMessageID != "" && input.ExpectedEventDigest != "" && input.AudienceDigest != "" && input.SourceEpoch != "" && input.MaterializeHandle != ""
}

func unavailableConversationMaterialize(reason string) uvim.ConversationMaterializeResult {
	return uvim.ConversationMaterializeResult{Version: uvim.ConversationMaterializeVersionV1, State: "unavailable", Reason: reason}
}

func conversationMaterializeRecordResult(record conversationMaterialRecord) uvim.ConversationMaterializeResult {
	result := uvim.ConversationMaterializeResult{Version: uvim.ConversationMaterializeVersionV1, State: record.State, Reason: record.Reason}
	if record.State == "ready" {
		result.Resource = record.Resource.Sanitized()
	}
	if result.State == "" || result.State == "active" || result.State == "effect_started" {
		return unavailableConversationMaterialize("handle_unavailable")
	}
	return result
}

func materializeFailureState(ctx context.Context, err error) (string, string) {
	if errors.Is(err, errConversationMaterialCacheUnavailable) {
		return "unavailable", "cached_resource_unavailable"
	}
	if errors.Is(err, uvim.ErrConversationResourceUnavailable) {
		return "revoked", "source_unavailable"
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "unknown", "provider_materialize_unknown"
	}
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		switch failure.Category {
		case uvim.SendFailurePermission, uvim.SendFailureAuthentication, uvim.SendFailureInvalidRequest, uvim.SendFailureProviderRejected, uvim.SendFailurePayloadTooLarge:
			return "definite_failed", "provider_materialize_failed"
		}
	}
	return "unknown", "provider_materialize_unknown"
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

// rebindConversationMaterialRelation preserves byte identity/internal storage
// from a verified physical cache while keeping the current opaque handle's
// source relation descriptive fields. It never imports a provider key, secret
// or private map from either side.
func rebindConversationMaterialRelation(cached, current uvim.ResourceRef) uvim.ResourceRef {
	relation := current.Sanitized()
	cached.Provider = uvim.FirstNonEmpty(relation.Provider, cached.Provider)
	cached.Connector = uvim.FirstNonEmpty(relation.Connector, cached.Connector)
	cached.Kind, cached.Name, cached.MIME = relation.Kind, relation.Name, relation.MIME
	cached.SourceMessageID, cached.SourceBotAppID = relation.SourceMessageID, relation.SourceBotAppID
	cached.ReferencedMessageIDs = append([]string(nil), relation.ReferencedMessageIDs...)
	cached.Metadata = relation.Metadata
	cached.Error = relation.Error
	return cached.Sanitized()
}
