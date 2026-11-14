package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

type Hub struct {
	registry       *uvim.ProviderRegistry
	eventLog       *uvim.EventLog
	resources      *uvim.ResourceStore
	authToken      string
	identityMapper *uvim.UserIdentityMapper
	identityLinks  *identityLinks

	mu                 sync.Mutex
	subscribers        map[chan uvim.Event]struct{}
	upgrader           websocket.Upgrader
	materialResources  *conversationMaterialRegistry
	receivedContext    *receivedContextIndex
	interactionUpdates *interactionTransportStore
}

func NewHub(registry *uvim.ProviderRegistry, eventLog *uvim.EventLog, resources *uvim.ResourceStore) *Hub {
	if registry == nil {
		registry = uvim.NewProviderRegistry()
	}
	if eventLog == nil {
		eventLog, _ = uvim.NewEventLog("")
	}
	if resources == nil {
		resources = &uvim.ResourceStore{}
	}
	hub := &Hub{
		registry:           registry,
		eventLog:           eventLog,
		resources:          resources,
		subscribers:        map[chan uvim.Event]struct{}{},
		materialResources:  newConversationMaterialRegistry(eventLog),
		receivedContext:    newReceivedContextIndex(eventLog),
		interactionUpdates: newInteractionTransportStore(eventLog),
		upgrader:           websocket.Upgrader{},
	}
	hub.upgrader.CheckOrigin = hub.checkOrigin
	return hub
}

func (h *Hub) SetAuthToken(token string) {
	if h != nil {
		h.authToken = strings.TrimSpace(token)
	}
}

// SetUserIdentityMapper configures optional account associations before serving.
func (h *Hub) SetUserIdentityMapper(mapper *uvim.UserIdentityMapper) {
	h.identityMapper = mapper
}

func (h *Hub) Emit(ctx context.Context, event uvim.Event) error {
	if event.Type == uvim.EventInteractionAction && h.authToken == "" {
		return errors.New("interaction transport requires authentication")
	}
	if err := uvim.ValidateInteractionEvent(event); err != nil {
		return err
	}
	if event.Interaction != nil && event.Interaction.SchemaVersion == uvim.InteractionCardInstanceVersionV1 {
		provider := h.registry.Get(event.Provider, event.Connector)
		if provider == nil || !uvim.InteractionActorSupported(event.User, provider.Capabilities()) {
			return errors.New("interaction card-instance profile unsupported")
		}
	}
	event = h.identityMapper.Apply(event)
	if event.ID == "" {
		event.ID = uvim.NewID("evt")
	}
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	pendingReceived := ""
	if h.receivedContext != nil {
		var prepareErr error
		pendingReceived, prepareErr = h.receivedContext.prepare(h.authToken, event)
		if prepareErr != nil {
			slog.Warn("received context resource was not journaled", "provider", event.Provider, "connector", event.Connector)
		}
	}
	event = h.resolveEventResources(ctx, event)
	written, fresh, err := h.eventLog.Append(ctx, event)
	if err != nil {
		if h.receivedContext != nil {
			h.receivedContext.discard(pendingReceived)
		}
		return err
	}
	if !fresh {
		if h.receivedContext != nil {
			h.receivedContext.discard(pendingReceived)
		}
		return nil
	}
	if h.receivedContext != nil {
		if err := h.receivedContext.commit(ctx, pendingReceived, h.authToken, written); err != nil {
			slog.Warn("received context resource was not committed", "provider", written.Provider, "connector", written.Connector)
		}
		h.receivedContext.observe(ctx, written)
	}
	h.mu.Lock()
	for ch := range h.subscribers {
		select {
		case ch <- written.Sanitized():
		default:
			close(ch)
			delete(h.subscribers, ch)
		}
	}
	h.mu.Unlock()
	return nil
}

func (h *Hub) RunProviders(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, provider := range h.registry.List() {
		provider := provider
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.runProvider(ctx, provider)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// Each registered identity has one runner. A returned Run (including a normal
// remote close) is retried until the parent is cancelled, without restarting
// the HTTP server, replaying sends, or interrupting sibling providers.
func (h *Hub) runProvider(ctx context.Context, provider uvim.Provider) {
	const initialBackoff, maxBackoff = time.Second, time.Minute
	backoff := initialBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := provider.Run(ctx, h)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) >= maxBackoff {
			backoff = initialBackoff
		}
		retryIn := min(time.Duration(float64(backoff)*(0.8+rand.Float64()*0.4)), maxBackoff)
		// Provider errors can contain credential-bearing URLs or response bodies.
		// Leave provider-specific diagnostics to the adapter's safe logging.
		slog.Warn("provider runner stopped; retrying", "provider", provider.ID(), "connector", provider.ConnectorID(), "failed", err != nil, "retry_in", retryIn)
		timer := time.NewTimer(retryIn)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/v1/meta", h.handleMeta)
	mux.HandleFunc("/v1/events", h.handleEvents)
	mux.HandleFunc("/v1/events/ws", h.handleEventsWS)
	mux.HandleFunc("/v1/message.create", h.handleMessageCreate)
	mux.HandleFunc("/v1/message.update", h.handleMessageUpdate)
	mux.HandleFunc("/v1/interaction.update", h.handleInteractionUpdate)
	mux.HandleFunc("/v1/conversation.window", h.handleConversationWindow)
	mux.HandleFunc("/v1/conversation.search", h.handleConversationSearch)
	mux.HandleFunc("/v1/conversation.materialize", h.handleConversationMaterialize)
	mux.HandleFunc("/v1/identity.link", h.handleIdentity)
	mux.HandleFunc("/v1/identity.resolve", h.handleIdentity)
	mux.HandleFunc("/v1/identity.resolve-inbound", h.handleInboundIdentity)
	mux.HandleFunc("/v1/identity.unlink", h.handleIdentity)
	mux.HandleFunc("/v1/upload.create", h.handleUploadCreate)
	mux.HandleFunc("/v1/resource.download", h.handleResourceDownload)
	mux.HandleFunc("/v1/webhook/", h.handleWebhook)
	mux.HandleFunc("/v1/internal/", h.handleInternal)
	return h.authMiddleware(mux)
}

func (h *Hub) handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Hub) handleMeta(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var providers []uvim.ProviderMeta
	for _, provider := range h.registry.List() {
		capabilities := provider.Capabilities()
		if h.authToken == "" {
			clearInteractionCapabilities(&capabilities)
		}
		health := provider.Health(req.Context())
		if h.authToken == "" {
			clearInteractionCapabilities(&health.Capabilities)
		}
		if !h.nativeConversationWindowAvailable(provider) && h.receivedConversationWindowAvailable(provider) {
			capabilities.ConversationWindowVersion = uvim.ConversationWindowVersionV1
			health.Capabilities.ConversationWindowVersion = uvim.ConversationWindowVersionV1
		}
		if h.receivedMaterializeAvailable(provider) {
			capabilities.ConversationMaterializeVersion = uvim.ConversationMaterializeVersionV1
			health.Capabilities.ConversationMaterializeVersion = uvim.ConversationMaterializeVersionV1
		}
		if strings.TrimSpace(h.authToken) == "" {
			capabilities.ConversationWindowVersion = ""
			capabilities.ConversationSearchVersion = ""
			capabilities.ConversationMaterializeVersion = ""
			health.Capabilities.ConversationWindowVersion = ""
			health.Capabilities.ConversationSearchVersion = ""
			health.Capabilities.ConversationMaterializeVersion = ""
		}
		providers = append(providers, uvim.ProviderMeta{
			Provider:     provider.ID(),
			Connector:    provider.ConnectorID(),
			Capabilities: capabilities,
			Health:       health,
		})
	}
	meta := uvim.NewServiceMeta(providers)
	meta.IdentityLinks = h.identityLinks != nil && h.authToken != ""
	if h.authToken != "" && h.receivedContext != nil && h.receivedContext.path != "" {
		meta.ConversationContextQueryVersion = uvim.ConversationContextQueryVersionV1
	}
	writeJSON(w, http.StatusOK, meta)
}

func clearInteractionCapabilities(capabilities *uvim.Capabilities) {
	capabilities.InteractiveMessage, capabilities.InteractiveActionCallback, capabilities.InteractiveMessageUpdate = false, false, false
	capabilities.InteractionSchemaVersions, capabilities.InteractionUpdateVersion = nil, ""
	capabilities.InteractionCardInstanceVersion, capabilities.CardMaxActions, capabilities.InteractionFreeText = "", 0, false
	capabilities.InteractionActorIdentityTypes = nil
}

// nativeConversationWindowAvailable requires both the explicit adapter
// capability and its reader implementation. A broken native invocation never
// falls through to received context; the latter is selected only when this is
// absent before the request begins.
func (h *Hub) nativeConversationWindowAvailable(provider uvim.Provider) bool {
	if provider == nil || provider.Capabilities().ConversationWindowVersion != uvim.ConversationWindowVersionV1 {
		return false
	}
	_, ok := provider.(uvim.ConversationWindowReader)
	return ok
}

// receivedConversationWindowAvailable deliberately needs only the authenticated
// durable EventLog projection. It does not imply an adapter history reader or
// resource download/materialization capability.
func (h *Hub) receivedConversationWindowAvailable(provider uvim.Provider) bool {
	return h != nil && provider != nil && strings.TrimSpace(h.authToken) != "" && h.receivedContext != nil && strings.TrimSpace(h.receivedContext.path) != ""
}

// receivedMaterializeAvailable is deliberately based on the provider's
// explicit DownloadResource capability and durable Hub prerequisites. Native
// conversation-search support is neither necessary nor sufficient.
func (h *Hub) receivedMaterializeAvailable(provider uvim.Provider) bool {
	if h == nil || provider == nil || strings.TrimSpace(h.authToken) == "" || h.receivedContext == nil || h.materialResources == nil || strings.TrimSpace(h.receivedContext.path) == "" || strings.TrimSpace(h.materialResources.path) == "" || h.resources == nil || strings.TrimSpace(h.resources.Dir) == "" {
		return false
	}
	return provider.Capabilities().DownloadResource
}

func (h *Hub) handleEvents(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	after, _ := strconv.ParseInt(req.URL.Query().Get("after"), 10, 64)
	events, err := h.eventLog.ReadAfter(req.Context(), after)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	visible := make([]uvim.Event, 0, len(events))
	for _, event := range events {
		if h.authToken == "" && event.Type == uvim.EventInteractionAction {
			continue
		}
		visible = append(visible, event.Sanitized())
	}
	events = visible
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (h *Hub) handleEventsWS(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	conn, err := h.upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	after, _ := strconv.ParseInt(req.URL.Query().Get("after"), 10, 64)
	ch := make(chan uvim.Event, 32)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		h.mu.Unlock()
	}()
	backlog, err := h.eventLog.ReadAfter(req.Context(), after)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = conn.WriteJSON(map[string]any{"error": err.Error()})
		return
	}
	lastSeq := after
	for _, event := range backlog {
		if h.authToken == "" && event.Type == uvim.EventInteractionAction {
			continue
		}
		if err := conn.WriteJSON(event.Sanitized()); err != nil {
			return
		}
		if event.Sequence > lastSeq {
			lastSeq = event.Sequence
		}
	}
	for {
		select {
		case <-req.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			if h.authToken == "" && event.Type == uvim.EventInteractionAction {
				continue
			}
			if event.Sequence <= lastSeq {
				continue
			}
			if err := conn.WriteJSON(event.Sanitized()); err != nil {
				return
			}
			lastSeq = event.Sequence
		}
	}
}

func (h *Hub) handleMessageCreate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var msg uvim.OutboundMessage
	if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if msg.Interaction != nil && h.authToken == "" {
		writeError(w, http.StatusUnauthorized, "interaction_authentication_required")
		return
	}
	provider := h.registry.Get(msg.Provider, msg.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	if err := uvim.ValidateOutboundTarget(msg, provider.Capabilities()); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_target")
		return
	}
	if err := uvim.ValidateOutboundText(msg, provider.Capabilities()); err != nil {
		writeProviderError(w, err)
		return
	}
	if err := uvim.ValidateOutboundInteraction(msg, provider.Capabilities()); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_interaction_card")
		return
	}
	if err := h.validateIdentityLink(req.Context(), msg); err != nil {
		writeProviderError(w, uvim.NewProviderSendFailure(uvim.SendFailure{Category: uvim.SendFailureInvalidRequest, DeliveryState: uvim.DeliveryNotAttempted}, err.Error(), err))
		return
	}
	result, err := provider.Send(req.Context(), msg)
	if err != nil {
		reason := uvim.ProviderSendErrorLogDetail(err)
		if reason == "" {
			reason = "provider send failed"
		}
		slog.Error("provider send failed", "provider", provider.ID(), "connector", provider.ConnectorID(), "reason", reason)
		writeProviderError(w, err)
		return
	}
	if msg.Interaction != nil {
		if err := h.interactionUpdates.recordSent(context.WithoutCancel(req.Context()), msg, result, provider.Capabilities()); err != nil {
			// The send has already happened. Missing transport recovery evidence
			// cannot turn it into a retryable send or invent update authority.
			result.Interaction = nil
			slog.Warn("interaction publication receipt unavailable", "provider", provider.ID(), "connector", provider.ConnectorID())
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Hub) handleMessageUpdate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var msg uvim.MessageUpdate
	if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_message_update")
		return
	}
	if strings.TrimSpace(msg.Provider) == "" || strings.TrimSpace(msg.Connector) == "" {
		writeError(w, http.StatusBadRequest, "provider_and_connector_required")
		return
	}
	provider := h.registry.Get(msg.Provider, msg.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	updater, ok := provider.(uvim.MessageUpdater)
	if !provider.Capabilities().EditMessage || !ok {
		writeError(w, http.StatusNotImplemented, "message_update_not_supported")
		return
	}
	if err := uvim.ValidateMessageUpdate(msg, provider.Capabilities()); err != nil {
		if _, ok := uvim.ProviderSendFailure(err); ok {
			writeProviderError(w, err)
		} else {
			writeError(w, http.StatusBadRequest, "invalid_message_update")
		}
		return
	}
	if err := h.validateIdentityLink(req.Context(), msg.OutboundMessage); err != nil {
		writeProviderError(w, uvim.NewProviderSendFailure(uvim.SendFailure{Category: uvim.SendFailureInvalidRequest, DeliveryState: uvim.DeliveryNotAttempted}, err.Error(), err))
		return
	}
	result, err := updater.Update(req.Context(), msg)
	if err != nil {
		slog.Error("provider update failed", "provider", provider.ID(), "connector", provider.ConnectorID(), "reason", uvim.ProviderSendErrorLogDetail(err))
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Hub) handleConversationWindow(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if !h.conversationWindowAuthOK(req) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var input uvim.ConversationWindowRequest
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_conversation_window_request")
		return
	}
	input.Provider = strings.TrimSpace(input.Provider)
	input.Connector = strings.TrimSpace(input.Connector)
	input.TriggerEventID = strings.TrimSpace(input.TriggerEventID)
	input.TriggerMessageID = strings.TrimSpace(input.TriggerMessageID)
	if input.Provider == "" || input.Connector == "" || input.TriggerEventID == "" || input.TriggerMessageID == "" {
		writeError(w, http.StatusBadRequest, "invalid_conversation_window_request")
		return
	}
	provider := h.registry.Get(input.Provider, input.Connector)
	if provider == nil {
		writeJSON(w, http.StatusOK, unavailableConversationWindow(input, "unsupported"))
		return
	}
	nativeAvailable := h.nativeConversationWindowAvailable(provider)
	receivedAvailable := !nativeAvailable && h.receivedConversationWindowAvailable(provider)
	if !nativeAvailable && !receivedAvailable {
		writeJSON(w, http.StatusOK, unavailableConversationWindow(input, "unsupported"))
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), uvim.ConversationWindowTimeout)
	defer cancel()
	trigger, err := h.eventLog.ConversationWindowTrigger(ctx, input)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		writeJSON(w, http.StatusOK, unavailableConversationWindow(input, "window_timeout"))
		return
	}
	if err != nil {
		writeJSON(w, http.StatusOK, unavailableConversationWindow(input, "trigger_unavailable"))
		return
	}
	if !nativeAvailable {
		writeJSON(w, http.StatusOK, h.receivedConversationWindow(ctx, input, trigger))
		return
	}
	reader := provider.(uvim.ConversationWindowReader)
	result := reader.ReadConversationWindow(ctx, trigger)
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	if timedOut {
		writeJSON(w, http.StatusOK, unavailableConversationWindow(input, "window_timeout"))
		return
	}
	writeJSON(w, http.StatusOK, normalizeConversationWindow(input, trigger, result))
}

// receivedConversationWindow projects at most one metadata page from the
// shared received index. It intentionally never calls resource materialize or
// provider download. It retains safe media descriptors while clearing every
// presentation handle/epoch and Hub-private ResourceRef from this automatic,
// no-user-flags context path.
func (h *Hub) receivedConversationWindow(ctx context.Context, input uvim.ConversationWindowRequest, trigger uvim.Event) uvim.ConversationWindowResult {
	result := uvim.ConversationWindowResult{
		Version: uvim.ConversationWindowVersionV1, Source: "received", Status: "unavailable",
		ConversationID: trigger.Channel.ID, ThreadID: trigger.Referrer.ThreadID,
		TriggerMessageID: trigger.Message.ID, AnchorTime: conversationSearchAnchor(trigger), AnchorTimeSource: uvim.EventSourceTimeSource(trigger),
	}
	query, err := h.receivedContext.query(ctx, h.authToken, trigger, conversationSearchFilter{}, conversationSearchMaxLimit, time.Time{}, 0)
	if err != nil {
		result.Reason = "index_rebuild_limited"
		return result
	}
	result.EventLogGeneration, result.TriggerEntryDigest, result.IndexLowerBoundSequence = query.Generation, query.TriggerDigest, query.LowerBound
	result.CandidateCount, result.HasMore = query.CandidateCount, query.HasMore
	for _, item := range query.Items {
		for resourceIndex := range item.Resources {
			clearConversationResourcePresentation(&item.Resources[resourceIndex], "")
		}
		result.Items = append(result.Items, item)
	}
	if query.Reason != "" {
		result.Reason = query.Reason
		return result
	}
	// A retained trigger can still have earlier scoped rows evicted by the
	// bounded received index.  Preserve the safely revalidated projection, but
	// never present that partial window as a complete available/none result.
	if query.LowerBound > 1 {
		result.Status, result.Reason = "unavailable", "index_capacity_limited"
		return result
	}
	if len(result.Items) == 0 {
		result.Status = "none"
		return result
	}
	if result.HasMore {
		result.Status, result.Reason = "scan_limited", "window_scan_limited"
		return result
	}
	result.Status = "available"
	return result
}

func (h *Hub) conversationWindowAuthOK(req *http.Request) bool {
	if h == nil || strings.TrimSpace(h.authToken) == "" || req == nil {
		return false
	}
	value := strings.TrimSpace(req.Header.Get("Authorization"))
	if !strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return false
	}
	return strings.TrimSpace(value[7:]) == h.authToken
}

func unavailableConversationWindow(request uvim.ConversationWindowRequest, reason string) uvim.ConversationWindowResult {
	return uvim.ConversationWindowResult{
		Version:          uvim.ConversationWindowVersionV1,
		Source:           "native",
		Status:           "unavailable",
		Reason:           reason,
		TriggerMessageID: request.TriggerMessageID,
	}
}

func normalizeConversationWindow(request uvim.ConversationWindowRequest, trigger uvim.Event, result uvim.ConversationWindowResult) uvim.ConversationWindowResult {
	anchor := conversationSearchAnchor(trigger)
	if anchor.IsZero() || result.Version != uvim.ConversationWindowVersionV1 || result.Source != "native" || result.TriggerMessageID != trigger.Message.ID || result.ConversationID != trigger.Channel.ID || !result.AnchorTime.Equal(anchor) || (trigger.Referrer.ThreadID != "" && result.ThreadID != trigger.Referrer.ThreadID) || result.CandidateCount < 0 || result.BoundaryOmitted < 0 || !validConversationWindowStatus(result.Status, result.Reason) {
		return unavailableConversationWindow(request, "native_unavailable")
	}
	if !validConversationWindowItems(result, trigger) {
		return unavailableConversationWindow(request, "native_unavailable")
	}
	return result
}

func validConversationWindowItems(result uvim.ConversationWindowResult, trigger uvim.Event) bool {
	if result.CandidateCount > 10 || result.BoundaryOmitted > result.CandidateCount || len(result.Items) > 10 || result.CandidateCount < len(result.Items) {
		return false
	}
	switch result.Status {
	case "available":
		if len(result.Items) == 0 {
			return false
		}
	case "none", "permission_denied", "rate_limited", "unavailable":
		if len(result.Items) != 0 {
			return false
		}
	}
	seen := make(map[string]struct{}, len(result.Items))
	parentID := strings.TrimSpace(trigger.Referrer.ParentMessageID)
	anchor := conversationSearchAnchor(trigger)
	for _, item := range result.Items {
		messageID := strings.TrimSpace(item.MessageID)
		if anchor.IsZero() || messageID == "" || item.CreatedAt.IsZero() || !item.CreatedAt.Before(anchor) || messageID == trigger.Message.ID || (parentID != "" && messageID == parentID) {
			return false
		}
		if _, duplicate := seen[messageID]; duplicate {
			return false
		}
		seen[messageID] = struct{}{}
	}
	return true
}

func validConversationWindowStatus(status, reason string) bool {
	switch status {
	case "available", "none":
		return reason == ""
	case "scan_limited":
		return reason == "window_scan_limited"
	case "permission_denied":
		return reason == "native_permission_denied"
	case "rate_limited":
		return reason == "native_rate_limited"
	case "unavailable":
		switch reason {
		case "window_timeout", "native_unavailable":
			return true
		}
	}
	return false
}

func (h *Hub) handleUploadCreate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var input struct {
		Kind          string `json:"kind"`
		Name          string `json:"name"`
		MIME          string `json:"mime"`
		ContentBase64 string `json:"content_base64"`
	}
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	raw, err := base64.StdEncoding.DecodeString(input.ContentBase64)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ref, err := h.resources.Save(req.Context(), bytes.NewReader(raw), uvim.ResourceRef{Kind: input.Kind, Name: input.Name, MIME: input.MIME})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ref.Sanitized())
}

func (h *Hub) handleResourceDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var input uvim.ResourceDownloadRequest
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider := h.registry.Get(input.Resource.Provider, input.Resource.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	input.Dir = h.resources.Dir
	ref, err := provider.Download(req.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadGateway, "provider_download_failed")
		return
	}
	writeJSON(w, http.StatusOK, ref.Sanitized())
}

func (h *Hub) handleWebhook(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/webhook/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusNotFound, "webhook_not_found")
		return
	}
	provider := h.registry.Get(parts[0], parts[1])
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	webhook, ok := provider.(uvim.WebhookProvider)
	if !ok {
		writeError(w, http.StatusNotFound, "webhook_not_supported")
		return
	}
	webhook.ServeWebhook(w, req, h)
}

func (h *Hub) handleInternal(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	id := strings.TrimPrefix(req.URL.Path, "/v1/internal/")
	file, ref, err := h.resources.Open("internal://" + id)
	if err != nil {
		writeError(w, http.StatusNotFound, "resource_not_found")
		return
	}
	defer file.Close()
	if ref.MIME != "" {
		w.Header().Set("Content-Type", ref.MIME)
	}
	http.ServeContent(w, req, ref.Name, time.Now(), file)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"ok": false, "error": message})
}

func writeProviderError(w http.ResponseWriter, err error) {
	body := map[string]any{
		"ok":      false,
		"error":   "provider_send_failed",
		"failure": normalizedProviderFailure(err),
	}
	if detail := uvim.ProviderSendErrorDetail(err); detail != "" {
		body["detail"] = detail
	}
	writeJSON(w, http.StatusBadGateway, body)
}

func normalizedProviderFailure(err error) uvim.SendFailure {
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		return failure
	}
	return uvim.SendFailure{Category: uvim.SendFailureUnknown, DeliveryState: uvim.DeliveryUnknown}
}

func (h *Hub) resolveEventResources(ctx context.Context, event uvim.Event) uvim.Event {
	if len(event.Message.Resources) == 0 {
		return event.Sanitized()
	}
	provider := h.registry.Get(event.Provider, event.Connector)
	resources := append([]uvim.ResourceRef(nil), event.Message.Resources...)
	for i := range resources {
		resources[i].Provider = uvim.FirstNonEmpty(resources[i].Provider, event.Provider)
		resources[i].Connector = uvim.FirstNonEmpty(resources[i].Connector, event.Connector)
	}
	ambiguousIDs := conflictingResourceIDs(resources)
	ambiguousInternalURLs := conflictingResourceInternalURLs(resources)
	resolved := make([]uvim.ResourceRef, len(resources))
	resolvedByKey := make(map[string]uvim.ResourceRef, len(resources))
	for i, ref := range resources {
		originalID := ref.ID
		if ambiguousIDs[originalID] {
			// An opaque correlation ID is a relation key, not provider authority.
			// Do not let incompatible refs overwrite the same ResourceStore entry
			// or make an Element point at either set of bytes.
			ref.ID = uvim.NewID("res")
		}
		key := resourceRelationKey(ref)
		if ref.ID == "" && ambiguousInternalURLs[ref.InternalURL] {
			key = ""
		}
		if prior, ok := resolvedByKey[key]; ok {
			resolved[i] = resolvedResourceRelation(prior, ref)
			continue
		}
		// Only a fully identified, unaddressed group message from a non-bot
		// sender is retained as received ambient context. It is the one ingress
		// class whose provider reference must remain lazy for its durable,
		// provenance-bound materialization path. Keep the legacy eager path for
		// every other shape: addressed commands, direct messages, bot events,
		// and events without enough group/source identity to prove they are
		// received ambient context.
		if !isReceivedAmbientEvent(event) && provider != nil && provider.Capabilities().DownloadResource && ref.InternalURL == "" && ref.Error == "" {
			downloaded, err := provider.Download(ctx, uvim.ResourceDownloadRequest{
				Resource: ref,
				Message:  event.Message,
				Event:    event,
				Dir:      h.resources.Dir,
			})
			if err == nil {
				ref = downloaded
			} else {
				ref.Error = "download_failed"
			}
		}
		resolved[i] = ref.Sanitized()
		if key != "" {
			resolvedByKey[key] = resolved[i]
		}
		if resolvedKey := resourceRelationKey(ref); resolvedKey != "" && resolvedKey != key {
			if prior, ok := resolvedByKey[resolvedKey]; ok {
				resolved[i] = resolvedResourceRelation(prior, ref)
			} else {
				resolvedByKey[resolvedKey] = resolved[i]
			}
		}
	}
	event.Message.Resources = resolved
	if len(event.Message.Elements) == 0 {
		event.Message.Elements = elementsFromTextAndResources(event.Message.Text, resolved)
	} else {
		event.Message.Elements = resolveElementResources(event.Message.Elements, resources, resolved, ambiguousIDs, ambiguousInternalURLs)
	}
	return event.Sanitized()
}

func conflictingResourceIDs(resources []uvim.ResourceRef) map[string]bool {
	first := make(map[string]uvim.ResourceRef, len(resources))
	conflicts := make(map[string]bool)
	for _, resource := range resources {
		id := strings.TrimSpace(resource.ID)
		if id == "" {
			continue
		}
		if previous, ok := first[id]; ok {
			if !sameResourceDescriptor(previous, resource) {
				conflicts[id] = true
			}
			continue
		}
		first[id] = resource
	}
	return conflicts
}

func conflictingResourceInternalURLs(resources []uvim.ResourceRef) map[string]bool {
	first := make(map[string]uvim.ResourceRef, len(resources))
	conflicts := make(map[string]bool)
	for _, resource := range resources {
		internalURL := strings.TrimSpace(resource.InternalURL)
		if internalURL == "" {
			continue
		}
		if previous, ok := first[internalURL]; ok {
			if !sameResourceDescriptor(previous, resource) {
				conflicts[internalURL] = true
			}
			continue
		}
		first[internalURL] = resource
	}
	return conflicts
}

func sameResourceDescriptor(a, b uvim.ResourceRef) bool {
	return a.ID == b.ID &&
		a.Provider == b.Provider && a.Connector == b.Connector &&
		a.Kind == b.Kind && a.Name == b.Name && a.Key == b.Key && a.URL == b.URL &&
		a.InternalURL == b.InternalURL && a.MIME == b.MIME && a.SizeBytes == b.SizeBytes &&
		a.SHA256 == b.SHA256 && a.Error == b.Error &&
		a.SourceMessageID == b.SourceMessageID && a.SourceBotAppID == b.SourceBotAppID &&
		slices.Equal(a.ReferencedMessageIDs, b.ReferencedMessageIDs) &&
		a.Secret == b.Secret && maps.Equal(a.Private, b.Private) && maps.Equal(a.Metadata, b.Metadata)
}

func resourceRelationKey(resource uvim.ResourceRef) string {
	// This is a logical-reference key, deliberately not a content/cache key.
	// Equal bytes or a provider-private key can be reused by materialization,
	// but cannot collapse two resources whose provenance or private descriptor
	// differs into one persisted ResourceRef identity.
	if id := strings.TrimSpace(resource.ID); id != "" {
		return "id:" + id
	}
	if internalURL := strings.TrimSpace(resource.InternalURL); internalURL != "" {
		return "internal_url:" + internalURL
	}
	return ""
}

// resolvedResourceRelation shares stored bytes but never discards the source
// relation that made this occurrence explicit. A ResourceRef is descriptive
// material, so filename or digest equality cannot erase quote/forward lineage.
func resolvedResourceRelation(resolved, relation uvim.ResourceRef) uvim.ResourceRef {
	resolved.SourceMessageID = relation.SourceMessageID
	resolved.SourceBotAppID = relation.SourceBotAppID
	resolved.ReferencedMessageIDs = append([]string(nil), relation.ReferencedMessageIDs...)
	if relation.Name != "" {
		resolved.Name = relation.Name
	}
	if relation.Kind != "" {
		resolved.Kind = relation.Kind
	}
	return resolved.Sanitized()
}

func resolveElementResources(elements []uvim.Element, resources, resolved []uvim.ResourceRef, ambiguousIDs, ambiguousInternalURLs map[string]bool) []uvim.Element {
	byRelation := make(map[string]uvim.ResourceRef, len(resources)*2)
	for i, resource := range resources {
		if i >= len(resolved) {
			break
		}
		if id := strings.TrimSpace(resource.ID); id != "" && !ambiguousIDs[id] {
			byRelation["id:"+id] = resolved[i]
		}
		if internalURL := strings.TrimSpace(resource.InternalURL); internalURL != "" && !ambiguousInternalURLs[internalURL] {
			byRelation["internal_url:"+internalURL] = resolved[i]
		}
	}
	var resolve func([]uvim.Element) []uvim.Element
	resolve = func(input []uvim.Element) []uvim.Element {
		if len(input) == 0 {
			return nil
		}
		output := make([]uvim.Element, len(input))
		for i, element := range input {
			output[i] = element
			output[i].Children = resolve(element.Children)
			key := ""
			if element.Resource != nil {
				key = resourceRelationKey(*element.Resource)
			}
			if key == "" && strings.HasPrefix(strings.TrimSpace(element.URL), "internal://") {
				key = "internal_url:" + strings.TrimSpace(element.URL)
			}
			if resource, ok := byRelation[key]; ok {
				output[i].Resource = &resource
				output[i].URL = resource.InternalURL
				output[i].Name = resource.Name
				output[i].MIME = resource.MIME
				output[i].SizeBytes = resource.SizeBytes
			} else if uvim.IsResourceElement(element.Type) && (element.Resource != nil || strings.HasPrefix(strings.TrimSpace(element.URL), "internal://")) {
				unavailable := uvim.ResourceRef{Kind: element.Type, Name: element.Name, MIME: element.MIME, SizeBytes: element.SizeBytes, Error: "resource_relation_unavailable"}
				if element.Resource != nil {
					unavailable = *element.Resource
					unavailable.InternalURL = ""
					if unavailable.Error == "" {
						unavailable.Error = "resource_relation_unavailable"
					}
				}
				output[i].Resource = &unavailable
				output[i].URL = ""
			}
		}
		return output
	}
	return resolve(elements)
}

func elementsFromTextAndResources(text string, refs []uvim.ResourceRef) []uvim.Element {
	var out []uvim.Element
	if strings.TrimSpace(text) != "" {
		out = append(out, uvim.Text(text))
	}
	for _, ref := range refs {
		out = append(out, uvim.File(ref))
	}
	return out
}

func (h *Hub) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/health" || strings.HasPrefix(req.URL.Path, "/v1/webhook/") || h.authOK(req) {
			next.ServeHTTP(w, req)
			return
		}
		writeError(w, http.StatusUnauthorized, "unauthorized")
	})
}

func (h *Hub) authOK(req *http.Request) bool {
	if h == nil || h.authToken == "" {
		return true
	}
	token := strings.TrimSpace(req.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(req.URL.Query().Get("access_token"))
	}
	return token == h.authToken
}

func (h *Hub) checkOrigin(req *http.Request) bool {
	if h.authOK(req) {
		return true
	}
	origin := strings.TrimSpace(req.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	return strings.Contains(origin, "://"+req.Host)
}
