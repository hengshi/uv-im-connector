package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const interactionRetention = 7 * 24 * time.Hour

// The transport owns only presentation revisions and provider-call receipts.
// Each operation has its own bounded record; no card body, action token,
// vendor reply credential, Task or queue is stored here.
type interactionTransportStore struct {
	root       string
	active     sync.Map
	cleanupMu  sync.Mutex
	cleanupDir *os.File
}

type interactionTransportRecord struct {
	Version       string                       `json:"version"`
	Provider      string                       `json:"provider"`
	Connector     string                       `json:"connector"`
	MessageID     string                       `json:"native_message_id"`
	CardID        string                       `json:"card_id"`
	Revision      uint64                       `json:"revision"`
	PayloadSHA256 string                       `json:"payload_sha256"`
	Target        uvim.OutboundTarget          `json:"target"`
	Referrer      uvim.Referrer                `json:"referrer"`
	CreatedAt     time.Time                    `json:"created_at"`
	ExpiresAt     time.Time                    `json:"expires_at"`
	UpdatedAt     time.Time                    `json:"updated_at"`
	Pending       *interactionOperationReceipt `json:"pending,omitempty"`
	Unknown       bool                         `json:"unknown,omitempty"`
}

type interactionOperationReceipt struct {
	RequestDigest string                       `json:"request_digest"`
	Result        uvim.InteractionUpdateResult `json:"result"`
}

func newInteractionTransportStore(log *uvim.EventLog) *interactionTransportStore {
	store := &interactionTransportStore{}
	if log != nil && log.Path != "" {
		store.root = filepath.Join(filepath.Dir(log.Path), "state", "interaction-updates")
	}
	return store
}

// Expiry cleanup advances one bounded directory batch during ordinary use.
// It has no timer/daemon and cannot alter an active provider attempt.
func (s *interactionTransportStore) pruneExpired() {
	if s == nil || s.root == "" {
		return
	}
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.cleanupDir == nil {
		file, err := os.Open(s.root)
		if err != nil {
			return
		}
		s.cleanupDir = file
	}
	entries, err := s.cleanupDir.ReadDir(64)
	if err != nil {
		s.cleanupDir.Close()
		s.cleanupDir = nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	for _, entry := range entries {
		if cleanupCtx.Err() != nil {
			break
		}
		if !entry.IsDir() || len(entry.Name()) != 64 {
			continue
		}
		if _, err := hex.DecodeString(entry.Name()); err != nil {
			continue
		}
		directory := filepath.Join(s.root, entry.Name())
		_ = withInteractionLock(cleanupCtx, directory, func() error {
			var record interactionTransportRecord
			if readInteractionJSON(filepath.Join(directory, "presentation.json"), &record) != nil || record.Version != uvim.InteractionUpdateVersionV1 || record.CreatedAt.IsZero() || record.ExpiresAt.IsZero() || record.ExpiresAt.After(record.CreatedAt.Add(interactionRetention)) || time.Now().Before(record.ExpiresAt) {
				return nil
			}
			if record.Pending != nil {
				if _, active := s.active.Load(directory + "\x00" + record.Pending.Result.OperationID); active {
					return nil
				}
			}
			return os.RemoveAll(directory)
		})
	}
}

func interactionHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (s *interactionTransportStore) directory(provider, connector, message string) string {
	return filepath.Join(s.root, interactionHash(strings.Join([]string{provider, connector, message}, "\x00")))
}

func interactionReferrer(ref uvim.Referrer) uvim.Referrer {
	ref.ReplyToken = ""
	ref.ExpiresAt = nil
	if ref.Target != nil {
		copy := *ref.Target
		ref.Target = &copy
	}
	return ref
}

func writeInteractionJSON(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".interaction-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func readInteractionJSON(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return errors.New("interaction record is invalid")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("interaction record has trailing data")
	}
	return nil
}

func withInteractionLock(ctx context.Context, directory string, fn func() error) error {
	file, err := os.OpenFile(filepath.Join(directory, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *interactionTransportStore) recordSent(ctx context.Context, message uvim.OutboundMessage, result uvim.SendResult, capabilities uvim.Capabilities) error {
	if s == nil || s.root == "" || message.Interaction == nil || result.Interaction == nil || !result.Interaction.ValidFor(*message.Interaction) || result.Interaction.NativeMessageID != result.MessageID {
		return errors.New("interaction receipt unavailable")
	}
	if result.Interaction.NativeCardID != "" {
		if capabilities.InteractionCardInstanceVersion != uvim.InteractionCardInstanceVersionV1 {
			return errors.New("interaction card-instance confirmation unsupported")
		}
		// This receipt confirms a callback-addressable card, not a native
		// message that the persistent PATCH owner could update. The caller
		// persists its business delivery receipt; no second card store is needed.
		return nil
	}
	s.pruneExpired()
	digest, err := message.Interaction.Digest()
	if err != nil {
		return err
	}
	publication := result.Interaction
	if publication.CardID != message.Interaction.CardID || publication.Revision != message.Interaction.Revision || publication.PayloadSHA256 != digest || publication.NativeMessageID != result.MessageID {
		return errors.New("interaction publication mismatch")
	}
	directory := s.directory(result.Provider, result.Connector, result.MessageID)
	if err = os.MkdirAll(filepath.Join(directory, "operations"), 0o700); err != nil {
		return err
	}
	return withInteractionLock(ctx, directory, func() error {
		path := filepath.Join(directory, "presentation.json")
		var existing interactionTransportRecord
		if readErr := readInteractionJSON(path, &existing); readErr == nil {
			if existing.CardID == publication.CardID && existing.Revision == publication.Revision && existing.PayloadSHA256 == digest {
				return nil
			}
			return errors.New("interaction native identity already registered")
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		now := time.Now().UTC()
		return writeInteractionJSON(path, interactionTransportRecord{Version: uvim.InteractionUpdateVersionV1, Provider: result.Provider, Connector: result.Connector, MessageID: result.MessageID,
			CardID: publication.CardID, Revision: publication.Revision, PayloadSHA256: digest, Target: message.ResolvedTarget(), Referrer: interactionReferrer(message.Referrer), CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(interactionRetention)})
	})
}

func interactionUpdateResult(request uvim.InteractionMessageUpdate, state, reason string) uvim.InteractionUpdateResult {
	return uvim.InteractionUpdateResult{Version: uvim.InteractionUpdateVersionV1, OperationID: request.OperationID, NativeMessageID: request.NativeMessageID, CardID: request.CardID, ReplacementRevision: request.ReplacementRevision, PayloadSHA256: request.PayloadSHA256, DeliveryState: state, SafeReason: reason}
}

func validInteractionDelivery(state string) bool {
	return state == uvim.InteractionDelivered || state == uvim.InteractionNotAttempted || state == uvim.InteractionFailedDefinite || state == uvim.InteractionUnknown
}

func interactionReceiptMatches(pending, settled interactionOperationReceipt) bool {
	left, right := pending.Result, settled.Result
	left.DeliveryState, left.SafeReason = "", ""
	right.DeliveryState, right.SafeReason = "", ""
	return pending.RequestDigest == settled.RequestDigest && left == right && validInteractionDelivery(settled.Result.DeliveryState)
}

func applyInteractionReceipt(record *interactionTransportRecord, result uvim.InteractionUpdateResult) {
	record.Pending = nil
	if result.DeliveryState == uvim.InteractionDelivered {
		record.Revision, record.PayloadSHA256, record.UpdatedAt = result.ReplacementRevision, result.PayloadSHA256, time.Now().UTC()
	}
	if result.DeliveryState == uvim.InteractionUnknown {
		record.Unknown = true
	}
}

func (s *interactionTransportStore) update(ctx context.Context, request uvim.InteractionMessageUpdate, provider uvim.Provider) (uvim.InteractionUpdateResult, error) {
	refusal := func(reason string) uvim.InteractionUpdateResult {
		return interactionUpdateResult(request, uvim.InteractionNotAttempted, reason)
	}
	if s == nil || s.root == "" {
		return refusal("transport_record_unavailable"), nil
	}
	s.pruneExpired()
	updater, ok := provider.(uvim.InteractionMessageUpdater)
	if !ok || !provider.Capabilities().InteractiveMessageUpdate || provider.Capabilities().InteractionUpdateVersion != uvim.InteractionUpdateVersionV1 {
		return refusal("unsupported"), nil
	}
	directory := s.directory(request.Provider, request.Connector, request.NativeMessageID)
	if info, err := os.Lstat(directory); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return refusal("transport_record_unavailable"), nil
	}
	raw, _ := json.Marshal(request)
	digest := interactionHash(string(raw))
	opPath := filepath.Join(directory, "operations", interactionHash(request.OperationID)+".json")
	key := directory + "\x00" + request.OperationID
	var result uvim.InteractionUpdateResult
	claimed := false
	err := withInteractionLock(ctx, directory, func() error {
		var record interactionTransportRecord
		if err := readInteractionJSON(filepath.Join(directory, "presentation.json"), &record); err != nil {
			result = refusal("transport_record_unavailable")
			return nil
		}
		if record.Version != uvim.InteractionUpdateVersionV1 || record.Provider != request.Provider || record.Connector != request.Connector || record.MessageID != request.NativeMessageID || record.CardID != request.CardID || record.Target != request.Target || !reflect.DeepEqual(record.Referrer, interactionReferrer(request.Referrer)) || record.CreatedAt.IsZero() || record.ExpiresAt.After(record.CreatedAt.Add(interactionRetention)) || !time.Now().Before(record.ExpiresAt) {
			result = refusal("transport_scope_unavailable")
			return nil
		}
		// The operation receipt may have committed just before a crash that
		// prevented its presentation pointer update. Repair only this same seal.
		if pending := record.Pending; pending != nil {
			var settled interactionOperationReceipt
			pendingPath := filepath.Join(directory, "operations", interactionHash(pending.Result.OperationID)+".json")
			if err := readInteractionJSON(pendingPath, &settled); err == nil {
				if !interactionReceiptMatches(*pending, settled) {
					result = refusal("transport_record_unavailable")
					return nil
				}
				applyInteractionReceipt(&record, settled.Result)
				if err := writeInteractionJSON(filepath.Join(directory, "presentation.json"), record); err != nil {
					return err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				result = refusal("transport_record_unavailable")
				return nil
			}
		}
		var prior interactionOperationReceipt
		if err := readInteractionJSON(opPath, &prior); err == nil {
			if prior.RequestDigest != digest || !validInteractionDelivery(prior.Result.DeliveryState) || prior.Result.OperationID != request.OperationID || prior.Result.NativeMessageID != request.NativeMessageID || prior.Result.CardID != request.CardID || prior.Result.ReplacementRevision != request.ReplacementRevision || prior.Result.PayloadSHA256 != request.PayloadSHA256 || prior.Result.Version != uvim.InteractionUpdateVersionV1 {
				result = refusal("operation_conflict")
			} else {
				result = prior.Result
			}
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			result = refusal("transport_record_unavailable")
			return nil
		}
		if record.Pending != nil {
			pending := record.Pending
			if _, active := s.active.Load(directory + "\x00" + pending.Result.OperationID); active {
				result = refusal("update_in_progress")
				return nil
			}
			pending.Result.DeliveryState, pending.Result.SafeReason = uvim.InteractionUnknown, "attempt_interrupted"
			pendingPath := filepath.Join(directory, "operations", interactionHash(pending.Result.OperationID)+".json")
			if err := writeInteractionJSON(pendingPath, pending); err != nil {
				return err
			}
			record.Pending, record.Unknown = nil, true
			if err := writeInteractionJSON(filepath.Join(directory, "presentation.json"), record); err != nil {
				return err
			}
			if pending.Result.OperationID == request.OperationID && pending.RequestDigest == digest {
				result = pending.Result
			} else {
				result = refusal("prior_update_unknown")
			}
			return nil
		}
		if record.Unknown {
			result = refusal("prior_update_unknown")
			return nil
		}
		if record.Revision != request.ExpectedRevision {
			result = refusal("revision_changed")
			return nil
		}
		interval := time.Duration(provider.Capabilities().UpdateMinIntervalMS) * time.Millisecond
		if time.Since(record.UpdatedAt) < interval {
			result = refusal("rate_limited")
			return nil
		}
		result = interactionUpdateResult(request, uvim.InteractionUnknown, "")
		record.Pending = &interactionOperationReceipt{RequestDigest: digest, Result: result}
		s.active.Store(key, true)
		if err := writeInteractionJSON(filepath.Join(directory, "presentation.json"), record); err != nil {
			s.active.Delete(key)
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return refusal("transport_record_unavailable"), err
	}
	if !claimed {
		return result, nil
	}
	defer s.active.Delete(key)
	// No file/process lock is held during native transport I/O.
	observed, callErr := updater.UpdateInteraction(ctx, request)
	if observed.Version != result.Version || observed.OperationID != result.OperationID || observed.NativeMessageID != result.NativeMessageID || observed.CardID != result.CardID || observed.ReplacementRevision != result.ReplacementRevision || observed.PayloadSHA256 != result.PayloadSHA256 {
		observed = result
		observed.DeliveryState, observed.SafeReason = uvim.InteractionUnknown, "receipt_unverifiable"
	}
	switch observed.DeliveryState {
	case uvim.InteractionDelivered:
		if callErr != nil {
			observed.DeliveryState, observed.SafeReason = uvim.InteractionUnknown, "provider_update_unknown"
		}
	case uvim.InteractionNotAttempted, uvim.InteractionFailedDefinite, uvim.InteractionUnknown:
	default:
		observed.DeliveryState = uvim.InteractionUnknown
	}
	// Provider error strings never enter transport records or the HTTP result.
	if observed.DeliveryState != uvim.InteractionDelivered {
		observed.SafeReason = "provider_update_" + observed.DeliveryState
	}
	err = withInteractionLock(context.WithoutCancel(ctx), directory, func() error {
		var record interactionTransportRecord
		if err := readInteractionJSON(filepath.Join(directory, "presentation.json"), &record); err != nil {
			return err
		}
		if record.Pending == nil || record.Pending.RequestDigest != digest || record.Pending.Result.OperationID != request.OperationID {
			return errors.New("interaction settlement owner changed")
		}
		if err := writeInteractionJSON(opPath, interactionOperationReceipt{RequestDigest: digest, Result: observed}); err != nil {
			return err
		}
		applyInteractionReceipt(&record, observed)
		return writeInteractionJSON(filepath.Join(directory, "presentation.json"), record)
	})
	if err != nil {
		return interactionUpdateResult(request, uvim.InteractionUnknown, "receipt_unavailable"), nil
	}
	return observed, nil
}

func (h *Hub) handleInteractionUpdate(w http.ResponseWriter, r *http.Request) {
	if h.authToken == "" {
		writeError(w, http.StatusUnauthorized, "interaction_authentication_required")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	decoder.DisallowUnknownFields()
	var request uvim.InteractionMessageUpdate
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Validate() != nil {
		writeError(w, http.StatusBadRequest, "invalid_interaction_update")
		return
	}
	provider := h.registry.Get(request.Provider, request.Connector)
	if provider == nil {
		writeJSON(w, http.StatusOK, interactionUpdateResult(request, uvim.InteractionNotAttempted, "unsupported"))
		return
	}
	result, err := h.interactionUpdates.update(r.Context(), request, provider)
	if err != nil {
		result = interactionUpdateResult(request, uvim.InteractionNotAttempted, "transport_record_unavailable")
	}
	writeJSON(w, http.StatusOK, result)
}
