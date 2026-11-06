package uvim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	InteractionSchemaV1             = "interaction.v1"
	InteractionUpdateVersionV1      = "interaction-update.v1"
	EventInteractionAction          = "interaction.action"
	InteractionCardMaxBytes         = 16 << 10
	InteractionCallbackTextMaxBytes = 8 << 10
	InteractionMaxActions           = 8
	InteractionDelivered            = "delivered"
	InteractionNotAttempted         = "not_attempted"
	InteractionFailedDefinite       = "failed_definite"
	InteractionUnknown              = "unknown"
)

type InteractionActionKind string

const (
	InteractionAnswer   InteractionActionKind = "answer"
	InteractionContinue InteractionActionKind = "continue"
	InteractionCancel   InteractionActionKind = "cancel"
	InteractionDetails  InteractionActionKind = "details"
)

type CardFact struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type CardQuestion struct {
	ID               string `json:"id"`
	Prompt           string `json:"prompt"`
	Help             string `json:"help,omitempty"`
	AllowFreeText    bool   `json:"allow_free_text,omitempty"`
	FreeTextActionID string `json:"free_text_action_id,omitempty"`
}

// InteractionCard is a neutral presentation. Provider JSON and credentials
// cannot be supplied through it; opaque actions are authorized by the caller.
type InteractionCard struct {
	SchemaVersion string              `json:"schema_version"`
	CardID        string              `json:"card_id"`
	Revision      uint64              `json:"revision"`
	Title         string              `json:"title"`
	Summary       string              `json:"summary,omitempty"`
	Facts         []CardFact          `json:"facts,omitempty"`
	Question      *CardQuestion       `json:"question,omitempty"`
	Actions       []InteractionAction `json:"actions,omitempty"`
	DetailURL     string              `json:"detail_url,omitempty"`
}

type InteractionAction struct {
	ID       string                `json:"id"`
	Label    string                `json:"label"`
	Kind     InteractionActionKind `json:"kind"`
	Token    string                `json:"token"`
	Disabled bool                  `json:"disabled,omitempty"`
}

// InteractionActionEvent enters the existing authenticated EventLog. It is
// never converted to message text, and contains no vendor update credential.
type InteractionActionEvent struct {
	SchemaVersion  string                `json:"schema_version"`
	EventID        string                `json:"event_id"`
	Provider       string                `json:"provider"`
	Connector      string                `json:"connector"`
	ConversationID string                `json:"conversation_id"`
	MessageID      string                `json:"message_id"`
	CardID         string                `json:"card_id"`
	Revision       uint64                `json:"revision"`
	ActionID       string                `json:"action_id"`
	Kind           InteractionActionKind `json:"kind"`
	Token          string                `json:"token"`
	Actor          User                  `json:"actor"`
	SubmittedText  string                `json:"submitted_text,omitempty"`
	OccurredAt     time.Time             `json:"occurred_at"`
	BotAppID       string                `json:"bot_app_id"`
	BotProofDigest string                `json:"bot_proof_digest"`
}

func interactionLabel(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func validInteractionKind(kind InteractionActionKind) bool {
	return kind == InteractionAnswer || kind == InteractionContinue || kind == InteractionCancel || kind == InteractionDetails
}

func (card InteractionCard) Validate() error {
	if card.SchemaVersion != InteractionSchemaV1 || !interactionLabel(card.CardID, 256) || card.Revision == 0 || !interactionLabel(card.Title, 512) || !utf8.ValidString(card.Summary) || len(card.Facts) > 16 || len(card.Actions) > InteractionMaxActions {
		return errors.New("invalid interaction card")
	}
	for _, fact := range card.Facts {
		if !interactionLabel(fact.Label, 256) || !utf8.ValidString(fact.Value) {
			return errors.New("invalid interaction fact")
		}
	}
	if question := card.Question; question != nil {
		if !interactionLabel(question.ID, 256) || !utf8.ValidString(question.Prompt) || question.Prompt == "" || !utf8.ValidString(question.Help) {
			return errors.New("invalid interaction question")
		}
	}
	seen := map[string]bool{}
	for _, action := range card.Actions {
		if !interactionLabel(action.ID, 256) || seen[action.ID] || !interactionLabel(action.Label, 512) || !validInteractionKind(action.Kind) || !interactionLabel(action.Token, 512) {
			return errors.New("invalid interaction action")
		}
		seen[action.ID] = true
	}
	if card.Question != nil && card.Question.AllowFreeText {
		found := false
		for _, action := range card.Actions {
			if action.ID == card.Question.FreeTextActionID && action.Kind == InteractionAnswer && !action.Disabled {
				found = true
			}
		}
		if !found {
			return errors.New("free-text question requires an answer action")
		}
	}
	if card.DetailURL != "" {
		u, err := url.ParseRequestURI(card.DetailURL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" {
			return errors.New("invalid interaction detail URL")
		}
	}
	raw, err := json.Marshal(card)
	if err != nil || len(raw) > InteractionCardMaxBytes {
		return errors.New("interaction card budget exceeded")
	}
	return nil
}

func (card InteractionCard) Digest() (string, error) {
	if err := card.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateInteractionEvent(event Event) error {
	if event.Type != EventInteractionAction {
		if event.Interaction != nil {
			return errors.New("interaction payload requires interaction event type")
		}
		return nil
	}
	action := event.Interaction
	if action == nil || action.SchemaVersion != InteractionSchemaV1 || action.EventID != event.ID || action.Provider != event.Provider || action.Connector != event.Connector || action.ConversationID != event.Channel.ID || action.Actor.ID != event.User.ID || action.Actor.Kind != UserKindUser || event.User.Kind != UserKindUser || action.MessageID != event.Referrer.MessageID || event.Message.Text != "" || len(event.Message.Elements) != 0 || len(event.Message.Resources) != 0 || action.Revision == 0 || action.OccurredAt.IsZero() {
		return errors.New("invalid interaction event scope")
	}
	for _, value := range []string{action.EventID, action.Provider, action.Connector, action.ConversationID, action.MessageID, action.CardID, action.ActionID, action.Token, action.Actor.ID, action.BotAppID, action.BotProofDigest} {
		if !interactionLabel(value, 512) {
			return errors.New("invalid interaction event identity")
		}
	}
	if !validInteractionKind(action.Kind) || !utf8.ValidString(action.SubmittedText) || len(action.SubmittedText) > InteractionCallbackTextMaxBytes {
		return errors.New("invalid interaction submission")
	}
	return nil
}

func ValidateOutboundInteraction(message OutboundMessage, capabilities Capabilities) error {
	if message.Interaction == nil {
		return nil
	}
	if !capabilities.InteractiveMessage || len(message.Interaction.Actions) > 0 && !capabilities.InteractiveActionCallback {
		return errors.New("interaction transport is unsupported")
	}
	if message.Text != "" || len(message.Elements) != 0 || len(message.Resources) != 0 {
		return errors.New("interaction card cannot mix message bodies")
	}
	if err := message.Interaction.Validate(); err != nil {
		return err
	}
	if capabilities.CardMaxBytes > 0 {
		raw, _ := json.Marshal(message.Interaction)
		if len(raw) > capabilities.CardMaxBytes {
			return errors.New("provider interaction budget exceeded")
		}
	}
	return nil
}

func (update InteractionMessageUpdate) Validate() error {
	digest, err := update.Card.Digest()
	if err != nil || update.Version != InteractionUpdateVersionV1 || update.CardID != update.Card.CardID || update.ExpectedRevision == 0 || update.ReplacementRevision != update.ExpectedRevision+1 || update.Card.Revision != update.ReplacementRevision || update.PayloadSHA256 != digest {
		return errors.New("invalid interaction update")
	}
	for _, value := range []string{update.Provider, update.Connector, update.NativeMessageID, update.OperationID, update.Target.ID, update.Target.Kind} {
		if !interactionLabel(value, 512) {
			return errors.New("invalid interaction update identity")
		}
	}
	if update.Referrer.ReplyToken != "" {
		return errors.New("interaction update cannot carry reply credentials")
	}
	return nil
}

type InteractionMessageUpdate struct {
	Version             string          `json:"version"`
	Provider            string          `json:"provider"`
	Connector           string          `json:"connector"`
	NativeMessageID     string          `json:"native_message_id"`
	Target              OutboundTarget  `json:"target"`
	Referrer            Referrer        `json:"referrer"`
	CardID              string          `json:"card_id"`
	ExpectedRevision    uint64          `json:"expected_revision"`
	ReplacementRevision uint64          `json:"replacement_revision"`
	Card                InteractionCard `json:"card"`
	PayloadSHA256       string          `json:"payload_sha256"`
	OperationID         string          `json:"operation_id"`
}

type InteractionUpdateResult struct {
	Version             string `json:"version"`
	OperationID         string `json:"operation_id"`
	NativeMessageID     string `json:"native_message_id"`
	CardID              string `json:"card_id"`
	ReplacementRevision uint64 `json:"replacement_revision"`
	PayloadSHA256       string `json:"payload_sha256"`
	DeliveryState       string `json:"delivery_state"`
	SafeReason          string `json:"safe_reason,omitempty"`
}

type InteractionMessageUpdater interface {
	UpdateInteraction(context.Context, InteractionMessageUpdate) (InteractionUpdateResult, error)
}

type InteractionPublication struct {
	CardID          string `json:"card_id"`
	Revision        uint64 `json:"revision"`
	PayloadSHA256   string `json:"payload_sha256"`
	NativeMessageID string `json:"native_message_id"`
}
