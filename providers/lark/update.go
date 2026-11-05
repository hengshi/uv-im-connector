package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

type messageIdentity struct {
	ID       string `json:"message_id"`
	ChatID   string `json:"chat_id"`
	ThreadID string `json:"thread_id"`
	Type     string `json:"msg_type"`
	Deleted  bool   `json:"deleted"`
	Sender   struct {
		ID   string `json:"id"`
		Kind string `json:"id_type"`
		Type string `json:"sender_type"`
	} `json:"sender"`
}

// confirmedPublication converts only a provider-verified message lookup into
// a native locator. It deliberately returns no error: callers invoke it after
// a successful POST, where an unavailable lookup must not recategorize or
// retry the already-completed publication.
func (p *Provider) confirmedPublication(ctx context.Context, id string) (uvim.Publication, bool) {
	message, err := p.messageIdentity(ctx, id)
	if err != nil || message.Deleted || strings.TrimSpace(message.ID) != strings.TrimSpace(id) || strings.TrimSpace(message.ChatID) == "" {
		return uvim.Publication{}, false
	}
	if message.Sender.Type != "app" || message.Sender.Kind != "app_id" || message.Sender.ID != p.config.AppID {
		return uvim.Publication{}, false
	}
	return uvim.Publication{
		PartOrdinal:   1,
		DeliveryState: uvim.PublicationDeliveryConfirmed,
		Locator: uvim.PublicationLocator{
			Provider:             p.ID(),
			Connector:            p.ConnectorID(),
			MessageID:            message.ID,
			NativeConversationID: message.ChatID,
			BotAppID:             p.config.AppID,
			BotAppIDType:         "app_id",
			NativeTarget:         uvim.OutboundTarget{ID: message.ChatID, Kind: uvim.TargetConversation},
		},
	}, true
}

func (p *Provider) Update(ctx context.Context, msg uvim.MessageUpdate) (result uvim.SendResult, err error) {
	defer func() { err = uvim.NewProviderSendOperationError("lark update", err) }()
	if err := uvim.ValidateMessageUpdate(msg, p.Capabilities()); err != nil {
		return result, updatePreparationError(err)
	}
	if uvim.ProviderKey(msg.Provider, msg.Connector) != uvim.ProviderKey(p.ID(), p.ConnectorID()) {
		return result, updatePreparationError(fmt.Errorf("lark update: provider/connector mismatch"))
	}
	target := msg.ResolvedTarget()
	proofs := []string{msg.ChannelID, msg.Referrer.ChannelID}
	// An Open ID is not a chat ID. Check both supplied targets, including a
	// referrer target shadowed by an explicit target, before trusting any proof.
	for _, proof := range []*uvim.OutboundTarget{msg.Target, msg.Referrer.Target} {
		if proof == nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(proof.Kind))
		if (kind != uvim.TargetGroup && kind != uvim.TargetConversation) || proof.IDType != "" {
			return result, updatePreparationError(fmt.Errorf("lark update: conversation target is required"))
		}
		proofs = append(proofs, proof.ID)
	}
	if msg.Target == nil && msg.Referrer.Target == nil {
		proofs = append(proofs, target.ID)
	}
	chatID := ""
	for _, candidate := range proofs {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if chatID != "" && candidate != chatID {
			return result, updatePreparationError(fmt.Errorf("lark update: conflicting conversation proof"))
		}
		chatID = candidate
	}
	id := strings.TrimSpace(msg.MessageID)
	referenceID := strings.TrimSpace(msg.Referrer.MessageID)
	if chatID == "" && (referenceID == "" || referenceID == id) {
		return result, updatePreparationError(fmt.Errorf("lark update: original conversation proof is required"))
	}
	message, err := p.messageIdentity(ctx, id)
	if err != nil {
		return result, updatePreparationError(err)
	}
	if message.Sender.Type != "app" || message.Sender.Kind != "app_id" || message.Sender.ID != p.config.AppID {
		return result, uvim.NewProviderSendFailure(uvim.SendFailure{Category: uvim.SendFailurePermission, DeliveryState: uvim.DeliveryNotAttempted}, "lark update: message is not owned by this app", nil)
	}
	if message.Deleted || message.Type != "text" {
		return result, updatePreparationError(fmt.Errorf("lark update: existing text message is required"))
	}
	if chatID == "" {
		reference, err := p.messageIdentity(ctx, referenceID)
		if err != nil {
			return result, updatePreparationError(err)
		}
		chatID = reference.ChatID
	}
	if chatID == "" || message.ChatID != chatID || (msg.Referrer.ThreadID != "" && message.ThreadID != msg.Referrer.ThreadID) {
		return result, updatePreparationError(fmt.Errorf("lark update: message conversation/thread mismatch"))
	}
	content, _ := json.Marshal(map[string]string{"text": uvim.NormalizeOutboundText(msg.Text)})
	body, _ := json.Marshal(map[string]string{"msg_type": "text", "content": string(content)})
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return result, updatePreparationError(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, p.baseURL()+"/open-apis/im/v1/messages/"+url.PathEscape(id), bytes.NewReader(body))
	if err != nil {
		return result, updatePreparationError(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	raw, err := p.doJSON(req, "lark update")
	if err != nil {
		return result, err
	}
	var response struct {
		Code *int `json:"code"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return result, err
	}
	if response.Code == nil {
		return result, fmt.Errorf("lark update: response missing result code")
	}
	if *response.Code != 0 {
		return result, uvim.NewProviderResponseError(raw, "", fmt.Errorf("lark update: provider rejected request"))
	}
	if response.Data.MessageID != id {
		return result, fmt.Errorf("lark update: response message identity mismatch")
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: id, Time: p.now().UTC()}, nil
}

func (p *Provider) messageIdentity(ctx context.Context, id string) (messageIdentity, error) {
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return messageIdentity{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL()+"/open-apis/im/v1/messages/"+url.PathEscape(id), nil)
	if err != nil {
		return messageIdentity{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(req, "lark update lookup")
	if err != nil {
		return messageIdentity{}, err
	}
	var response struct {
		Code *int `json:"code"`
		Data struct {
			Items []messageIdentity `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return messageIdentity{}, err
	}
	if response.Code == nil {
		return messageIdentity{}, fmt.Errorf("lark update: lookup missing result code")
	}
	if *response.Code != 0 {
		return messageIdentity{}, uvim.NewProviderResponseError(raw, "", fmt.Errorf("lark update: lookup rejected"))
	}
	for _, message := range response.Data.Items {
		if message.ID == id {
			return message, nil
		}
	}
	return messageIdentity{}, fmt.Errorf("lark update: exact message not found")
}

// Lookup/authentication failures cannot have edited a message. Preserve their
// provider facts while making the update delivery boundary explicit.
func updatePreparationError(err error) error {
	err = uvim.NewProviderSendOperationError("lark update preparation", err)
	failure, _ := uvim.ProviderSendFailure(err)
	failure.DeliveryState = uvim.DeliveryNotAttempted
	return uvim.NewProviderSendFailure(failure, uvim.ProviderSendErrorDetail(err), err)
}
