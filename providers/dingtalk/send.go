package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

func (p *Provider) send(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() { err = uvim.NewProviderSendOperationError("dingtalk send", err) }()
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return result, err
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return result, err
	}
	result, err = uvim.SendResourceSequence(ctx, msg, p.sendOne)
	if err == nil && result.Provider == "" {
		err = fmt.Errorf("dingtalk send: text or resource is required")
	}
	return result, err
}

func (p *Provider) sendOne(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	if len(msg.Resources) == 0 && msg.Referrer.ReplyToken != "" {
		return p.base.Send(ctx, msg)
	}
	target := msg.ResolvedTarget()
	if target.ID == "" || (target.Kind != uvim.TargetUser && target.Kind != uvim.TargetGroup) {
		return uvim.SendResult{}, fmt.Errorf("dingtalk send: user or group target is required")
	}
	key, param := "sampleText", map[string]string{"content": uvim.NormalizeOutboundText(msg.Text)}
	if len(msg.Resources) == 1 {
		var err error
		key, param, err = p.uploadResource(ctx, msg.Resources[0])
		if err != nil {
			return uvim.SendResult{}, err
		}
	}
	encodedParam, err := json.Marshal(param)
	if err != nil {
		return uvim.SendResult{}, err
	}
	body := map[string]any{"robotCode": p.config.ClientID, "msgKey": key, "msgParam": string(encodedParam)}
	endpoint := "/v1.0/robot/groupMessages/send"
	if target.Kind == uvim.TargetUser {
		endpoint = "/v1.0/robot/oToMessages/batchSend"
		body["userIds"] = []string{target.ID}
	} else {
		body["openConversationId"] = target.ID
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return uvim.SendResult{}, err
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return uvim.SendResult{}, beforeSendError("dingtalk access token", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBaseURL()+endpoint, bytes.NewReader(raw))
	if err != nil {
		return uvim.SendResult{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	response, err := doDingTalkJSON(req)
	if err != nil {
		return uvim.SendResult{}, err
	}
	var decoded struct {
		ProcessQueryKey           string   `json:"processQueryKey"`
		Code                      string   `json:"code"`
		InvalidStaffIDList        []string `json:"invalidStaffIdList"`
		FlowControlledStaffIDList []string `json:"flowControlledStaffIdList"`
	}
	if err := json.Unmarshal(response, &decoded); err != nil {
		return uvim.SendResult{}, err
	}
	if decoded.Code != "" {
		err := fmt.Errorf("dingtalk send: provider rejected message")
		return uvim.SendResult{}, uvim.NewProviderResponseError(response, err.Error(), err)
	}
	if len(decoded.InvalidStaffIDList) > 0 {
		err := fmt.Errorf("dingtalk send: recipient rejected")
		return uvim.SendResult{}, uvim.NewProviderSendFailure(uvim.SendFailure{
			Category: uvim.SendFailureInvalidRequest, DeliveryState: uvim.DeliveryRejected,
		}, err.Error(), err)
	}
	if len(decoded.FlowControlledStaffIDList) > 0 {
		err := fmt.Errorf("dingtalk send: recipient rate limited")
		return uvim.SendResult{}, uvim.NewProviderSendFailure(uvim.SendFailure{
			Category: uvim.SendFailureRateLimited, DeliveryState: uvim.DeliveryRejected, Retryable: true,
		}, err.Error(), err)
	}
	if strings.TrimSpace(decoded.ProcessQueryKey) == "" {
		return uvim.SendResult{}, fmt.Errorf("dingtalk send: response missing delivery receipt")
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: decoded.ProcessQueryKey, Time: time.Now().UTC()}, nil
}

func (p *Provider) uploadResource(ctx context.Context, ref uvim.ResourceRef) (key string, param map[string]string, err error) {
	defer func() { err = beforeSendError("dingtalk upload", err) }()
	if p.config.ResourceStore == nil || !strings.HasPrefix(ref.InternalURL, "internal://") {
		return "", nil, fmt.Errorf("dingtalk upload: internal resource is required")
	}
	file, stored, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", nil, err
	}
	name := uvim.ResourceUploadName(0, ref, stored.MIME)
	mediaType := "file"
	if ref.Kind == uvim.ElementImage {
		mediaType = "image"
	}
	token, err := p.mediaAccessToken(ctx)
	if err != nil {
		return "", nil, err
	}
	// Keep the multipart envelope in memory; stream the original file bytes.
	var envelope bytes.Buffer
	writer := multipart.NewWriter(&envelope)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "media", "filename": name}))
	header.Set("Content-Type", firstNonEmpty(stored.MIME, "application/octet-stream"))
	if _, err := writer.CreatePart(header); err != nil {
		return "", nil, err
	}
	prefix := append([]byte(nil), envelope.Bytes()...)
	if err := writer.Close(); err != nil {
		return "", nil, err
	}
	suffix := envelope.Bytes()[len(prefix):]
	endpoint := p.config.BaseURL + "/media/upload?" + url.Values{"access_token": {token}, "type": {mediaType}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, io.MultiReader(bytes.NewReader(prefix), file, bytes.NewReader(suffix)))
	if err != nil {
		return "", nil, err
	}
	req.ContentLength = int64(len(prefix)) + info.Size() + int64(len(suffix))
	req.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := doDingTalkJSON(req)
	if err != nil {
		return "", nil, err
	}
	var decoded struct {
		ErrCode int    `json:"errcode"`
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(response, &decoded); err != nil {
		return "", nil, err
	}
	if decoded.ErrCode != 0 || decoded.MediaID == "" {
		err := fmt.Errorf("dingtalk upload: response did not accept media")
		return "", nil, uvim.NewProviderResponseError(response, err.Error(), err)
	}
	if mediaType == "image" {
		return "sampleImageMsg", map[string]string{"photoURL": decoded.MediaID}, nil
	}
	// Audio and video retain their bytes and filename as downloadable files.
	return "sampleFile", map[string]string{"mediaId": decoded.MediaID, "fileName": name, "fileType": strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), ".")}, nil
}

func (p *Provider) mediaAccessToken(ctx context.Context) (string, error) {
	endpoint := p.config.BaseURL + "/gettoken?" + url.Values{"appkey": {p.config.ClientID}, "appsecret": {p.config.ClientSecret}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	response, err := doDingTalkJSON(req)
	if err != nil {
		return "", err
	}
	var decoded struct {
		ErrCode int    `json:"errcode"`
		Token   string `json:"access_token"`
	}
	if err := json.Unmarshal(response, &decoded); err != nil {
		return "", err
	}
	if decoded.ErrCode != 0 || decoded.Token == "" {
		err := fmt.Errorf("dingtalk upload: token request rejected")
		return "", uvim.NewProviderResponseError(response, err.Error(), err)
	}
	return decoded.Token, nil
}

// Token and upload failures occur before a message send, even when the upload
// response is lost. The shared sequence owner still protects earlier sent parts.
func beforeSendError(operation string, err error) error {
	if err == nil {
		return nil
	}
	err = uvim.NewProviderSendOperationError(operation, err)
	failure, _ := uvim.ProviderSendFailure(err)
	failure.DeliveryState = uvim.DeliveryNotAttempted
	return uvim.NewProviderSendFailure(failure, uvim.ProviderSendErrorDetail(err), err)
}
