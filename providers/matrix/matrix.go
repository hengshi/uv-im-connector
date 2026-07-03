package matrix

import (
	"encoding/json"
	"net/url"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

type Config struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	WebhookSecret string
}

func New(config Config) (*httpchannel.Provider, error) {
	return httpchannel.New(httpchannel.Config{
		ProviderID:    "matrix",
		ConnectorID:   firstNonEmpty(config.ConnectorID, "matrix"),
		BaseURL:       config.BaseURL,
		Token:         config.Token,
		WebhookSecret: config.WebhookSecret,
		Decode:        Decode,
		Send:          Send,
		Capabilities: uvim.Capabilities{
			Inbound:      true,
			Outbound:     true,
			GroupMessage: true,
			ChannelTypes: []string{uvim.ChannelGroup},
		},
	})
}

func Decode(raw []byte, config httpchannel.Config) (uvim.Event, bool, error) {
	var event struct {
		EventID string `json:"event_id"`
		RoomID  string `json:"room_id"`
		Sender  string `json:"sender"`
		Type    string `json:"type"`
		Content struct {
			Body    string `json:"body"`
			MsgType string `json:"msgtype"`
			URL     string `json:"url"`
			Info    struct {
				MIME string `json:"mimetype"`
				Size int64  `json:"size"`
			} `json:"info"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return uvim.Event{}, false, err
	}
	if event.EventID == "" || event.Type != "m.room.message" {
		return uvim.Event{}, false, nil
	}
	refs := matrixResources(event.Content.URL, event.Content.Body, event.Content.MsgType, event.Content.Info.MIME, event.Content.Info.Size, config)
	return uvim.Event{
		ID:        event.EventID,
		Type:      uvim.EventMessageCreate,
		Provider:  "matrix",
		Connector: config.ConnectorID,
		Channel:   uvim.Channel{ID: event.RoomID, Type: uvim.ChannelGroup},
		User:      uvim.User{ID: event.Sender},
		Message:   uvim.Message{ID: event.EventID, Text: event.Content.Body, Type: event.Content.MsgType, Resources: refs},
		Referrer:  uvim.Referrer{MessageID: event.EventID, ChannelID: event.RoomID},
		Addressed: true,
	}, true, nil
}

func Send(msg uvim.OutboundMessage, _ httpchannel.Config) (httpchannel.Request, error) {
	txnID := url.PathEscape(uvim.FirstNonEmpty(msg.ID, uvim.NewID("txn")))
	roomID := url.PathEscape(msg.ChannelID)
	return httpchannel.Request{
		Method: "PUT",
		Path:   "/_matrix/client/v3/rooms/" + roomID + "/send/m.room.message/" + txnID,
		Body:   map[string]string{"msgtype": "m.text", "body": msg.Text},
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func matrixResources(rawURL, name, msgType, mime string, size int64, config httpchannel.Config) []uvim.ResourceRef {
	if rawURL == "" || msgType == "m.text" {
		return nil
	}
	downloadURL := rawURL
	if strings.HasPrefix(rawURL, "mxc://") {
		downloadURL = matrixDownloadURL(config.BaseURL, rawURL)
	}
	if downloadURL == "" {
		return nil
	}
	kind := uvim.ResourceKindFromMIME(mime, uvim.ElementFile)
	switch strings.ToLower(strings.TrimSpace(msgType)) {
	case "m.image":
		kind = uvim.ElementImage
	case "m.audio":
		kind = uvim.ElementAudio
	case "m.video":
		kind = uvim.ElementVideo
	}
	return []uvim.ResourceRef{{
		Provider:  "matrix",
		Connector: config.ConnectorID,
		Kind:      kind,
		Name:      name,
		URL:       downloadURL,
		MIME:      mime,
		SizeBytes: size,
		Secret:    config.Token,
	}}
}

func matrixDownloadURL(baseURL, mxc string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" || !strings.HasPrefix(mxc, "mxc://") {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(mxc, "mxc://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	return baseURL + "/_matrix/media/v3/download/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1])
}
