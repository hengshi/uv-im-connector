package uvim

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestEventDedupeKeyUsesProviderConnectorAndMessage(t *testing.T) {
	event := Event{Provider: "lark", Connector: "main", Channel: Channel{ID: "c1"}, Message: Message{ID: "m1"}}
	if got, want := event.DedupeKey(), "lark:main:event:c1:m1"; got != want {
		t.Fatalf("DedupeKey() = %q, want %q", got, want)
	}
}

func TestSanitizedResourceDropsProviderSecrets(t *testing.T) {
	ref := ResourceRef{
		Key:         "provider-key",
		URL:         "https://download.example",
		Secret:      "secret",
		Private:     map[string]string{"token": "x"},
		Metadata:    map[string]string{"message_id": "m1"},
		InternalURL: "internal://r1",
	}
	got := ref.Sanitized()
	if got.Key != "" || got.URL != "" || got.Secret != "" || got.Private != nil || got.Metadata != nil {
		t.Fatalf("sanitized resource leaked private fields: %+v", got)
	}
	if got.InternalURL != "internal://r1" {
		t.Fatalf("InternalURL = %q", got.InternalURL)
	}
}

func TestEventDedupeKeyIncludesTypeAndStableMessageID(t *testing.T) {
	create := Event{ID: "evt-1", Type: EventMessageCreate, Provider: "p", Connector: "c", Channel: Channel{ID: "room"}, Message: Message{ID: "m1"}}
	retry := Event{ID: "evt-2", Type: EventMessageCreate, Provider: "p", Connector: "c", Channel: Channel{ID: "room"}, Message: Message{ID: "m1"}}
	update := Event{ID: "evt-3", Type: EventMessageUpdate, Provider: "p", Connector: "c", Channel: Channel{ID: "room"}, Message: Message{ID: "m1"}}
	if create.DedupeKey() != retry.DedupeKey() {
		t.Fatalf("retry key changed: %q != %q", create.DedupeKey(), retry.DedupeKey())
	}
	if create.DedupeKey() == update.DedupeKey() {
		t.Fatalf("different event types share key: %q", create.DedupeKey())
	}
}

func TestEventDedupeKeySeparatesChannels(t *testing.T) {
	first := Event{Type: EventMessageCreate, Provider: "telegram", Connector: "main", Channel: Channel{ID: "chat-1"}, Message: Message{ID: "1"}}
	second := Event{Type: EventMessageCreate, Provider: "telegram", Connector: "main", Channel: Channel{ID: "chat-2"}, Message: Message{ID: "1"}}
	if first.DedupeKey() == second.DedupeKey() {
		t.Fatalf("different channels share key: %q", first.DedupeKey())
	}
}

func TestEventLogDedupesAfterSuccessfulAppend(t *testing.T) {
	log, err := NewEventLog("")
	if err != nil {
		t.Fatal(err)
	}
	event := Event{ID: "evt-1", Provider: "p", Connector: "c", Message: Message{ID: "m1"}}
	if _, fresh, err := log.Append(nil, event); err != nil || !fresh {
		t.Fatalf("first append fresh=%v err=%v", fresh, err)
	}
	if _, fresh, err := log.Append(nil, event); err != nil || fresh {
		t.Fatalf("second append fresh=%v err=%v", fresh, err)
	}
}

func TestSanitizedMessageDropsNestedElementResourceSecrets(t *testing.T) {
	msg := Message{
		Elements: []Element{{
			Type:  ElementFile,
			URL:   "https://download.example/file",
			Attrs: map[string]any{"raw_url": "https://download.example/file"},
			Resource: &ResourceRef{
				Key:         "provider-key",
				URL:         "https://download.example/file",
				InternalURL: "internal://r1",
				Metadata:    map[string]string{"message_id": "m1"},
				Secret:      "token",
			},
			Children: []Element{{
				Type: ElementImage,
				URL:  "https://download.example/image",
				Resource: &ResourceRef{
					Key:         "child-key",
					URL:         "https://download.example/image",
					InternalURL: "internal://r2",
					Secret:      "child-token",
				},
			}},
		}},
	}
	got := msg.Sanitized()
	if len(got.Elements) != 1 || got.Elements[0].Resource == nil {
		t.Fatalf("elements = %+v", got.Elements)
	}
	if ref := got.Elements[0].Resource; ref.Key != "" || ref.URL != "" || ref.Secret != "" || ref.Metadata != nil {
		t.Fatalf("nested resource leaked private fields: %+v", ref)
	}
	if got.Elements[0].URL != "internal://r1" {
		t.Fatalf("element URL = %q", got.Elements[0].URL)
	}
	child := got.Elements[0].Children[0]
	if child.Resource.Key != "" || child.Resource.URL != "" || child.Resource.Secret != "" {
		t.Fatalf("child resource leaked private fields: %+v", child.Resource)
	}
	if child.URL != "internal://r2" {
		t.Fatalf("child URL = %q", child.URL)
	}
	rawURLOnly := Message{Elements: []Element{{Type: ElementImage, URL: "https://download.example/raw", Attrs: map[string]any{"token": "secret"}}}}.Sanitized()
	if rawURLOnly.Elements[0].URL != "" || rawURLOnly.Elements[0].Attrs != nil {
		t.Fatalf("resource element leaked raw URL or attrs: %+v", rawURLOnly.Elements[0])
	}
}

func TestOutboundMessageResolvesExplicitAndLegacyTargets(t *testing.T) {
	tests := []struct {
		name string
		msg  OutboundMessage
		want OutboundTarget
	}{
		{
			name: "explicit user",
			msg:  OutboundMessage{Target: &OutboundTarget{ID: " ou_user ", Kind: " USER "}, ChannelID: "legacy"},
			want: OutboundTarget{ID: "ou_user", Kind: TargetUser},
		},
		{
			name: "legacy direct",
			msg:  OutboundMessage{ChannelID: "u1", ChannelType: ChannelDirect},
			want: OutboundTarget{ID: "u1", Kind: TargetUser},
		},
		{
			name: "legacy group",
			msg:  OutboundMessage{ChannelID: "g1", ChannelType: ChannelGroup},
			want: OutboundTarget{ID: "g1", Kind: TargetGroup},
		},
		{
			name: "referrer fallback",
			msg:  OutboundMessage{Referrer: Referrer{ChannelID: "c1"}},
			want: OutboundTarget{ID: "c1", Kind: TargetConversation},
		},
		{
			name: "referrer explicit target",
			msg:  OutboundMessage{Referrer: Referrer{ChannelID: "legacy", Target: &OutboundTarget{ID: " C1 ", Kind: " CHANNEL "}}},
			want: OutboundTarget{ID: "C1", Kind: TargetChannel},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.msg.ResolvedTarget(); got != tt.want {
				t.Fatalf("ResolvedTarget() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestOutboundTargetIsAdditiveJSON(t *testing.T) {
	raw, err := json.Marshal(OutboundMessage{
		Provider: "lark",
		Target:   &OutboundTarget{ID: "ou_user", Kind: TargetUser},
		Text:     "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	target, _ := got["target"].(map[string]any)
	if target["id"] != "ou_user" || target["kind"] != TargetUser {
		t.Fatalf("target = %+v", target)
	}
}

func TestValidateOutboundTarget(t *testing.T) {
	caps := Capabilities{ProactiveDirect: true, TargetKinds: []string{TargetUser}}
	if err := ValidateOutboundTarget(OutboundMessage{Target: &OutboundTarget{ID: "u1", Kind: TargetUser}}, caps); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []OutboundMessage{
		{Target: &OutboundTarget{Kind: TargetUser}},
		{Target: &OutboundTarget{ID: "u1"}},
		{Target: &OutboundTarget{ID: "u1", Kind: "unknown"}},
		{Target: &OutboundTarget{ID: "g1", Kind: TargetGroup}},
	} {
		if err := ValidateOutboundTarget(msg, caps); err == nil {
			t.Fatalf("ValidateOutboundTarget(%+v) error = nil", msg.Target)
		}
	}
	if err := ValidateOutboundTarget(OutboundMessage{ChannelID: "legacy", ChannelType: ChannelDirect}, caps); err != nil {
		t.Fatalf("legacy target rejected: %v", err)
	}
	if err := ValidateOutboundTarget(OutboundMessage{ChannelID: "legacy", ChannelType: ChannelGroup}, caps); err == nil {
		t.Fatal("unsupported legacy target kind was accepted")
	}
	noDirect := Capabilities{TargetKinds: []string{TargetUser}}
	if err := ValidateOutboundTarget(OutboundMessage{Target: &OutboundTarget{ID: "u1", Kind: TargetUser}}, noDirect); err == nil {
		t.Fatal("unsupported proactive direct send was accepted")
	}
	if err := ValidateOutboundTarget(OutboundMessage{ChannelID: "legacy", ChannelType: "bogus"}, caps); err == nil {
		t.Fatal("unknown legacy channel type was accepted")
	}
	if err := ValidateOutboundTarget(OutboundMessage{ChannelType: ChannelDirect}, caps); err == nil {
		t.Fatal("legacy target without a recipient was accepted")
	}
	replyCaps := Capabilities{ReplyMessage: true, TargetKinds: []string{TargetUser}}
	if err := ValidateOutboundTarget(OutboundMessage{Referrer: Referrer{MessageID: "m1"}}, replyCaps); err != nil {
		t.Fatalf("legacy reply handle rejected: %v", err)
	}
}

func TestProviderSendErrorDetail(t *testing.T) {
	internal := errors.New("request URL contains a secret")
	err := NewProviderSendError("provider rejected recipient", internal)
	if got := ProviderSendErrorDetail(err); got != "provider rejected recipient" {
		t.Fatalf("ProviderSendErrorDetail() = %q", got)
	}
	if !errors.Is(err, internal) {
		t.Fatal("provider send error does not preserve its internal cause")
	}
	if got := ProviderSendErrorDetail(internal); got != "" {
		t.Fatalf("unmarked error detail = %q", got)
	}
}
