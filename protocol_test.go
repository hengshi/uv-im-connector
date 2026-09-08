package uvim

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
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

func TestEventSanitizedFillsDirectChannelNameFromUser(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "display name",
			event: Event{Channel: Channel{Type: ChannelDirect}, User: User{Name: "Alice", DisplayName: "Ada"}},
			want:  "Ada",
		},
		{
			name:  "name fallback",
			event: Event{Channel: Channel{Type: ChannelDirect}, User: User{Name: "Alice"}},
			want:  "Alice",
		},
		{
			name:  "explicit channel name",
			event: Event{Channel: Channel{Type: ChannelDirect, Name: "Support"}, User: User{Name: "Alice"}},
			want:  "Support",
		},
		{
			name:  "group remains unnamed",
			event: Event{Channel: Channel{Type: ChannelGroup}, User: User{Name: "Alice"}},
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.Sanitized().Channel.Name; got != tt.want {
				t.Fatalf("channel name = %q, want %q", got, tt.want)
			}
		})
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

func TestReplyHandlePolicyIsAdditiveJSON(t *testing.T) {
	expiresAt := time.Date(2026, 7, 16, 10, 10, 0, 0, time.UTC)
	raw, err := json.Marshal(Event{
		ID:       "evt-1",
		Provider: "line",
		Referrer: Referrer{ReplyToken: "reply-1", ExpiresAt: &expiresAt},
	})
	if err != nil {
		t.Fatal(err)
	}
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("reply expiry = %v, want %v", event.Referrer.ExpiresAt, expiresAt)
	}

	raw, err = json.Marshal(Capabilities{ReplyMessage: true, ReplyMaxUses: 1})
	if err != nil {
		t.Fatal(err)
	}
	var capabilities Capabilities
	if err := json.Unmarshal(raw, &capabilities); err != nil {
		t.Fatal(err)
	}
	if capabilities.ReplyMaxUses != 1 {
		t.Fatalf("reply max uses = %d, want 1", capabilities.ReplyMaxUses)
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

func TestValidateOutboundResources(t *testing.T) {
	caps := Capabilities{UploadResource: true, ResourceKinds: []string{ElementFile, ElementImage}}
	if err := ValidateOutboundResources(OutboundMessage{Resources: []ResourceRef{{Kind: ElementFile}}}, caps); err != nil {
		t.Fatalf("supported resource rejected: %v", err)
	}
	for _, test := range []OutboundMessage{
		{Resources: []ResourceRef{{Kind: "archive"}}},
		{Resources: []ResourceRef{{Kind: ""}}},
	} {
		if err := ValidateOutboundResources(test, caps); err == nil {
			t.Fatalf("unsupported resources accepted: %+v", test.Resources)
		}
	}
	if err := ValidateOutboundResources(OutboundMessage{Resources: []ResourceRef{{Kind: ElementFile}}}, Capabilities{}); err == nil {
		t.Fatal("resource accepted when uploads are disabled")
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

func TestProviderSendFailureClassifiesHTTPResponsesWithoutExposingBody(t *testing.T) {
	failure := ProviderHTTPFailure(http.StatusTooManyRequests, http.Header{
		"Retry-After":  []string{"17"},
		"X-Request-Id": []string{"request-123"},
	}, []byte(`{"error":{"code":429001,"message":"token=secret"}}`))
	if failure.Category != SendFailureRateLimited || !failure.Retryable || failure.DeliveryState != DeliveryRejected {
		t.Fatalf("failure classification = %+v", failure)
	}
	if failure.HTTPStatus != http.StatusTooManyRequests || failure.ProviderCode != "429001" || failure.RetryAfterSeconds != 17 || failure.RequestID != "request-123" {
		t.Fatalf("failure evidence = %+v", failure)
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret") {
		t.Fatalf("failure leaked response body: %s", raw)
	}
}

func TestProviderHTTPFailureParsesHTTPDateRetryAfterDeterministically(t *testing.T) {
	// HTTP dates have one-second precision. Keeping now on a half-second
	// boundary proves the remaining duration is rounded up, not truncated.
	now := time.Date(2026, time.August, 17, 9, 30, 0, 500_000_000, time.UTC)
	for _, test := range []struct {
		name  string
		delay time.Duration
		want  int
	}{
		{name: "remaining seconds", delay: 17 * time.Second, want: 17},
		{name: "full provider delay", delay: 2 * time.Hour, want: 7200},
		{name: "elapsed", delay: -time.Second, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := providerHTTPFailureAt(http.StatusServiceUnavailable, http.Header{
				"Retry-After": []string{now.Add(test.delay).Format(http.TimeFormat)},
			}, nil, now)
			if failure.Category != SendFailureProviderUnavailable || !failure.Retryable || failure.RetryAfterSeconds != test.want {
				t.Fatalf("failure = %+v", failure)
			}
		})
	}
}

func TestProviderHTTPFailureCapsOverflowingDeltaSeconds(t *testing.T) {
	failure := providerHTTPFailureAt(http.StatusTooManyRequests, http.Header{
		"Retry-After": []string{"999999999999999999999999999999999999"},
	}, nil, time.Time{})
	if failure.RetryAfterSeconds != int(^uint(0)>>1) {
		t.Fatalf("failure = %+v", failure)
	}
}

func TestProviderResponseFailureDoesNotPromoteArbitraryErrorText(t *testing.T) {
	internal := errors.New("provider returned access_token=secret")
	err := NewProviderResponseError(
		[]byte(`{"error":"xoxb-secret","message":"access_token=secret"}`),
		"provider rejected request: access_token=secret",
		internal,
	)
	failure, ok := ProviderSendFailure(err)
	if !ok {
		t.Fatal("provider response error has no normalized failure")
	}
	if failure.ProviderCode != "" || failure.Retryable || failure.DeliveryState != DeliveryRejected {
		t.Fatalf("failure = %+v", failure)
	}
	if got := ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail leaked provider text: %q", got)
	}
	if got := ProviderSendErrorLogDetail(err); got != "provider rejected request" {
		t.Fatalf("private log detail = %q", got)
	}
	if !errors.Is(err, internal) {
		t.Fatal("provider response error does not preserve its internal cause")
	}
}

func TestProviderResponseFailureAcceptsOnlySafeTypedCode(t *testing.T) {
	for _, test := range []struct {
		name string
		code string
		want string
	}{
		{name: "safe code", code: "channel_not_found", want: "channel_not_found"},
		{name: "unsafe provider text", code: "access_token=secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := NewProviderResponseErrorWithCode(nil, test.code, errors.New("provider rejected request"))
			failure, ok := ProviderSendFailure(err)
			if !ok || failure.ProviderCode != test.want {
				t.Fatalf("failure = %+v, ok=%v", failure, ok)
			}
		})
	}
}

func TestProviderSendOperationErrorAlwaysCarriesNormalizedFailure(t *testing.T) {
	transport := &url.Error{Op: "Post", URL: "https://api.example.test/send?token=secret", Err: context.DeadlineExceeded}
	err := NewProviderSendOperationError("slack send", transport)
	failure, ok := ProviderSendFailure(err)
	if !ok {
		t.Fatal("transport error has no normalized send failure")
	}
	if failure.Category != SendFailureTimeout || !failure.Retryable || failure.DeliveryState != DeliveryUnknown {
		t.Fatalf("transport failure = %+v", failure)
	}
}

func TestProviderSendErrorLogDetail(t *testing.T) {
	internal := errors.New("request URL contains a secret")
	marked := NewProviderSendError("provider rejected recipient", internal)
	if got := ProviderSendErrorLogDetail(marked); got != "provider rejected recipient" {
		t.Fatalf("marked log detail = %q", got)
	}
	private := NewProviderSendLogError("decode lark send response: unexpected EOF", internal)
	if got := ProviderSendErrorLogDetail(private); got != "decode lark send response: unexpected EOF" {
		t.Fatalf("private log detail = %q", got)
	}
	if got := ProviderSendErrorDetail(private); got != "" {
		t.Fatalf("private public detail = %q", got)
	}
	if !errors.Is(private, internal) {
		t.Fatal("private log error does not preserve its internal cause")
	}

	transport := &url.Error{
		Op:  "Post",
		URL: "https://api.example.test/bot-secret/send?access_token=query-secret",
		Err: context.DeadlineExceeded,
	}
	got := ProviderSendErrorLogDetail(transport)
	if got != `Post "https://api.example.test": provider request timed out` {
		t.Fatalf("transport log detail = %q", got)
	}
	nested := &url.Error{
		Op:  "Post",
		URL: "https://api.example.test/send?access_token=outer-secret",
		Err: &url.Error{
			Op:  "Get",
			URL: "https://download.example.test/private/file?token=inner-secret",
			Err: context.DeadlineExceeded,
		},
	}
	if got := ProviderSendErrorLogDetail(nested); got != `Post "https://api.example.test": Get "https://download.example.test": provider request timed out` {
		t.Fatalf("nested transport log detail = %q", got)
	}
	refused := &url.Error{
		Op:  "Post",
		URL: "https://api.example.test/private/send?access_token=secret",
		Err: &net.OpError{
			Op:   "dial",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443},
			Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
		},
	}
	if got := ProviderSendErrorLogDetail(refused); got != `Post "https://api.example.test": dial tcp: connect: connection refused` {
		t.Fatalf("connection-refused log detail = %q", got)
	}
	dnsFailure := &url.Error{
		Op:  "Post",
		URL: "https://api.example.test/private/send?access_token=secret",
		Err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: &net.DNSError{Err: "lookup secret.internal: no such host", Name: "secret.internal", IsNotFound: true},
		},
	}
	if got := ProviderSendErrorLogDetail(dnsFailure); got != `Post "https://api.example.test": dial tcp: dns name not found` {
		t.Fatalf("dns log detail = %q", got)
	}
	tlsFailure := &url.Error{
		Op:  "Post",
		URL: "https://api.example.test/private/send?access_token=secret",
		Err: &net.OpError{Op: "read", Net: "tcp", Err: x509.UnknownAuthorityError{}},
	}
	if got := ProviderSendErrorLogDetail(tlsFailure); got != `Post "https://api.example.test": read tcp: tls certificate signed by unknown authority` {
		t.Fatalf("tls log detail = %q", got)
	}
	plain := errors.New("POST https://user:password@example.test/private?access_token=secret failed")
	if got := ProviderSendErrorLogDetail(plain); got != "unmarked provider error" {
		t.Fatalf("unmarked log detail = %q", got)
	}
}

func TestNewProviderSendOperationErrorPreservesSafeCauseAndHidesUnknownText(t *testing.T) {
	transport := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	err := NewProviderSendOperationError("slack upload", transport)
	if got := ProviderSendErrorLogDetail(err); got != "slack upload: read tcp: connection reset" {
		t.Fatalf("transport log detail = %q", got)
	}
	if !errors.Is(err, transport) {
		t.Fatal("transport cause was not preserved")
	}
	secret := errors.New("POST https://user:password@example.test/private?access_token=secret failed")
	err = NewProviderSendOperationError("discord create dm", secret)
	if got := ProviderSendErrorLogDetail(err); got != "discord create dm" {
		t.Fatalf("unknown log detail = %q", got)
	}
	if got := ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("unknown public detail = %q", got)
	}
	if !errors.Is(err, secret) {
		t.Fatal("unknown cause was not preserved")
	}
	publicErr := NewProviderSendLogError("matrix upload: http 502", NewProviderSendError("matrix send: http 502", secret))
	if got := ProviderSendErrorLogDetail(publicErr); got != "matrix upload: http 502" {
		t.Fatalf("private stage detail = %q", got)
	}
	if got := ProviderSendErrorDetail(publicErr); got != "matrix send: http 502" {
		t.Fatalf("public compatibility detail = %q", got)
	}
}
