package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/server"
)

// Runs the real WebSocket adapter and Hub with a local Feishu API stub.
func TestQuotedMessageResourcesThroughLarkTransportStub(t *testing.T) {
	for _, scenario := range []string{"auto-file", "auto-other", "file", "folder", "folder-failed", "text", "interactive", "share-chat", "share-user", "system", "merge-forward", "rich-post", "rich-post-v2-only", "rich-post-fallback", "rich-post-markers", "rich-post-unavailable", "denied", "deleted", "wrong-chat", "wrong-message", "download-failed", "unaddressed"} {
		t.Run(scenario, func(t *testing.T) {
			var api *httptest.Server
			api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/callback/ws/endpoint":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(api.URL, "http:", "ws:", 1) + "/ws?service_id=1"}})
				case "/ws":
					conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					mentions := []any{map[string]any{"key": "@bot", "id": map[string]any{"open_id": "bot"}}}
					if scenario == "auto-other" {
						mentions = []any{map[string]any{"key": "@bot", "id": map[string]any{"open_id": "other-bot"}}}
					}
					if scenario == "unaddressed" {
						mentions = nil
					}
					raw, _ := json.Marshal(map[string]any{"header": map[string]any{"event_type": "im.message.receive_v1"}, "event": map[string]any{"message": map[string]any{
						"message_id": "current", "parent_id": "parent", "root_id": "unrelated-root", "chat_id": "chat", "chat_type": "group", "message_type": "text", "content": `{"text":"analyze this log"}`, "mentions": mentions,
					}}})
					frame := &wsFrame{Method: frameMethodData, Headers: []frameHeader{{Key: frameHeaderMessageID, Value: "current"}}, Payload: raw}
					if err := conn.WriteMessage(websocket.BinaryMessage, frame.marshal()); err != nil {
						t.Error(err)
						return
					}
					conn.SetReadDeadline(time.Now().Add(3 * time.Second))
					if _, _, err := conn.ReadMessage(); err != nil {
						t.Error(err)
					}
				case "/open-apis/auth/v3/tenant_access_token/internal":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/bot/v3/info":
					if req.Header.Get("Authorization") != "Bearer token" {
						t.Error("missing bot identity")
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "bot": map[string]any{"open_id": "bot"}})
				case "/open-apis/im/v1/chats/chat":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "group"}})
				case "/open-apis/im/v1/messages/parent":
					if scenario == "unaddressed" || scenario == "auto-other" {
						t.Error("fetched quote for an unaddressed group message")
					}
					if req.Header.Get("Authorization") != "Bearer token" {
						t.Error("missing bot identity")
					}
					if scenario == "denied" {
						json.NewEncoder(w).Encode(map[string]any{"code": 99991672, "msg": "private diagnostic"})
						return
					}
					kind, content := "file", `{"file_key":"log-key","file_name":"device.log"}`
					if strings.HasPrefix(scenario, "folder") {
						kind = "folder"
					}
					if scenario == "text" {
						kind, content = "text", `{"text":"fatal: tracking failed\n"}`
					}
					if scenario == "interactive" {
						kind, content = "interactive", `{"elements":[{"tag":"img","img_key":"card-image"}]}`
					}
					if scenario == "share-chat" {
						kind, content = "share_chat", `{"chat_id":"shared-chat"}`
					}
					if scenario == "share-user" {
						kind, content = "share_user", `{"user_id":"shared-user"}`
					}
					if scenario == "system" {
						kind, content = "system", `{}`
					}
					if scenario == "merge-forward" {
						kind, content = "merge_forward", "[Merged forward placeholder]"
					}
					if scenario == "rich-post" {
						kind, content = "post", `{"title":"Classic title","content":[[{"tag":"text","text":"before"},{"tag":"a","text":"site","href":"https://example.test/a?x=1"},{"tag":"at","user_id":"ou_123","user_name":"Ada"}],[{"tag":"code_block","language":"go","text":"  fmt.Println(1)\n\t// exact"}],[{"tag":"md","text":"**original markdown**"},{"tag":"hr"},{"tag":"emotion","emoji_type":"SMILE"}]]}`
					}
					if scenario == "rich-post-v2-only" {
						kind, content = "post", `{"content_v2":[[{"tag":"md","text":"v2 only"}]],"files":[{"file_key":"top-file","file_name":"top-file.txt"},{"file_key":"top-folder","file_name":"assets\\\"<unsafe>","is_folder":true}]}`
					}
					if scenario == "rich-post-fallback" {
						kind, content = "post", `{"content_v2":[],"content":[[{"tag":"text","text":"legacy fallback"}]]}`
					}
					if scenario == "rich-post-markers" {
						kind, content = "post", `{"zh_cn":{"content":[[{"tag":"text","text":"known"},{"tag":"mystery","text":"raw unknown"},17],"bad paragraph"]}}`
					}
					if scenario == "rich-post-unavailable" {
						kind, content = "post", `{"zh_cn":{"content":[[{"tag":"mystery"}],"bad paragraph"]}}`
					}
					chat, id := "chat", "parent"
					if scenario == "wrong-chat" {
						chat = "other-chat"
					}
					if scenario == "wrong-message" {
						id = "other-message"
					}
					items := []any{map[string]any{
						"message_id": id, "chat_id": chat, "msg_type": kind, "deleted": scenario == "deleted", "body": map[string]any{"content": content},
					}}
					if scenario == "interactive" {
						items[0].(map[string]any)["sender"] = map[string]any{"id": "app", "id_type": "app_id", "sender_type": "app"}
					}
					if scenario == "text" {
						items[0].(map[string]any)["sender"] = map[string]any{"id": "other-app", "id_type": "app_id", "sender_type": "app"}
					}
					if scenario == "share-chat" {
						items[0].(map[string]any)["sender"] = map[string]any{"id": "app", "id_type": "open_id", "sender_type": "user"}
					}
					if scenario == "merge-forward" {
						// The native response uses upper_message_id for the forwarded
						// hierarchy. The first child deliberately omits it: Feishu's
						// documented CLI keeps such legacy rows directly below the selected
						// container, so the connector must preserve that compatibility.
						items[0].(map[string]any)["sender"] = map[string]any{"id": "app", "id_type": "app_id", "sender_type": "app"}
						items[0].(map[string]any)["create_time"] = "1793930000000"
						items = append(items,
							map[string]any{"message_id": "child", "chat_id": "chat", "msg_type": "file", "sender": map[string]any{"id": "speaker-file-child", "id_type": "open_id", "sender_type": "user"}, "create_time": "1793930000001", "body": map[string]any{"content": `{"file_key":"forwarded-file","file_name":"same-name.bin"}`}},
							map[string]any{"message_id": "nested", "chat_id": "chat", "upper_message_id": "parent", "msg_type": "merge_forward", "sender": map[string]any{"id": "speaker-nested\ninjected", "id_type": "open_id", "sender_type": "user"}, "create_time": "1793930000002", "body": map[string]any{"content": "[Nested merged forward placeholder]"}},
							map[string]any{"message_id": "grandchild", "upper_message_id": "nested", "msg_type": "file", "sender": map[string]any{"id": "speaker-nested-file", "id_type": "open_id", "sender_type": "user"}, "create_time": "1793930000003", "body": map[string]any{"content": `{"file_key":"nested-file","file_name":"same-name.bin"}`}},
							map[string]any{"message_id": "image-child", "upper_message_id": "parent", "msg_type": "image", "sender": map[string]any{"id": "speaker-image-child", "id_type": "open_id", "sender_type": "user"}, "create_time": "1793930000004", "body": map[string]any{"content": `{"image_key":"nested-image"}`}},
							map[string]any{"message_id": "wrong-chat", "chat_id": "other-chat", "upper_message_id": "parent", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"wrong-chat-file"}`}},
							map[string]any{"message_id": "orphan", "upper_message_id": "missing", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"orphan-file"}`}},
							map[string]any{"message_id": "deleted-child", "upper_message_id": "parent", "msg_type": "file", "deleted": true, "body": map[string]any{"content": `{"file_key":"deleted-file"}`}},
							map[string]any{"message_id": "duplicate", "upper_message_id": "parent", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"duplicate-one"}`}},
							map[string]any{"message_id": "duplicate", "upper_message_id": "parent", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"duplicate-two"}`}},
							map[string]any{"message_id": "cycle-a", "upper_message_id": "cycle-b", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"cycle-a"}`}},
							map[string]any{"message_id": "cycle-b", "upper_message_id": "cycle-a", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"cycle-b"}`}},
							map[string]any{"message_id": "malformed", "upper_message_id": "parent", "msg_type": "file", "body": map[string]any{"content": `{`}},
							map[string]any{"message_id": "post-child", "chat_id": "chat", "upper_message_id": "parent", "msg_type": "post", "body": map[string]any{"content": `{"zh_cn":{"title":"selected zh","content":[[{"tag":"text","text":"legacy text"},{"tag":"media","file_key":"shared-media"}]],"content_v2":[[{"tag":"md","text":"v2 nested body"},{"tag":"img","image_key":"post-image"},{"tag":"media","file_key":"shared-media"}]]},"en_us":{"title":"wrong locale","content":[[{"tag":"text","text":"wrong locale body"}]]}}`}},
						)
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items}})
				case "/open-apis/im/v1/messages/parent/resources/log-key":
					if scenario != "auto-file" && scenario != "file" && scenario != "folder" && scenario != "folder-failed" && scenario != "download-failed" {
						t.Error("downloaded an unverified quote")
					}
					if scenario == "folder-failed" {
						w.WriteHeader(http.StatusInternalServerError)
						json.NewEncoder(w).Encode(map[string]any{"code": 40009, "msg": "internal server error"})
						return
					}
					if scenario == "download-failed" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					io.WriteString(w, "fatal: tracking failed\n")
				case "/open-apis/im/v1/messages/parent/resources/forwarded-file":
					if scenario != "merge-forward" {
						t.Error("downloaded an unexpected forwarded resource")
					}
					io.WriteString(w, "forwarded attachment\n")
				case "/open-apis/im/v1/messages/parent/resources/nested-file":
					if scenario != "merge-forward" {
						t.Error("downloaded an unexpected nested forwarded resource")
					}
					io.WriteString(w, "nested forwarded attachment\n")
				case "/open-apis/im/v1/messages/parent/resources/nested-image":
					if scenario != "merge-forward" {
						t.Error("downloaded an unexpected nested resource")
					}
					io.WriteString(w, "nested image\n")
				case "/open-apis/im/v1/messages/parent/resources/post-image":
					if scenario != "merge-forward" {
						t.Error("downloaded an unexpected post image")
					}
					io.WriteString(w, "post image\n")
				case "/open-apis/im/v1/messages/parent/resources/shared-media":
					if scenario != "merge-forward" {
						t.Error("downloaded unexpected post media")
					}
					io.WriteString(w, "post media\n")
				case "/open-apis/im/v1/messages/parent/resources/top-file":
					if scenario != "rich-post-v2-only" {
						t.Error("downloaded an unexpected top-level post file")
					}
					io.WriteString(w, "top file\n")
				default:
					t.Errorf("unexpected request %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			providerStore := store
			if scenario == "text" {
				providerStore = nil
			} else if scenario == "auto-file" {
				providerStore = &uvim.ResourceStore{Dir: t.TempDir()}
			}
			botID := "bot"
			if strings.HasPrefix(scenario, "auto-") {
				botID = ""
			}
			var identityLogs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&identityLogs, nil))
			provider, err := New(Config{Logger: logger, AppID: "app", AppSecret: "secret", BotOpenID: botID, BaseURL: api.URL, CallbackBaseURL: api.URL, ResourceStore: providerStore})
			if err != nil {
				t.Fatal(err)
			}
			log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_ = provider.Run(ctx, hub) // Peer disconnect drains the queued event.
			source := "configured"
			if botID == "" {
				source = "discovered"
			}
			if !strings.Contains(identityLogs.String(), "source="+source) || !strings.Contains(identityLogs.String(), "open_id=bot") {
				t.Fatalf("identity diagnostic missing: %s", identityLogs.String())
			}
			events, err := log.ReadAfter(t.Context(), 0)
			if err != nil || len(events) != 1 {
				t.Fatalf("events=%+v err=%v", events, err)
			}
			event := events[0]
			if event.Message.Text != "analyze this log" || event.Referrer.MessageID != "current" || event.Referrer.ParentMessageID != "parent" {
				t.Fatalf("changed instruction/reply authority: %+v", event)
			}
			if scenario == "unaddressed" || scenario == "auto-other" {
				if event.Addressed {
					t.Fatal("another mention admitted as bot")
				}
				if len(event.Message.Resources) != 0 {
					t.Fatal("unexpected quote resources")
				}
				return
			}
			if scenario == "interactive" && len(event.Message.Resources) != 1 {
				t.Fatalf("interactive resources = %+v", event.Message.Resources)
			}
			if scenario == "merge-forward" && len(event.Message.Resources) != 6 {
				t.Fatalf("forwarded resources = %+v", event.Message.Resources)
			}
			if scenario == "rich-post-v2-only" && len(event.Message.Resources) != 2 {
				t.Fatalf("direct v2 post resources = %+v", event.Message.Resources)
			}
			if !event.Addressed {
				t.Fatal("bot mention was not recognized")
			}
			if len(event.Message.Resources) == 0 {
				t.Fatal("quoted message context missing from emitted resources")
			}
			var contents string
			var failures []string
			for _, ref := range event.Message.Resources {
				if ref.SourceMessageID != "parent" {
					t.Fatalf("quote resource lost parent lineage through Hub sanitization: %+v", ref)
				}
				wantBotApp := ""
				if scenario == "interactive" || scenario == "merge-forward" {
					wantBotApp = "app"
				}
				if ref.SourceBotAppID != wantBotApp {
					t.Fatalf("quoted bot ownership = %q, want %q", ref.SourceBotAppID, wantBotApp)
				}
				if ref.Error != "" {
					failures = append(failures, ref.Error)
					continue
				}
				file, _, err := store.Open(ref.InternalURL)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(file)
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
				contents += ref.Name + "\n" + string(data)
			}
			if strings.HasPrefix(scenario, "folder") && (!strings.Contains(contents, `Message type: "folder"`) || !strings.Contains(contents, "ZIP archive")) {
				t.Fatalf("folder context missing: %q", contents)
			}
			switch scenario {
			case "auto-file", "file", "folder", "text", "interactive", "share-chat", "share-user", "system", "merge-forward", "rich-post", "rich-post-v2-only", "rich-post-fallback", "rich-post-markers":
				if !strings.Contains(contents, "parent") || len(failures) != 0 {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				if scenario == "auto-file" || scenario == "file" || scenario == "folder" || scenario == "text" {
					if !strings.Contains(contents, "fatal: tracking failed") {
						t.Fatal("message content missing")
					}
				}
				if (scenario == "auto-file" || scenario == "file") && !strings.Contains(contents, "device.log") {
					t.Fatal("log attachment missing")
				}
				if scenario == "interactive" && !strings.Contains(contents, "Interactive card") {
					t.Fatal("interactive context missing")
				}
				if scenario == "share-chat" && !strings.Contains(contents, "shared-chat") {
					t.Fatal("shared chat context missing")
				}
				if scenario == "share-user" && !strings.Contains(contents, "shared-user") {
					t.Fatal("shared user context missing")
				}
				if scenario == "system" && !strings.Contains(contents, "System message") {
					t.Fatal("system context missing")
				}
				if scenario == "text" && !strings.HasSuffix(contents, "  fatal: tracking failed\n") {
					t.Fatalf("trailing body newline acquired a dangling indentation prefix: %q", contents)
				}
				if scenario == "merge-forward" {
					if !strings.Contains(contents, `Message ID: "parent"`) || !strings.Contains(contents, `Container message ID: "parent"`) || !strings.Contains(contents, `Message type: "merge_forward"`) || !strings.Contains(contents, `Sender: sender_type="app" id_type="app_id" id="app"`) || !strings.Contains(contents, `Create time: "1793930000000"`) {
						t.Fatalf("merge-forward root provenance missing: %q", contents)
					}
					if !strings.Contains(contents, "Forwarded messages unavailable") || strings.Contains(contents, "wrong-chat-file") || strings.Contains(contents, "orphan-file") {
						t.Fatalf("forwarded context did not isolate invalid rows: %q", contents)
					}
					quote := findQuotedElement(event.Message.Elements, "parent")
					if quote == nil {
						t.Fatalf("forwarded quote root missing: %+v", event.Message.Elements)
					}
					child := findQuotedElement(quote.Children, "child")
					nested := findQuotedElement(quote.Children, "nested")
					if child == nil || nested == nil {
						t.Fatalf("forwarded quote hierarchy = %+v", event.Message.Elements)
					}
					grandchild := findQuotedElement(nested.Children, "grandchild")
					if grandchild == nil {
						t.Fatalf("forwarded quote hierarchy = %+v", event.Message.Elements)
					}
					if findQuotedElement(quote.Children, "wrong-chat") != nil || findQuotedElement(quote.Children, "orphan") != nil || findQuotedElement(quote.Children, "duplicate") != nil || findQuotedElement(quote.Children, "cycle-a") != nil {
						t.Fatalf("invalid forwarded material entered hierarchy: %+v", quote.Children)
					}
					for _, node := range []*uvim.Element{child, grandchild} {
						if len(node.Children) != 2 || node.Children[0].Type != uvim.ElementText || node.Children[1].Resource == nil || node.Children[1].Resource.InternalURL == "" || node.Children[1].Resource.Key != "" || node.Children[1].Resource.Metadata != nil {
							t.Fatalf("forwarded resource was not safely resolved in hierarchy: %+v", node)
						}
					}
					childRef, grandchildRef := child.Children[1].Resource, grandchild.Children[1].Resource
					if childRef.Name != "same-name.bin" || grandchildRef.Name != "same-name.bin" || childRef.ID == "" || grandchildRef.ID == "" || childRef.ID == grandchildRef.ID {
						t.Fatalf("same-name forwarded resources lost distinct public identity: child=%+v grandchild=%+v", childRef, grandchildRef)
					}
					childNodeAt := strings.Index(contents, `  Message ID: "child"`)
					nestedNodeAt := strings.Index(contents, `  Message ID: "nested"`)
					grandchildNodeAt := strings.Index(contents, `    Message ID: "grandchild"`)
					if !strings.Contains(contents, "    Upper message ID: \"nested\"\n") {
						t.Fatalf("nested material lost its actual native parent relation: %q", contents)
					}
					childResourceAt := strings.Index(contents, fmt.Sprintf(`  Resource: ID=%q kind=%q name=%q`, childRef.ID, childRef.Kind, childRef.Name))
					grandchildResourceAt := strings.Index(contents, fmt.Sprintf(`    Resource: ID=%q kind=%q name=%q`, grandchildRef.ID, grandchildRef.Kind, grandchildRef.Name))
					if childNodeAt < 0 || nestedNodeAt < 0 || grandchildNodeAt < 0 || childResourceAt < childNodeAt || childResourceAt > nestedNodeAt || grandchildResourceAt < grandchildNodeAt || strings.Contains(contents, "Attachment:") {
						t.Fatalf("forwarded node/resource cache correspondence missing: %q", contents)
					}
					if !strings.Contains(contents, `Sender: sender_type="user" id_type="open_id" id="speaker-file-child"`) || !strings.Contains(contents, `Create time: "1793930000001"`) || !strings.Contains(contents, `Sender: sender_type="user" id_type="open_id" id="speaker-nested\ninjected"`) || strings.Contains(contents, "speaker-nested\ninjected") || !strings.Contains(contents, `Create time: "1793930000003"`) {
						t.Fatalf("forwarded node provenance was not safely rendered: %q", contents)
					}
				}
				if scenario == "system" && (!strings.Contains(contents, "Sender: unknown") || strings.Contains(contents, "Create time:")) {
					t.Fatalf("missing native sender/time fallback changed: %q", contents)
				}
				if scenario == "rich-post" {
					for _, want := range []string{
						"Classic title", "before[site](https://example.test/a?x=1)[Mention: user_id=\"ou_123\" name=\"Ada\"]",
						"[Code block language=\"go\"]", "fmt.Println(1)\n  \t// exact", "[/Code block]",
						"**original markdown**", "---", ":SMILE:",
					} {
						if !strings.Contains(contents, want) {
							t.Fatalf("rich post lost %q: %q", want, contents)
						}
					}
					quote := findQuotedElement(event.Message.Elements, "parent")
					if quote == nil || len(quote.Children) < 2 || quote.Children[1].Type != uvim.ElementText || !strings.Contains(quote.Children[1].Text, "[Code block language=") || !strings.Contains(quote.Children[1].Text, "  fmt.Println(1)\n\t// exact") {
						t.Fatalf("root quote element lost rich post body: %+v", event.Message.Elements)
					}
				}
				if scenario == "rich-post-v2-only" {
					if !strings.Contains(contents, "v2 only") || !strings.Contains(contents, "top-file.txt") || !strings.Contains(contents, "ZIP archive") || strings.Contains(contents, "top-folder") {
						t.Fatalf("direct v2/top-file post was not safely preserved: %q", contents)
					}
				}
				if scenario == "rich-post-fallback" && !strings.Contains(contents, "legacy fallback") {
					t.Fatalf("empty content_v2 did not fall back: %q", contents)
				}
				if scenario == "rich-post-markers" {
					for _, want := range []string{"Post locale: \"zh_cn\"", "knownraw unknown", "[Unsupported post element: \"mystery\"]", "[Post element unavailable]", "[Post paragraph unavailable]"} {
						if !strings.Contains(contents, want) {
							t.Fatalf("rich post diagnostic/material missing %q: %q", want, contents)
						}
					}
				}
				if scenario == "merge-forward" {
					if !strings.Contains(contents, "Post locale: \"zh_cn\"") || !strings.Contains(contents, "selected zh") || !strings.Contains(contents, "v2 nested body") || strings.Contains(contents, "legacy text") || strings.Contains(contents, "wrong locale body") || strings.Count(contents, "post media") != 1 {
						t.Fatalf("nested localized/v2 post was not rendered and deduplicated: %q", contents)
					}
					postChild := findQuotedElement(event.Message.Elements, "post-child")
					if postChild == nil || len(postChild.Children) == 0 || postChild.Children[0].Type != uvim.ElementText || !strings.Contains(postChild.Children[0].Text, "Post locale: \"zh_cn\"") {
						t.Fatalf("nested quote element lost selected post body: %+v", event.Message.Elements)
					}
				}
			default:
				if len(failures) == 0 || strings.Contains(contents, "fatal:") || strings.Contains(strings.Join(failures, ""), "private diagnostic") {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				want := map[string]string{"denied": "quoted_message_lookup_failed: code=99991672", "deleted": "quoted_message_deleted", "wrong-chat": "quoted_message_identity_mismatch", "wrong-message": "quoted_message_not_found", "download-failed": "download_failed", "folder-failed": "download_failed", "rich-post-unavailable": "quoted_message_content_unavailable"}[scenario]
				if len(failures) != 1 || failures[0] != want {
					t.Fatalf("failure=%v want=%q", failures, want)
				}
				if scenario == "denied" || scenario == "deleted" || scenario == "wrong-chat" || scenario == "wrong-message" {
					quote := findQuotedElement(event.Message.Elements, "parent")
					if quote == nil || len(quote.Children) != 1 || quote.Children[0].Resource == nil || quote.Children[0].Resource.Error != want || quote.Children[0].Resource.InternalURL != "" || quote.Children[0].Resource.Key != "" || quote.Children[0].Resource.Metadata != nil {
						t.Fatalf("quoted error resource missing from hierarchy: %+v", event.Message.Elements)
					}
				}
			}
		})
	}
}

func findQuotedElement(elements []uvim.Element, id string) *uvim.Element {
	for i := range elements {
		if elements[i].Type == uvim.ElementQuote && elements[i].ID == id {
			return &elements[i]
		}
		if found := findQuotedElement(elements[i].Children, id); found != nil {
			return found
		}
	}
	return nil
}
