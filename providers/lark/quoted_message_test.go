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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/server"
)

// Runs the real WebSocket adapter and Hub with a local Feishu API stub.
func TestQuotedMessageResourcesThroughLarkTransportStub(t *testing.T) {
	for _, scenario := range []string{"auto-file", "auto-other", "file", "folder", "folder-failed", "text", "interactive", "share-chat", "share-user", "system", "merge-forward", "rich-post", "rich-post-v2-only", "rich-post-fallback", "rich-post-markers", "rich-post-unavailable", "reply-chain", "reply-chain-no-edge", "reply-chain-cycle", "reply-chain-wrong-chat", "reply-chain-deleted", "reply-chain-denied", "reply-chain-depth", "reply-chain-budget", "denied", "deleted", "wrong-chat", "wrong-message", "download-failed", "unaddressed"} {
		t.Run(scenario, func(t *testing.T) {
			var api *httptest.Server
			var ancestorLookups int
			largeAncestorBody := `{"text":"` + strings.Repeat("x", 25*1024) + `"}`
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
					threadID := ""
					if strings.HasPrefix(scenario, "reply-chain") {
						threadID = "callback-topic"
					}
					raw, _ := json.Marshal(map[string]any{"header": map[string]any{"event_type": "im.message.receive_v1"}, "event": map[string]any{"message": map[string]any{
						"message_id": "current", "parent_id": "parent", "root_id": "unrelated-root", "thread_id": threadID, "chat_id": "chat", "chat_type": "group", "message_type": "text", "content": `{"text":"analyze this log"}`, "mentions": mentions,
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
					if strings.HasPrefix(scenario, "reply-chain") && scenario != "reply-chain-denied" {
						kind, content = "text", `{"text":"selected direct reply"}`
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
					if strings.HasPrefix(scenario, "reply-chain") {
						items[0].(map[string]any)["thread_id"] = "selected-topic"
						items[0].(map[string]any)["create_time"] = "1793930000000"
						switch scenario {
						case "reply-chain-no-edge":
							items[0].(map[string]any)["root_id"] = "earlier-reply"
							items[0].(map[string]any)["upper_message_id"] = "earlier-reply"
						case "reply-chain-depth":
							items[0].(map[string]any)["parent_id"] = "ancestor-1"
						case "reply-chain-budget":
							items[0].(map[string]any)["parent_id"] = "ancestor-budget"
						default:
							items[0].(map[string]any)["parent_id"] = "earlier-reply"
						}
					}
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
				case "/open-apis/im/v1/messages/earlier-reply":
					ancestorLookups++
					if scenario == "reply-chain-denied" {
						json.NewEncoder(w).Encode(map[string]any{"code": 99991672, "msg": "private ancestor diagnostic"})
						return
					}
					chat, parentID := "chat", "earlier-origin"
					if scenario == "reply-chain-wrong-chat" {
						chat = "other-chat"
					}
					if scenario == "reply-chain-cycle" {
						parentID = "parent"
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
						"message_id": "earlier-reply", "parent_id": parentID, "chat_id": chat, "thread_id": "selected-topic", "msg_type": "text", "deleted": scenario == "reply-chain-deleted", "sender": map[string]any{"id": "ou-reply", "id_type": "open_id", "sender_type": "user"}, "create_time": "1793929999000", "body": map[string]any{"content": `{"text":"earlier reply condition"}`},
					}}}})
				case "/open-apis/im/v1/messages/earlier-origin":
					ancestorLookups++
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
						"message_id": "earlier-origin", "chat_id": "chat", "thread_id": "selected-topic", "msg_type": "file", "sender": map[string]any{"id": "app", "id_type": "app_id", "sender_type": "app"}, "create_time": "1793929998000", "body": map[string]any{"content": `{"file_key":"ancestor-file-key","file_name":"earlier-evidence.bin"}`},
					}}}})
				case "/open-apis/im/v1/messages/earlier-origin/resources/ancestor-file-key":
					t.Error("downloaded an ancestor descriptor")
					w.WriteHeader(http.StatusInternalServerError)
				case "/open-apis/im/v1/messages/ancestor-1", "/open-apis/im/v1/messages/ancestor-2", "/open-apis/im/v1/messages/ancestor-3", "/open-apis/im/v1/messages/ancestor-4":
					ancestorLookups++
					id := strings.TrimPrefix(req.URL.Path, "/open-apis/im/v1/messages/")
					parentID := fmt.Sprintf("ancestor-%c", id[len(id)-1]+1)
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
						"message_id": id, "parent_id": parentID, "chat_id": "chat", "thread_id": "selected-topic", "msg_type": "text", "body": map[string]any{"content": `{"text":"depth fact"}`},
					}}}})
				case "/open-apis/im/v1/messages/ancestor-budget":
					ancestorLookups++
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
						"message_id": "ancestor-budget", "chat_id": "chat", "thread_id": "selected-topic", "msg_type": "text", "body": map[string]any{"content": largeAncestorBody},
					}}}})
				case "/open-apis/im/v1/messages/parent/resources/log-key":
					if scenario != "auto-file" && scenario != "file" && scenario != "folder" && scenario != "folder-failed" && scenario != "download-failed" && scenario != "reply-chain-denied" {
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
			if strings.HasPrefix(scenario, "reply-chain") && event.Referrer.ThreadID != "callback-topic" {
				t.Fatalf("callback thread changed: %+v", event.Referrer)
			}
			if scenario == "unaddressed" || scenario == "auto-other" {
				if ancestorLookups != 0 {
					t.Fatalf("unaddressed event made ancestor reads=%d", ancestorLookups)
				}
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
			quote := findQuotedElement(event.Message.Elements, "parent")
			if quote == nil || len(quote.Children) == 0 || quote.Children[0].Resource == nil {
				t.Fatalf("quoted context resource missing from quote hierarchy: %+v", event.Message.Elements)
			}
			contextResourceID := quote.Children[0].Resource.ID
			contextInternalURL := quote.Children[0].Resource.InternalURL
			wantResources := 1
			if scenario == "reply-chain-denied" {
				wantResources = 2
			}
			if strings.HasPrefix(scenario, "reply-chain") && len(event.Message.Resources) != wantResources {
				t.Fatalf("ancestor resources must remain descriptors: %+v", event.Message.Resources)
			}
			if !strings.HasPrefix(scenario, "reply-chain") && ancestorLookups != 0 {
				t.Fatalf("event without a native parent edge made ancestor reads=%d", ancestorLookups)
			}
			var contents string
			var failures []string
			var contextRef *uvim.ResourceRef
			for _, ref := range event.Message.Resources {
				isContextRef := ref.ID == contextResourceID && ref.InternalURL == contextInternalURL
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
				if isContextRef {
					ref := ref
					contextRef = &ref
				} else if len(ref.ReferencedMessageIDs) != 0 {
					t.Fatalf("only quote context may carry ancestor lineage: %+v", ref)
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
			if strings.HasPrefix(scenario, "reply-chain") {
				if contextRef == nil {
					t.Fatal("quoted context resource missing")
				}
				wantAcceptedIDs := map[string][]string{
					"reply-chain":       {"earlier-reply", "earlier-origin"},
					"reply-chain-cycle": {"earlier-reply"},
					"reply-chain-depth": {"ancestor-1", "ancestor-2", "ancestor-3", "ancestor-4"},
				}[scenario]
				if !slices.Equal(contextRef.ReferencedMessageIDs, wantAcceptedIDs) {
					t.Fatalf("accepted ancestor IDs = %v, want %v", contextRef.ReferencedMessageIDs, wantAcceptedIDs)
				}
				if len(failures) != 0 || !strings.Contains(contents, `Message ID: "parent"`) {
					t.Fatalf("selected quote was not retained: contents=%q failures=%v", contents, failures)
				}
				if scenario == "reply-chain-denied" {
					if !strings.Contains(contents, "device.log") || !strings.Contains(contents, "fatal: tracking failed") {
						t.Fatalf("selected resource was not retained after ancestor failure: %q", contents)
					}
				} else if !strings.Contains(contents, "selected direct reply") {
					t.Fatalf("selected quote body was not retained: %q", contents)
				}
				wantLookups := map[string]int{
					"reply-chain":            2,
					"reply-chain-no-edge":    0,
					"reply-chain-cycle":      1,
					"reply-chain-wrong-chat": 1,
					"reply-chain-deleted":    1,
					"reply-chain-denied":     1,
					"reply-chain-depth":      4,
					"reply-chain-budget":     1,
				}[scenario]
				if ancestorLookups != wantLookups {
					t.Fatalf("ancestor lookups=%d, want %d", ancestorLookups, wantLookups)
				}
				wantStatus := map[string]string{
					"reply-chain-cycle":      "quoted_message_ancestor_cycle",
					"reply-chain-wrong-chat": "quoted_message_identity_mismatch",
					"reply-chain-deleted":    "quoted_message_deleted",
					"reply-chain-denied":     "quoted_message_lookup_failed: code=99991672",
					"reply-chain-depth":      "quoted_message_ancestor_depth_limited",
					"reply-chain-budget":     "quoted_message_ancestor_budget_limited",
				}[scenario]
				if wantStatus != "" && !strings.Contains(contents, wantStatus) {
					t.Fatalf("reply chain missing status %q: %q", wantStatus, contents)
				}
				switch scenario {
				case "reply-chain":
					for _, want := range []string{`Message ID: "earlier-reply"`, `Parent message ID: "earlier-origin"`, "earlier reply condition", `Message ID: "earlier-origin"`, `Media description: kind="file" name="earlier-evidence.bin"`} {
						if !strings.Contains(contents, want) {
							t.Fatalf("reply chain lost %q: %q", want, contents)
						}
					}
				case "reply-chain-no-edge":
					if !strings.Contains(contents, `Upper message ID: "earlier-reply"`) || strings.Contains(contents, "Earlier replies") {
						t.Fatalf("non-parent IDs created a reply edge: %q", contents)
					}
				case "reply-chain-depth":
					if !strings.Contains(contents, `Message ID: "ancestor-4"`) || strings.Contains(contents, `Message ID: "ancestor-5"`) {
						t.Fatalf("depth boundary did not keep only fetched complete nodes: %q", contents)
					}
				case "reply-chain-budget":
					if strings.Contains(contents, strings.Repeat("x", 128)) {
						t.Fatalf("budget boundary appended a partial ancestor node: %q", contents)
					}
				}
				if scenario == "reply-chain-no-edge" {
					cancelled, cancel := context.WithCancel(t.Context())
					cancel()
					expired, expire := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					expire()
					for _, tc := range []struct {
						name, want string
						ctx        context.Context
					}{
						{name: "cancelled", want: "quoted_message_ancestor_cancelled", ctx: cancelled},
						{name: "timed out", want: "quoted_message_ancestor_timeout", ctx: expired},
					} {
						text, ids := provider.quotedMessageAncestors(tc.ctx, "token", quotedMessageItem{ID: "parent", ParentID: "never-read"}, "current", "chat")
						if !strings.Contains(text, tc.want) || len(ids) != 0 || ancestorLookups != 0 {
							t.Fatalf("%s ancestor context result=%q ids=%v reads=%d", tc.name, text, ids, ancestorLookups)
						}
					}
				}
				return
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

func TestReadConversationWindowUsesVerifiedNativeThread(t *testing.T) {
	for _, tc := range []struct {
		name     string
		overflow bool
	}{
		{name: "interactive card is excluded"},
		{name: "overflow timestamp is excluded", overflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var triggerCalls, listCalls int
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/im/v1/messages/current":
					triggerCalls++
					if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer token" {
						t.Errorf("trigger request method=%s authorization=%q", req.Method, req.Header.Get("Authorization"))
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": 0,
						"data": map[string]any{
							"items": []any{map[string]any{
								"message_id": "current", "chat_id": "group", "thread_id": "topic", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"current"}`},
							}},
						},
					})
				case "/open-apis/im/v1/messages":
					listCalls++
					query := req.URL.Query()
					if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer token" || query.Get("container_id_type") != "thread" || query.Get("container_id") != "topic" || query.Get("page_size") != "10" || query.Get("sort_type") != "ByCreateTimeDesc" || query.Get("end_time") != "" || query.Get("page_token") != "" {
						t.Errorf("list request method=%s authorization=%q query=%v", req.Method, req.Header.Get("Authorization"), query)
					}
					item := func(id, threadID, createTime, messageType, content string) map[string]any {
						return map[string]any{"message_id": id, "chat_id": "group", "thread_id": threadID, "create_time": createTime, "msg_type": messageType, "sender": map[string]any{"id": "ou-" + id, "id_type": "open_id", "sender_type": "user"}, "body": map[string]any{"content": content}}
					}
					items := []any{
						item("current", "topic", "10001", "text", `{"text":"trigger duplicate"}`),
						item("future", "topic", "10002", "text", `{"text":"future"}`),
						item("sibling", "other-topic", "10000", "text", `{"text":"sibling secret"}`),
						item("parent", "topic", "9999", "text", `{"text":"quoted parent"}`),
						item("duplicate", "topic", "9998", "text", `{"text":"duplicate one"}`),
						item("duplicate", "topic", "9997", "text", `{"text":"duplicate two"}`),
						item("deleted", "topic", "9996", "text", `{"text":"deleted"}`),
						item("card", "topic", "9995", "interactive", `{"elements":[]}`),
						item("older-text", "topic", "9994", "text", `{"text":"older text"}`),
						item("older-post", "topic", "9993", "post", `{"zh_cn":{"content":[[{"tag":"text","text":"older post"},{"tag":"img","image_key":"image-key"}]]}}`),
					}
					items[6].(map[string]any)["deleted"] = true
					if tc.overflow {
						items[7] = item("overflow", "topic", "18446744073709561615", "text", `{"text":"overflow timestamp leaked"}`)
					} else {
						items[7].(map[string]any)["sender"] = map[string]any{"id": "app", "id_type": "app_id", "sender_type": "app"}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"has_more": true, "items": items}})
				default:
					t.Errorf("unexpected native window request %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()

			provider, err := New(Config{ConnectorID: "main", AppID: "app", AppSecret: "secret", BaseURL: api.URL})
			if err != nil {
				t.Fatal(err)
			}
			triggerTime := time.UnixMilli(10001).UTC()
			result := provider.ReadConversationWindow(t.Context(), uvim.Event{
				Provider:  "lark",
				Connector: "main",
				Login:     uvim.Login{Platform: "lark", Connector: "main", ID: "app"},
				Channel:   uvim.Channel{ID: "group", Type: uvim.ChannelGroup},
				Message:   uvim.Message{ID: "current", CreatedAt: triggerTime},
				// Older retained callbacks did not have a thread_id. The native GET is
				// authoritative and must recover it before choosing the list container.
				Referrer: uvim.Referrer{MessageID: "current", ParentMessageID: "parent", ChannelID: "group"},
			})
			if triggerCalls != 1 || listCalls != 1 {
				t.Fatalf("native calls trigger=%d list=%d, want one each", triggerCalls, listCalls)
			}
			if result.Version != uvim.ConversationWindowVersionV1 || result.Source != "native" || result.Status != "scan_limited" || result.Reason != "window_scan_limited" || result.ConversationID != "group" || result.ThreadID != "topic" || result.TriggerMessageID != "current" || !result.AnchorTime.Equal(triggerTime) || result.CandidateCount != 10 || result.BoundaryOmitted != 2 || !result.HasMore {
				t.Fatalf("window result=%+v", result)
			}
			if len(result.Items) != 2 || result.Items[0].MessageID != "older-post" || result.Items[1].MessageID != "older-text" || result.Items[0].Text != "Post locale: \"zh_cn\"\nolder post[Image]" || len(result.Items[0].Resources) != 1 || result.Items[0].Resources[0].Kind != uvim.ElementImage || result.Items[0].Resources[0].Name != "" {
				t.Fatalf("filtered native window items=%+v", result.Items)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"image-key", "token", "sibling secret", "quoted parent", "duplicate one", "deleted", "overflow timestamp leaked"} {
				if strings.Contains(string(encoded), forbidden) {
					t.Fatalf("native window leaked %q: %s", forbidden, encoded)
				}
			}
			wrongBot := provider.ReadConversationWindow(t.Context(), uvim.Event{
				Provider: "lark", Connector: "main", Login: uvim.Login{Platform: "lark", Connector: "main", ID: "other-app"},
				Channel: uvim.Channel{ID: "group", Type: uvim.ChannelGroup}, Message: uvim.Message{ID: "current", CreatedAt: triggerTime},
				Referrer: uvim.Referrer{MessageID: "current", ParentMessageID: "parent", ThreadID: "topic", ChannelID: "group"},
			})
			if wrongBot.Status != "unavailable" || wrongBot.Reason != "native_unavailable" || triggerCalls != 1 || listCalls != 1 {
				t.Fatalf("wrong current bot should make no native call: result=%+v trigger=%d list=%d", wrongBot, triggerCalls, listCalls)
			}
			mismatchedThread := provider.ReadConversationWindow(t.Context(), uvim.Event{
				Provider: "lark", Connector: "main", Login: uvim.Login{Platform: "lark", Connector: "main", ID: "app"},
				Channel: uvim.Channel{ID: "group", Type: uvim.ChannelGroup}, Message: uvim.Message{ID: "current", CreatedAt: triggerTime},
				Referrer: uvim.Referrer{MessageID: "current", ParentMessageID: "parent", ThreadID: "other-topic", ChannelID: "group"},
			})
			if mismatchedThread.Status != "unavailable" || mismatchedThread.Reason != "native_unavailable" || triggerCalls != 2 || listCalls != 1 {
				t.Fatalf("mismatched native thread should fail closed before list: result=%+v trigger=%d list=%d", mismatchedThread, triggerCalls, listCalls)
			}
		})
	}
}

func TestNativeWindowFailureNormalizesKnownCodes(t *testing.T) {
	base := uvim.ConversationWindowResult{Version: uvim.ConversationWindowVersionV1, Source: "native"}
	for _, tc := range []struct {
		name, status, reason string
		code                 int
	}{
		{name: "permission", code: 99991672, status: "permission_denied", reason: "native_permission_denied"},
		{name: "rate", code: 230020, status: "rate_limited", reason: "native_rate_limited"},
		{name: "unknown", code: 12345, status: "unavailable", reason: "native_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nativeWindowUnavailable(base, tc.code, nil)
			if got.Status != tc.status || got.Reason != tc.reason {
				t.Fatalf("normalized result=%+v", got)
			}
		})
	}
}

func TestReadConversationSearchPageUsesVerifiedScopeAndPrivateContinuation(t *testing.T) {
	var lists int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages/current":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "current", "chat_id": "group", "thread_id": "topic", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"current"}`}}}}})
		case "/open-apis/im/v1/messages":
			lists++
			query := req.URL.Query()
			if req.Header.Get("Authorization") != "Bearer token" || query.Get("container_id_type") != "thread" || query.Get("container_id") != "topic" || query.Get("page_size") != "2" || query.Get("sort_type") != "ByCreateTimeDesc" {
				t.Errorf("native search query=%v authorization=%q", query, req.Header.Get("Authorization"))
			}
			if lists == 1 {
				if query.Get("page_token") != "" {
					t.Errorf("first page token=%q", query.Get("page_token"))
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"has_more": true, "page_token": "native-next-secret", "items": []any{
					map[string]any{"message_id": "current", "chat_id": "group", "thread_id": "topic", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"trigger"}`}},
					map[string]any{"message_id": "older", "chat_id": "group", "thread_id": "topic", "create_time": "10000", "msg_type": "text", "sender": map[string]any{"id": "ou-older", "id_type": "open_id", "sender_type": "user"}, "body": map[string]any{"content": `{"text":"needle fact"}`}},
				}}})
				return
			}
			if lists == 2 {
				if query.Get("page_token") != "native-next-secret" {
					t.Errorf("second page token=%q", query.Get("page_token"))
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "oldest", "chat_id": "group", "thread_id": "topic", "create_time": "9999", "msg_type": "text", "body": map[string]any{"content": `{"text":"older fact"}`}}}}})
				return
			}
			if lists == 3 {
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"has_more": true, "items": []any{map[string]any{"message_id": "malformed", "chat_id": "group", "thread_id": "topic", "create_time": "9998", "msg_type": "text", "body": map[string]any{"content": `{"text":"must not publish"}`}}}}})
				return
			}
			t.Errorf("unexpected list request %d", lists)
		default:
			t.Errorf("unexpected native search request %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	provider, err := New(Config{ConnectorID: "main", AppID: "app", AppSecret: "secret", BaseURL: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.UnixMilli(10001).UTC()
	trigger := uvim.Event{Provider: "lark", Connector: "main", Login: uvim.Login{Platform: "lark", Connector: "main", ID: "app"}, Channel: uvim.Channel{ID: "group", Type: uvim.ChannelGroup}, Message: uvim.Message{ID: "current", CreatedAt: anchor}, Referrer: uvim.Referrer{MessageID: "current", ChannelID: "group"}}
	first := provider.ReadConversationSearchPage(t.Context(), uvim.ConversationSearchPageRequest{Trigger: trigger, Limit: 2})
	if first.Status != "available" || first.Scope.ThreadID != "topic" || first.Scope.ConversationID != "group" || !first.HasMore || first.NextPageToken != "native-next-secret" || first.CandidateCount != 2 || first.BoundaryOmitted != 1 || len(first.Candidates) != 1 || first.Candidates[0].Message.MessageID != "older" || !first.Candidates[0].Readable {
		t.Fatalf("first search page=%+v", first)
	}
	second := provider.ReadConversationSearchPage(t.Context(), uvim.ConversationSearchPageRequest{Trigger: trigger, Limit: 2, PageToken: first.NextPageToken, ExpectedScope: &first.Scope})
	if second.Status != "available" || second.HasMore || second.NextPageToken != "" || len(second.Candidates) != 1 || second.Candidates[0].Message.MessageID != "oldest" || lists != 2 {
		t.Fatalf("second search page=%+v lists=%d", second, lists)
	}
	malformed := provider.ReadConversationSearchPage(t.Context(), uvim.ConversationSearchPageRequest{Trigger: trigger, Limit: 2, PageToken: "unexpected", ExpectedScope: &first.Scope})
	if malformed.Status != "unavailable" || malformed.Reason != "native_unavailable" || len(malformed.Candidates) != 0 || malformed.NextPageToken != "" || lists != 3 {
		t.Fatalf("malformed continuation page=%+v lists=%d", malformed, lists)
	}
}

func TestReadConversationWindowUsesCeilingBoundaryForChat(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages/current":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
				"message_id": "current", "chat_id": "group", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"current"}`},
			}}}})
		case "/open-apis/im/v1/messages":
			query := req.URL.Query()
			if query.Get("container_id_type") != "chat" || query.Get("container_id") != "group" || query.Get("page_size") != "10" || query.Get("sort_type") != "ByCreateTimeDesc" || query.Get("end_time") != "11" || query.Get("page_token") != "" {
				t.Errorf("chat list query=%v", query)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{
				map[string]any{"message_id": "current", "chat_id": "group", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"equal"}`}},
				map[string]any{"message_id": "topic", "chat_id": "group", "thread_id": "other-topic", "create_time": "10000", "msg_type": "text", "body": map[string]any{"content": `{"text":"topic leak"}`}},
				map[string]any{"message_id": "older", "chat_id": "group", "create_time": "9999", "msg_type": "text", "body": map[string]any{"content": `{"text":"ordinary chat context"}`}},
			}}})
		default:
			t.Errorf("unexpected request %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()

	provider, err := New(Config{ConnectorID: "main", AppID: "app", AppSecret: "secret", BaseURL: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	triggerTime := time.UnixMilli(10001).UTC()
	result := provider.ReadConversationWindow(t.Context(), uvim.Event{
		Provider: "lark", Connector: "main", Login: uvim.Login{Platform: "lark", Connector: "main", ID: "app"},
		Channel: uvim.Channel{ID: "group", Type: uvim.ChannelGroup}, Message: uvim.Message{ID: "current", CreatedAt: triggerTime},
		Referrer: uvim.Referrer{MessageID: "current", ChannelID: "group"},
	})
	if result.Status != "available" || result.BoundaryOmitted != 1 || len(result.Items) != 1 || result.Items[0].MessageID != "older" || result.Items[0].Text != "ordinary chat context" {
		t.Fatalf("chat result=%+v", result)
	}
}

func TestReadConversationWindowRejectsOverfullNativePage(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages/current":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
				"message_id": "current", "chat_id": "group", "create_time": "10001", "msg_type": "text", "body": map[string]any{"content": `{"text":"current"}`},
			}}}})
		case "/open-apis/im/v1/messages":
			items := make([]any, nativeWindowPageSize+1)
			for i := range items {
				items[i] = map[string]any{"message_id": fmt.Sprintf("older-%d", i), "chat_id": "group", "create_time": "9999", "msg_type": "text", "body": map[string]any{"content": `{"text":"unexpected overfull page"}`}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items}})
		default:
			t.Errorf("unexpected request %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()

	provider, err := New(Config{ConnectorID: "main", AppID: "app", AppSecret: "secret", BaseURL: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	result := provider.ReadConversationWindow(t.Context(), uvim.Event{
		Provider: "lark", Connector: "main", Login: uvim.Login{Platform: "lark", Connector: "main", ID: "app"},
		Channel: uvim.Channel{ID: "group", Type: uvim.ChannelGroup}, Message: uvim.Message{ID: "current", CreatedAt: time.UnixMilli(10001).UTC()},
		Referrer: uvim.Referrer{MessageID: "current", ChannelID: "group"},
	})
	if result.Status != "unavailable" || result.Reason != "native_unavailable" || result.CandidateCount != 0 || len(result.Items) != 0 {
		t.Fatalf("overfull native page should be rejected: %+v", result)
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
