package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/dingtalk"
	"github.com/hengshi/uv-im-connector/providers/discord"
	"github.com/hengshi/uv-im-connector/providers/kook"
	"github.com/hengshi/uv-im-connector/providers/lark"
	"github.com/hengshi/uv-im-connector/providers/line"
	"github.com/hengshi/uv-im-connector/providers/mail"
	"github.com/hengshi/uv-im-connector/providers/matrix"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/onebot"
	"github.com/hengshi/uv-im-connector/providers/qq"
	"github.com/hengshi/uv-im-connector/providers/qqguild"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/telegram"
	"github.com/hengshi/uv-im-connector/providers/wechatofficial"
	"github.com/hengshi/uv-im-connector/providers/wecom"
	"github.com/hengshi/uv-im-connector/providers/whatsapp"
	"github.com/hengshi/uv-im-connector/providers/zulip"
	"github.com/hengshi/uv-im-connector/server"
)

func main() {
	addr := flag.String("addr", env("UV_IM_ADDR", "127.0.0.1:8787"), "listen address")
	stateDir := flag.String("state-dir", env("UV_IM_STATE_DIR", ".uv-im-connector"), "state directory")
	providerList := flag.String("providers", env("UV_IM_PROVIDERS", ""), "comma-separated provider list")
	flag.Parse()

	if err := run(*addr, *stateDir, *providerList); err != nil {
		log.Fatal(err)
	}
}

func run(addr, stateDir, providerList string) error {
	if stateDir == "" {
		return errors.New("state-dir is required")
	}
	providers, err := buildProviders(providerList, filepath.Join(stateDir, "resources"))
	if err != nil {
		return err
	}
	if len(providers) == 0 {
		return errors.New("no providers configured")
	}
	eventLog, err := uvim.NewEventLog(filepath.Join(stateDir, "events.jsonl"))
	if err != nil {
		return err
	}
	resources := &uvim.ResourceStore{Dir: filepath.Join(stateDir, "resources")}
	hub := server.NewHub(uvim.NewProviderRegistry(providers...), eventLog, resources)
	hub.SetAuthToken(os.Getenv("UV_IM_AUTH_TOKEN"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := hub.RunProviders(ctx); err != nil && ctx.Err() == nil {
			log.Printf("provider runner stopped: %v", err)
			stop()
		}
	}()
	httpServer := &http.Server{Addr: addr, Handler: hub.Handler()}
	go func() {
		<-ctx.Done()
		_ = httpServer.Shutdown(context.Background())
	}()
	log.Printf("uv-im-connector listening on http://%s", addr)
	err = httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func buildProviders(providerList string, resourceDir string) ([]uvim.Provider, error) {
	names := splitCSV(providerList)
	if len(names) == 0 {
		names = autoProviders()
	}
	var providers []uvim.Provider
	for _, name := range names {
		switch name {
		case "memory":
			providers = append(providers, memory.New("memory"))
		case "wecom":
			provider, err := wecom.New(wecom.Config{
				ConnectorID:   env("UV_WECOM_CONNECTOR_ID", "wecom"),
				BotID:         os.Getenv("UV_WECOM_BOT_ID"),
				Secret:        os.Getenv("UV_WECOM_BOT_SECRET"),
				WSURL:         os.Getenv("UV_WECOM_WS_URL"),
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "lark":
			provider, err := lark.New(lark.Config{
				ConnectorID:     env("UV_LARK_CONNECTOR_ID", "lark"),
				AppID:           os.Getenv("UV_LARK_APP_ID"),
				AppSecret:       os.Getenv("UV_LARK_APP_SECRET"),
				Region:          env("UV_LARK_REGION", lark.RegionFeishu),
				BotOpenID:       os.Getenv("UV_LARK_BOT_OPEN_ID"),
				BotUnionID:      os.Getenv("UV_LARK_BOT_UNION_ID"),
				BaseURL:         os.Getenv("UV_LARK_BASE_URL"),
				CallbackBaseURL: os.Getenv("UV_LARK_CALLBACK_BASE_URL"),
				ResourceStore:   &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "dingtalk":
			c := httpEnv("DINGTALK", "dingtalk")
			provider, err := dingtalk.New(dingtalk.Config(c))
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "discord":
			c := httpEnv("DISCORD", "discord")
			provider, err := discord.New(discord.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "kook":
			c := httpEnv("KOOK", "kook")
			provider, err := kook.New(kook.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "line":
			c := httpEnv("LINE", "line")
			provider, err := line.New(line.Config(c))
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "mail":
			provider, err := mail.New(mail.Config{
				ConnectorID:   env("UV_MAIL_CONNECTOR_ID", "mail"),
				SMTPAddr:      os.Getenv("UV_MAIL_SMTP_ADDR"),
				SMTPUsername:  os.Getenv("UV_MAIL_SMTP_USERNAME"),
				SMTPPassword:  os.Getenv("UV_MAIL_SMTP_PASSWORD"),
				From:          os.Getenv("UV_MAIL_FROM"),
				WebhookSecret: os.Getenv("UV_MAIL_WEBHOOK_SECRET"),
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "matrix":
			c := httpEnv("MATRIX", "matrix")
			provider, err := matrix.New(matrix.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "onebot":
			c := httpEnv("ONEBOT", "onebot")
			provider, err := onebot.New(onebot.Config(c))
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "qq":
			c := httpEnv("QQ", "qq")
			provider, err := qq.New(qq.Config(c))
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "qqguild", "qq-guild":
			c := httpEnv("QQGUILD", "qqguild")
			provider, err := qqguild.New(qqguild.Config(c))
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "slack":
			c := httpEnv("SLACK", "slack")
			provider, err := slack.New(slack.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "telegram":
			c := httpEnv("TELEGRAM", "telegram")
			provider, err := telegram.New(telegram.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "wechat-official", "wechatofficial":
			c := httpEnv("WECHAT_OFFICIAL", "wechat-official")
			provider, err := wechatofficial.New(wechatofficial.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "whatsapp":
			c := httpEnv("WHATSAPP", "whatsapp")
			provider, err := whatsapp.New(whatsapp.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				PhoneNumberID: os.Getenv("UV_WHATSAPP_PHONE_NUMBER_ID"),
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		case "zulip":
			c := httpEnv("ZULIP", "zulip")
			provider, err := zulip.New(zulip.Config{
				ConnectorID:   c.ConnectorID,
				BaseURL:       c.BaseURL,
				Token:         c.Token,
				WebhookSecret: c.WebhookSecret,
				ResourceStore: &uvim.ResourceStore{Dir: resourceDir},
			})
			if err != nil {
				return nil, err
			}
			providers = append(providers, provider)
		default:
			return nil, errors.New("unknown provider: " + name)
		}
	}
	return providers, nil
}

func autoProviders() []string {
	var out []string
	if os.Getenv("UV_WECOM_BOT_ID") != "" || os.Getenv("UV_WECOM_BOT_SECRET") != "" {
		out = append(out, "wecom")
	}
	if os.Getenv("UV_LARK_APP_ID") != "" || os.Getenv("UV_LARK_APP_SECRET") != "" {
		out = append(out, "lark")
	}
	for _, item := range []struct {
		name   string
		prefix string
	}{
		{"dingtalk", "DINGTALK"},
		{"discord", "DISCORD"},
		{"kook", "KOOK"},
		{"line", "LINE"},
		{"matrix", "MATRIX"},
		{"onebot", "ONEBOT"},
		{"qq", "QQ"},
		{"qqguild", "QQGUILD"},
		{"slack", "SLACK"},
		{"telegram", "TELEGRAM"},
		{"wechat-official", "WECHAT_OFFICIAL"},
		{"whatsapp", "WHATSAPP"},
		{"zulip", "ZULIP"},
	} {
		if hasHTTPProviderEnv(item.prefix) {
			out = append(out, item.name)
		}
	}
	if os.Getenv("UV_MAIL_SMTP_ADDR") != "" || os.Getenv("UV_MAIL_WEBHOOK_SECRET") != "" {
		out = append(out, "mail")
	}
	return out
}

type httpConfig struct {
	ConnectorID   string
	BaseURL       string
	Token         string
	WebhookSecret string
}

func httpEnv(prefix, connector string) httpConfig {
	return httpConfig{
		ConnectorID:   env("UV_"+prefix+"_CONNECTOR_ID", connector),
		BaseURL:       os.Getenv("UV_" + prefix + "_BASE_URL"),
		Token:         os.Getenv("UV_" + prefix + "_TOKEN"),
		WebhookSecret: os.Getenv("UV_" + prefix + "_WEBHOOK_SECRET"),
	}
}

func hasHTTPProviderEnv(prefix string) bool {
	for _, suffix := range []string{"TOKEN", "BASE_URL", "WEBHOOK_SECRET"} {
		if os.Getenv("UV_"+prefix+"_"+suffix) != "" {
			return true
		}
	}
	return false
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
