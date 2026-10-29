package main

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
)

func TestLoadUserIdentityMappings(t *testing.T) {
	if mapper, err := loadUserIdentityMapper(""); mapper != nil || err != nil {
		t.Fatalf("optional mapping: %v %v", mapper, err)
	}
	for _, tt := range []struct {
		name, raw string
		valid     bool
	}{
		{"empty", `[]`, true},
		{"verified binding", `[{"provider":"wecom","connector":"main","user_id":"native","identities":[{"type":"wecom.userid","id":"directory","scope":"corp"}]}]`, true},
		{"null", `null`, false}, {"object", `{}`, false}, {"trailing value", `[] []`, false},
		{"unknown selector", `[{"provider":"wecom","connector":"main","user_name":"Alice"}]`, false},
		{"missing selector", `[{"provider":"wecom","user_id":"native","identities":[{"type":"employee","id":"id"}]}]`, false},
		{"null identity", `[{"provider":"wecom","connector":"main","user_id":"native","identities":[null]}]`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "identities.json")
			if err := os.WriteFile(path, []byte(tt.raw), 0600); err != nil {
				t.Fatal(err)
			}
			mapper, err := loadUserIdentityMapper(path)
			if (err == nil) != tt.valid {
				t.Fatalf("load valid=%v err=%v", tt.valid, err)
			}
			if tt.name == "verified binding" {
				event := mapper.Apply(uvim.Event{Provider: "wecom", Connector: "main", User: uvim.User{ID: "native"}})
				if len(event.User.Identities) != 1 || event.User.Identities[0].ID != "directory" || event.User.Identities[0].Source != uvim.IdentitySourceConfigured {
					t.Fatalf("user=%+v", event.User)
				}
			}
		})
	}
}

func TestAutoProvidersDetectsDingTalkStreamCredentials(t *testing.T) {
	t.Setenv("UV_DINGTALK_CLIENT_ID", "client-id")
	t.Setenv("UV_DINGTALK_CLIENT_SECRET", "client-secret")
	if providers := autoProviders(); !slices.Contains(providers, "dingtalk") {
		t.Fatalf("autoProviders() = %v, want dingtalk", providers)
	}
}

func TestBuildProvidersRejectsPartialDingTalkStreamCredentials(t *testing.T) {
	t.Setenv("UV_DINGTALK_CLIENT_ID", "client-id")
	t.Setenv("UV_DINGTALK_CLIENT_SECRET", "")
	if _, err := buildProviders("dingtalk", t.TempDir()); err == nil {
		t.Fatal("buildProviders() accepted partial DingTalk Stream credentials")
	}
}

func TestEnvNameMapParsesJSON(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `{"u1":"张三"," u2 ":" 李四 "}`)
	got, err := envNameMap("UV_TEST_NAMES")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"u1": "张三", "u2": "李四"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("envNameMap() = %#v, want %#v", got, want)
	}
}

func TestEnvNameMapRejectsMalformedJSON(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `{not-json}`)
	if _, err := envNameMap("UV_TEST_NAMES"); err == nil {
		t.Fatal("envNameMap() error = nil")
	}
}

func TestEnvNameMapRejectsJSONNull(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `null`)
	if _, err := envNameMap("UV_TEST_NAMES"); err == nil {
		t.Fatal("envNameMap() accepted JSON null")
	}
}

func TestBuildProvidersRejectsJSONNullNameMapMember(t *testing.T) {
	t.Setenv("UV_WECOM_USER_NAMES", `{"u1":null}`)
	if _, err := buildProviders("wecom", t.TempDir()); err == nil {
		t.Fatal("buildProviders() accepted a JSON null name-map member")
	}
}

func TestBuildProvidersRequiresCompleteWeComApplicationCredentials(t *testing.T) {
	t.Setenv("UV_WECOM_BOT_ID", "bot")
	t.Setenv("UV_WECOM_BOT_SECRET", "secret")
	t.Setenv("UV_WECOM_CORP_ID", "corp")
	t.Setenv("UV_WECOM_CORP_SECRET", "")
	if _, err := buildProviders("wecom", t.TempDir()); err == nil {
		t.Fatal("partial WeCom application credentials accepted")
	}
	t.Setenv("UV_WECOM_CORP_SECRET", "app")
	if _, err := buildProviders("wecom", t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
