package main

import (
	"reflect"
	"slices"
	"testing"
)

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

func TestEnvStringMapParsesJSON(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `{"u1":"张三"," u2 ":" 李四 "}`)
	got, err := envStringMap("UV_TEST_NAMES")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"u1": "张三", "u2": "李四"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("envStringMap() = %#v, want %#v", got, want)
	}
}

func TestEnvStringMapRejectsMalformedJSON(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `{not-json}`)
	if _, err := envStringMap("UV_TEST_NAMES"); err == nil {
		t.Fatal("envStringMap() error = nil")
	}
}

func TestEnvStringMapRejectsJSONNull(t *testing.T) {
	t.Setenv("UV_TEST_NAMES", `null`)
	if _, err := envStringMap("UV_TEST_NAMES"); err == nil {
		t.Fatal("envStringMap() accepted JSON null")
	}
}

func TestBuildProvidersRejectsJSONNullMapMember(t *testing.T) {
	for _, key := range []string{"UV_WECOM_USER_NAMES", "UV_WECOM_CONVERSATION_NAMES", "UV_WECOM_USER_EXTERNAL_IDS", "UV_WECOM_CONVERSATION_EXTERNAL_IDS"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("UV_WECOM_BOT_ID", "bot")
			t.Setenv("UV_WECOM_BOT_SECRET", "secret")
			t.Setenv(key, `{"u1":null}`)
			if _, err := buildProviders("wecom", t.TempDir()); err == nil {
				t.Fatal("buildProviders() accepted a JSON null map member")
			}
		})
	}
}
