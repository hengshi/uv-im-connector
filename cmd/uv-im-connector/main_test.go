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
