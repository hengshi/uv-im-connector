package main

import (
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
