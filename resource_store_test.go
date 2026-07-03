package uvim

import (
	"context"
	"strings"
	"testing"
)

func TestResourceStoreSaveOpenAndSanitize(t *testing.T) {
	store := &ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), strings.NewReader("hello"), ResourceRef{ID: "r1", Kind: ElementFile, Name: "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if ref.InternalURL != "internal://r1" {
		t.Fatalf("InternalURL = %q", ref.InternalURL)
	}
	if ref.Metadata["path"] != "" {
		t.Fatalf("metadata leaked path: %+v", ref.Metadata)
	}
	file, opened, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if opened.ID != "r1" {
		t.Fatalf("opened = %+v", opened)
	}
	safe := ref.Sanitized()
	if safe.URL != "" || safe.Secret != "" || safe.Private != nil {
		t.Fatalf("sanitized = %+v", safe)
	}
}
