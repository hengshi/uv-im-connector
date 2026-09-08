package uvim

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestResourceStoreSavesBeyondLegacySizeLimits(t *testing.T) {
	data := strings.Repeat("x", 100*1024*1024+1)
	store := &ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), strings.NewReader(data), ResourceRef{Name: "large.bin"})
	if err != nil {
		t.Fatal(err)
	}
	file, _, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	size, err := io.Copy(io.Discard, file)
	file.Close()
	if err != nil || size != int64(len(data)) || ref.SizeBytes != size {
		t.Fatalf("stored size=%d metadata=%d err=%v", size, ref.SizeBytes, err)
	}
}

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

func TestResourceUploadNameSanitizesPathsAndHeaderBytes(t *testing.T) {
	got := ResourceUploadName(0, ResourceRef{Name: "../report\r\nInjected: yes.pdf"}, "application/pdf")
	if got != "report-Injected-yes.pdf" {
		t.Fatalf("ResourceUploadName() = %q", got)
	}
}
