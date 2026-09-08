package uvim

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
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
	if opened.ID != "r1" || opened.Name != "hello.txt" || !strings.HasPrefix(opened.MIME, "text/plain") {
		t.Fatalf("opened = %+v", opened)
	}
	safe := ref.Sanitized()
	if safe.URL != "" || safe.Secret != "" || safe.Private != nil {
		t.Fatalf("sanitized = %+v", safe)
	}
}

func TestResourceStoreOpensLegacyFiles(t *testing.T) {
	for _, name := range []string{"r1-01-hello.txt", filepath.Join(resourceDirectory("r1"), "hello.txt")} {
		t.Run(name, func(t *testing.T) {
			store := &ResourceStore{Dir: t.TempDir()}
			path := filepath.Join(store.Dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
				t.Fatal(err)
			}
			file, ref, err := store.Open("internal://r1")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			data, err := io.ReadAll(file)
			if err != nil || string(data) != "hello" || ref.ID != "r1" || ref.SizeBytes != 5 {
				t.Fatalf("legacy resource: ref=%+v data=%q err=%v", ref, data, err)
			}
		})
	}
}

func TestResourceStoreCleansFailedSave(t *testing.T) {
	for _, stage := range []string{"copy", "metadata"} {
		t.Run(stage, func(t *testing.T) {
			store := &ResourceStore{Dir: t.TempDir()}
			dir := filepath.Join(store.Dir, resourceDirectory("r1"), ".resource")
			var src io.Reader = io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(errors.New("read failed")))
			if stage == "metadata" {
				// An existing directory prevents the metadata-file rename.
				if err := os.MkdirAll(filepath.Join(dir, "metadata.json"), 0o700); err != nil {
					t.Fatal(err)
				}
				src = strings.NewReader("complete")
			}
			if _, err := store.Save(context.Background(), src, ResourceRef{ID: "r1", Name: "report"}); err == nil {
				t.Fatal("expected save failure")
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					t.Fatalf("failed save left file %q", entry.Name())
				}
			}
			if file, _, err := store.Open("internal://r1"); err == nil {
				file.Close()
				t.Fatal("failed save exposed a resource")
			}
		})
	}
}

func TestResourceUploadNameSanitizesPathsAndHeaderBytes(t *testing.T) {
	got := ResourceUploadName(0, ResourceRef{Name: "../report\r\nInjected: yes.pdf"}, "application/pdf")
	if got != "report-Injected-yes.pdf" {
		t.Fatalf("ResourceUploadName() = %q", got)
	}
}
