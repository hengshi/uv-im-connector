package uvim

import (
	"context"
	"errors"
	"io"
	"mime"
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
	ref, err := store.Save(context.Background(), strings.NewReader("hello"), ResourceRef{ID: "r1", Kind: ElementFile, Name: "客户反馈 ｜版本 é-📄.txt "})
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
	if opened.ID != "r1" || opened.Name != "客户反馈 ｜版本 é-📄.txt " || !strings.HasPrefix(opened.MIME, "text/plain") {
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

func TestResourceStoreWritesUnderTraversalOnlyParent(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "resources")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o100); err != nil {
		t.Skipf("cannot set traversal-only permissions: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })
	if dir, err := os.Open(parent); err == nil {
		dir.Close()
		t.Skip("parent can be opened despite traversal-only permissions")
	}
	store := &ResourceStore{Dir: root}
	if _, err := store.Save(t.Context(), strings.NewReader("bytes"), ResourceRef{Name: "test.txt"}); err != nil {
		t.Fatalf("save into writable root: %v", err)
	}
}

func TestResourceReplacementPinsMetadataAndBytes(t *testing.T) {
	store := &ResourceStore{Dir: t.TempDir()}
	dir := filepath.Join(store.Dir, resourceDirectory("r1"), ".resource")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Existing stores use the original fixed content and metadata pair.
	if err := os.WriteFile(filepath.Join(dir, "content"), []byte("text"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("{\"name\":\"old.txt\",\"mime\":\"text/plain\"}"), 0o600); err != nil {
		t.Fatal(err)
	}
	read := func() (*os.File, ResourceRef) {
		f, ref, err := store.Open("internal://r1")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f, ref
	}
	first, firstRef := read()
	save := func(name, mimeType, body string) {
		if _, err := (&ResourceStore{Dir: store.Dir}).Save(t.Context(), strings.NewReader(body), ResourceRef{ID: "r1", Name: name, MIME: mimeType}); err != nil {
			t.Fatal(err)
		}
	}
	save("new.pdf", "application/pdf", "pdf")
	second, secondRef := read()
	save("latest.zip", "application/zip", "zip")
	latest, latestRef := read()
	// A new generation must never alter the legacy pair in place.
	if legacy, err := os.ReadFile(filepath.Join(dir, "content")); err != nil || string(legacy) != "text" {
		t.Fatalf("fixed legacy content was changed: %q, %v", legacy, err)
	}
	current, err := readResourceMetadata(filepath.Join(dir, resourceCurrentMetadata))
	if err != nil || !validResourceContentName(current.Content) || current.Name != "latest.zip" {
		t.Fatalf("current resource metadata = %+v, %v", current, err)
	}
	if data, err := os.ReadFile(filepath.Join(dir, current.Content)); err != nil || string(data) != "zip" {
		t.Fatalf("metadata does not point at the committed bytes: %q, %v", data, err)
	}
	for _, got := range []struct {
		file             *os.File
		ref              ResourceRef
		name, mime, body string
	}{
		{first, firstRef, "old.txt", "text/plain", "text"},
		{second, secondRef, "new.pdf", "application/pdf", "pdf"},
		{latest, latestRef, "latest.zip", "application/zip", "zip"},
	} {
		body, err := io.ReadAll(got.file)
		if err != nil || got.ref.Name != got.name || got.ref.MIME != got.mime || string(body) != got.body {
			t.Fatalf("attachment generation name=%q MIME=%q bytes=%q error=%v", got.ref.Name, got.ref.MIME, body, err)
		}
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
				if err := os.MkdirAll(filepath.Join(dir, resourceCurrentMetadata), 0o700); err != nil {
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
	if got != "report-Injected: yes.pdf" {
		t.Fatalf("ResourceUploadName() = %q", got)
	}
	name := ResourceUploadName(0, ResourceRef{Name: `C:\work\客户反馈.pdf`}, "application/pdf")
	disposition := MultipartFileDisposition("file\r\n", name)
	_, params, err := mime.ParseMediaType(disposition)
	if err != nil || strings.ContainsAny(disposition, "\r\n") || params["filename"] != "客户反馈.pdf" {
		t.Fatalf("Content-Disposition = %q, err = %v", disposition, err)
	}
	if got := ResourceMediaExtension(ResourceRef{Name: "report.p\x00df"}, ""); got != ".pdf" {
		t.Fatalf("ResourceMediaExtension() = %q", got)
	}
}

func TestResourceUploadNameKeepsNamesWithoutSuffix(t *testing.T) {
	for _, name := range []string{"无扩展名｜报告", "   "} {
		if got := ResourceUploadName(0, ResourceRef{Name: name}, "application/pdf"); got != name {
			t.Fatalf("ResourceUploadName(%q) = %q", name, got)
		}
	}
	if got := ResourceUploadName(0, ResourceRef{Key: "upload token+v1"}, "application/pdf"); got != "upload-token-v1.pdf" {
		t.Fatalf("fallback ResourceUploadName() = %q", got)
	}
}
