package uvim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type ResourceStore struct {
	Dir           string
	PublicBaseURL string
	HTTPClient    *http.Client
}

type resourceFileMetadata struct {
	Name string `json:"name"`
	MIME string `json:"mime,omitempty"`
}

func (s *ResourceStore) SaveHTTP(ctx context.Context, req *http.Request, ref ResourceRef) (ResourceRef, error) {
	if s == nil {
		return ref, fmt.Errorf("resource store is nil")
	}
	client := s.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := client.Do(req)
	if err != nil {
		return ref, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ref, fmt.Errorf("http %d", resp.StatusCode)
	}
	ref.MIME = FirstNonEmpty(ref.MIME, resp.Header.Get("Content-Type"))
	return s.Save(ctx, resp.Body, ref)
}

func (s *ResourceStore) Save(ctx context.Context, src io.Reader, ref ResourceRef) (ResourceRef, error) {
	if s == nil {
		return ref, fmt.Errorf("resource store is nil")
	}
	dir := s.Dir
	if dir == "" {
		return ref, fmt.Errorf("resource store dir is required")
	}
	id := FirstNonEmpty(ref.ID, NewID("res"))
	ref.ID = id
	// Names and inferred extensions are metadata, never filesystem components.
	// The reserved subdirectory cannot collide with old sanitized basenames.
	dir = filepath.Join(dir, resourceDirectory(id), ".resource")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ref, err
	}
	path := filepath.Join(dir, "content")
	metadataPath := filepath.Join(dir, "metadata.json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return ref, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(path)
			_ = os.Remove(metadataPath)
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), src)
	closeErr := file.Close()
	if copyErr != nil {
		return ref, copyErr
	}
	if closeErr != nil {
		return ref, closeErr
	}
	metadata := resourceFileMetadata{
		Name: ResourceUploadName(0, ref, ""),
		// Preserve the content type that ServeContent previously inferred from
		// the on-disk extension, including names whose MIME supplied that suffix.
		MIME: mime.TypeByExtension(filepath.Ext(ResourceUploadName(0, ref, ref.MIME))),
	}
	metaFile, err := os.CreateTemp(dir, ".metadata-*")
	if err != nil {
		return ref, err
	}
	defer os.Remove(metaFile.Name())
	encodeErr := json.NewEncoder(metaFile).Encode(metadata)
	closeErr = metaFile.Close()
	if encodeErr != nil {
		return ref, encodeErr
	}
	if closeErr != nil {
		return ref, closeErr
	}
	if err := os.Rename(metaFile.Name(), metadataPath); err != nil {
		return ref, err
	}
	committed = true
	ref.SizeBytes = size
	ref.SHA256 = hex.EncodeToString(hash.Sum(nil))
	ref.InternalURL = "internal://" + id
	return ref, nil
}

func (s *ResourceStore) Open(internalURL string) (*os.File, ResourceRef, error) {
	if s == nil {
		return nil, ResourceRef{}, fmt.Errorf("resource store is nil")
	}
	id := strings.TrimPrefix(strings.TrimSpace(internalURL), "internal://")
	if id == "" {
		return nil, ResourceRef{}, fmt.Errorf("invalid internal resource url")
	}
	dir := filepath.Join(s.Dir, resourceDirectory(id))
	raw, err := os.ReadFile(filepath.Join(dir, ".resource", "metadata.json"))
	if err == nil {
		var metadata resourceFileMetadata
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, ResourceRef{}, err
		}
		file, ref, err := openResourceFile(filepath.Join(dir, ".resource", "content"), id)
		if err != nil {
			return nil, ResourceRef{}, err
		}
		ref.Name, ref.MIME = metadata.Name, metadata.MIME
		return file, ref, nil
	}
	if !os.IsNotExist(err) {
		return nil, ResourceRef{}, err
	}
	// Read both earlier layouts without migrating or renaming existing files.
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, ResourceRef{}, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return openResourceFile(filepath.Join(dir, entry.Name()), id)
		}
	}
	// Resources saved before the per-ID directory layout remain readable.
	entries, err = os.ReadDir(s.Dir)
	if err != nil {
		return nil, ResourceRef{}, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), SafeSegment(id)+"-") {
			return openResourceFile(filepath.Join(s.Dir, entry.Name()), id)
		}
	}
	return nil, ResourceRef{}, os.ErrNotExist
}

func resourceDirectory(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "res-" + hex.EncodeToString(sum[:])
}

func openResourceFile(path, id string) (*os.File, ResourceRef, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, ResourceRef{}, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, ResourceRef{}, err
	}
	return file, ResourceRef{ID: id, InternalURL: "internal://" + id, Name: info.Name(), SizeBytes: info.Size()}, nil
}
