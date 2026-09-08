package uvim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
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
	// Keep the ID in a separate component so it cannot make a legal filename
	// too long. Hashing the ID also keeps that component independent of its size.
	dir = filepath.Join(dir, resourceDirectory(id))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ref, err
	}
	name := ResourceUploadName(0, ref, ref.MIME)
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return ref, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), src)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return ref, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return ref, closeErr
	}
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
