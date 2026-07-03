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

const (
	DefaultResourceMaxBytes      int64 = 100 * 1024 * 1024
	DefaultResourceTotalMaxBytes int64 = 100 * 1024 * 1024
	DefaultResourceMaxCount            = 10
)

type ResourceStore struct {
	Dir           string
	PublicBaseURL string
	HTTPClient    *http.Client
	MaxBytes      int64
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ref, err
	}
	maxBytes := s.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultResourceMaxBytes
	}
	id := FirstNonEmpty(ref.ID, NewID("res"))
	ref.ID = id
	name := SafeSegment(id) + "-" + ResourceFileName(0, ref, ref.MIME)
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return ref, err
	}
	hash := sha256.New()
	limited := io.LimitReader(src, maxBytes+1)
	size, copyErr := io.Copy(io.MultiWriter(file, hash), limited)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return ref, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return ref, closeErr
	}
	if size > maxBytes {
		_ = os.Remove(path)
		return ref, fmt.Errorf("resource exceeds max size %d bytes", maxBytes)
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
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, ResourceRef{}, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), SafeSegment(id)+"-") {
			path := filepath.Join(s.Dir, entry.Name())
			file, err := os.Open(path)
			if err != nil {
				return nil, ResourceRef{}, err
			}
			info, _ := entry.Info()
			return file, ResourceRef{ID: id, InternalURL: "internal://" + id, Name: entry.Name(), SizeBytes: info.Size()}, nil
		}
	}
	return nil, ResourceRef{}, os.ErrNotExist
}
