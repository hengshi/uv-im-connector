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
	"sync"
)

type ResourceStore struct {
	Dir           string
	PublicBaseURL string
	HTTPClient    *http.Client
}

const resourceCurrentMetadata = "current.json"

// Publishing and opening a generation share a short lock, even when callers
// use separate ResourceStore values. The open descriptor pins the bytes.
var resourcePublicationLocks [256]sync.Mutex

type resourceFileMetadata struct {
	Content string `json:"content,omitempty"`
	Name    string `json:"name"`
	MIME    string `json:"mime,omitempty"`
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
	file, err := os.CreateTemp(dir, ".content-*")
	if err != nil {
		return ref, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(file.Name())
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), src)
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return ref, copyErr
	}
	if closeErr != nil {
		return ref, closeErr
	}
	metadata := resourceFileMetadata{
		Content: filepath.Base(file.Name()),
		Name:    ResourceUploadName(0, ref, ""),
		// Preserve the content type that ServeContent previously inferred from
		// the on-disk extension, including names whose MIME supplied that suffix.
		MIME: mime.TypeByExtension(ResourceMediaExtension(ref, ref.MIME)),
	}
	metaFile, err := os.CreateTemp(dir, ".metadata-*")
	if err != nil {
		return ref, err
	}
	defer os.Remove(metaFile.Name())
	encodeErr := json.NewEncoder(metaFile).Encode(metadata)
	if encodeErr == nil {
		encodeErr = metaFile.Sync()
	}
	closeErr = metaFile.Close()
	if encodeErr != nil {
		return ref, encodeErr
	}
	if closeErr != nil {
		return ref, closeErr
	}
	lock := &resourcePublicationLocks[sha256.Sum256([]byte(id))[0]]
	lock.Lock()
	defer lock.Unlock()
	metadataPath := filepath.Join(dir, resourceCurrentMetadata)
	previous, err := readResourceMetadata(metadataPath)
	if err != nil && !os.IsNotExist(err) {
		return ref, err
	}
	// The metadata rename is the single publication point. Readers always
	// get the name, MIME, and descriptor of the same immutable generation.
	if err := os.Rename(metaFile.Name(), metadataPath); err != nil {
		return ref, err
	}
	committed = true
	directory, err := os.Open(dir)
	if err != nil {
		return ref, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return ref, err
	}
	if validResourceContentName(previous.Content) {
		_ = os.Remove(filepath.Join(dir, previous.Content))
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
	resourceDir := filepath.Join(dir, ".resource")
	lock := &resourcePublicationLocks[sha256.Sum256([]byte(id))[0]]
	lock.Lock()
	defer lock.Unlock()
	metadata, err := readResourceMetadata(filepath.Join(resourceDir, resourceCurrentMetadata))
	content := metadata.Content
	if os.IsNotExist(err) {
		metadata, err = readResourceMetadata(filepath.Join(resourceDir, "metadata.json"))
		content = "content"
	} else if err == nil && !validResourceContentName(content) {
		return nil, ResourceRef{}, fmt.Errorf("invalid resource content generation")
	}
	if err == nil {
		file, ref, err := openResourceFile(filepath.Join(resourceDir, content), id)
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

func readResourceMetadata(path string) (resourceFileMetadata, error) {
	var metadata resourceFileMetadata
	raw, err := os.ReadFile(path)
	if err != nil {
		return metadata, err
	}
	err = json.Unmarshal(raw, &metadata)
	return metadata, err
}

func validResourceContentName(name string) bool {
	return strings.HasPrefix(name, ".content-") && filepath.Base(name) == name
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
