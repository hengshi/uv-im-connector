package matrix

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

func TestEncryptedMatrixAttachmentDownloadsDecryptedBytes(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	iv := []byte("0123456789abcdef")
	plain := []byte("secret report")
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCTR(block, iv).XORKeyStream(encrypted, plain)
	hash := sha256.Sum256(encrypted)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasPrefix(req.URL.Path, "/_matrix/client/v1/media/download/matrix.test/media1") {
			t.Fatalf("download path = %s", req.URL.Path)
		}
		_, _ = w.Write(encrypted)
	}))
	defer server.Close()

	raw := `{
  "event_id":"$m1",
  "room_id":"!room:matrix.test",
  "sender":"@ada:matrix.test",
  "type":"m.room.message",
  "content":{
    "body":"secret.pdf",
    "msgtype":"m.file",
    "file":{
      "url":"mxc://matrix.test/media1",
      "key":{"alg":"A256CTR","k":"` + base64.RawURLEncoding.EncodeToString(key) + `"},
      "iv":"` + base64.RawStdEncoding.EncodeToString(iv) + `",
      "hashes":{"sha256":"` + base64.RawStdEncoding.EncodeToString(hash[:]) + `"}
    },
    "info":{"mimetype":"application/pdf","size":42}
  }
}`
	event, ok, err := Decode([]byte(raw), httpchannel.Config{BaseURL: server.URL, ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(event.Message.Resources) != 1 {
		t.Fatalf("event=%+v ok=%t", event, ok)
	}
	ref := event.Message.Resources[0]
	if ref.Private["matrix_file_key"] == "" || ref.Private["matrix_file_iv"] == "" || ref.Private["matrix_sha256"] == "" {
		t.Fatalf("encrypted metadata missing: %+v", ref.Private)
	}

	store := &uvim.ResourceStore{Dir: t.TempDir(), HTTPClient: server.Client()}
	provider, err := New(Config{BaseURL: server.URL, ConnectorID: "main", ResourceStore: store, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	downloaded, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{Resource: ref})
	if err != nil {
		t.Fatal(err)
	}
	if downloaded.Private != nil {
		t.Fatalf("downloaded private metadata = %+v, want nil", downloaded.Private)
	}
	file, _, err := store.Open(downloaded.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var got bytes.Buffer
	if _, err := got.ReadFrom(file); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), plain) {
		t.Fatalf("decrypted bytes = %q, want %q", got.Bytes(), plain)
	}
}

func TestEncryptedMatrixAttachmentRequiresHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ciphertext"))
	}))
	defer server.Close()

	ref := uvim.ResourceRef{
		URL: server.URL + "/file",
		Private: map[string]string{
			"matrix_file_alg": "A256CTR",
			"matrix_file_key": base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
			"matrix_file_iv":  base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef")),
		},
	}
	provider, err := New(Config{BaseURL: server.URL, ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()}, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{Resource: ref}); err == nil || !strings.Contains(err.Error(), "sha256 is required") {
		t.Fatalf("download error = %v, want required sha256", err)
	}
}
