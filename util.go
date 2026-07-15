package uvim

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var unsafeSegmentPattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func StringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return fmt.Sprintf("%d", int64(typed))
		}
		return fmt.Sprintf("%v", typed)
	case int:
		return fmt.Sprintf("%d", typed)
	case int64:
		return fmt.Sprintf("%d", typed)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func MapStringAny(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok && typed != nil {
		return typed
	}
	return map[string]any{}
}

func SafeSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	value = unsafeSegmentPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, ".-_")
	if value == "" {
		return "unknown"
	}
	if len(value) > 96 {
		value = strings.TrimRight(value[:96], ".-_")
	}
	if value == "" {
		return "unknown"
	}
	return value
}

func NewID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", SafeSegment(prefix), time.Now().UnixNano())
	}
	return SafeSegment(prefix) + "_" + hex.EncodeToString(b[:])
}

func ResourceFileName(index int, ref ResourceRef, contentType string) string {
	return fmt.Sprintf("%02d-%s", index+1, ResourceUploadName(index, ref, contentType))
}

// ResourceUploadName returns a provider-safe basename for multipart, MIME, and
// JSON upload metadata. It deliberately removes path and header control bytes.
func ResourceUploadName(index int, ref ResourceRef, contentType string) string {
	raw := filepath.Base(FirstNonEmpty(ref.Name, ref.Key, ref.ID, fmt.Sprintf("resource-%d", index)))
	ext := filepath.Ext(raw)
	if ext == "" && contentType != "" {
		if exts, _ := mime.ExtensionsByType(contentType); len(exts) > 0 {
			ext = exts[0]
		}
	}
	base := SafeSegment(strings.TrimSuffix(raw, filepath.Ext(raw)))
	ext = strings.TrimPrefix(ext, ".")
	ext = unsafeSegmentPattern.ReplaceAllString(ext, "")
	if len(ext) > 16 {
		ext = ext[:16]
	}
	if ext == "" {
		return base
	}
	return base + "." + ext
}

func ResourceKindFromMIME(contentType, fallback string) string {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case strings.HasPrefix(contentType, "image/"):
		return ElementImage
	case strings.HasPrefix(contentType, "audio/"):
		return ElementAudio
	case strings.HasPrefix(contentType, "video/"):
		return ElementVideo
	case fallback != "":
		return fallback
	default:
		return ElementFile
	}
}

func NowUTC() time.Time {
	return time.Now().UTC()
}

func TrimOutboundText(text string, maxBytes int) string {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\r\n", "\n"))
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text
	}
	out := make([]rune, 0, len(text))
	total := 0
	for _, r := range text {
		size := len(string(r))
		if total+size > maxBytes {
			break
		}
		out = append(out, r)
		total += size
	}
	return string(out)
}
