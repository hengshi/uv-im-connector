package uvim

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var unsafeSegmentPattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

var fileNameControls = regexp.MustCompile(`[\p{Cc}]+`)
var multipartQuotes = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

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
	return value
}

func NewID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s_%d", SafeSegment(prefix), time.Now().UnixNano())
	}
	return SafeSegment(prefix) + "_" + hex.EncodeToString(b[:])
}

// ResourceFileName returns a local filesystem name with an ordinal prefix.
func ResourceFileName(index int, ref ResourceRef, contentType string) string {
	return fmt.Sprintf("%02d-%s", index+1, localResourceName(index, ref, contentType))
}

func localResourceName(index int, ref ResourceRef, contentType string) string {
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
	if ext == "" {
		return base
	}
	return base + "." + ext
}

// ResourceUploadName retains the display name while removing path components
// and characters that cannot be safely used in upload headers.
func ResourceUploadName(index int, ref ResourceRef, contentType string) string {
	ref.Name = resourceBasename(ref.Name)
	if ref.Name == "" {
		return localResourceName(index, ref, contentType)
	}
	return fileNameControls.ReplaceAllString(ref.Name, "-")
}

// ResourceMediaExtension infers the type suffix independently of display-name
// escaping, using the original basename and legacy extension fallback.
func ResourceMediaExtension(ref ResourceRef, contentType string) string {
	ref.Name = resourceBasename(ref.Name)
	return filepath.Ext(localResourceName(0, ref, contentType))
}

func resourceBasename(name string) string {
	name = path.Base(strings.ReplaceAll(name, `\`, "/"))
	if name == "." || name == ".." || name == "/" {
		return ""
	}
	return name
}

// MultipartFileDisposition quotes form-data parameters without changing UTF-8
// filename bytes or encoding them as the unsupported multipart filename* field.
func MultipartFileDisposition(field, name string) string {
	field = fileNameControls.ReplaceAllString(field, "-")
	name = fileNameControls.ReplaceAllString(name, "-")
	return `form-data; name="` + multipartQuotes.Replace(field) + `"; filename="` + multipartQuotes.Replace(name) + `"`
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

func NormalizeOutboundText(text string) string {
	return strings.TrimSpace(strings.ReplaceAll(text, "\r\n", "\n"))
}

// OptionalDeadline leaves transport I/O unbounded unless the caller configures a timeout.
func OptionalDeadline(now time.Time, timeout time.Duration) time.Time {
	if timeout > 0 {
		return now.Add(timeout)
	}
	return time.Time{}
}
