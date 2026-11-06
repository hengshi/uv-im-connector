package lark

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	uvim "github.com/hengshi/uv-im-connector"
)

// quotedPostMaterial is deliberately local to quoted-message rendering. Rich
// post decoding for ordinary incoming commands remains in decode.go.
type quotedPostMaterial struct {
	text      string
	resources []uvim.ResourceRef
	material  bool
}

func renderQuotedPost(raw string) quotedPostMaterial {
	var document map[string]any
	if json.Unmarshal([]byte(raw), &document) != nil {
		return quotedPostMaterial{}
	}

	resources, folders := quotedPostFiles(document)
	body, locale := selectQuotedPostBody(document)
	if body == nil {
		return quotedPostMaterial{text: strings.Join(folders, "\n"), resources: resources, material: len(folders) > 0}
	}

	bodyText, material := renderQuotedPostBody(body)
	resources = append(resources, quotedPostBodyResources(body)...)
	parts := make([]string, 0, 2+len(folders))
	if locale != "" {
		parts = append(parts, fmt.Sprintf("Post locale: %q", locale))
	}
	if bodyText != "" {
		parts = append(parts, bodyText)
	}
	parts = append(parts, folders...)
	return quotedPostMaterial{
		text:      strings.Join(parts, "\n"),
		resources: resources,
		material:  material || len(folders) > 0,
	}
}

func selectQuotedPostBody(document map[string]any) (map[string]any, string) {
	if quotedPostBodyCandidate(document) {
		return document, ""
	}
	for _, locale := range []string{"zh_cn", "en_us", "ja_jp"} {
		if body, ok := document[locale].(map[string]any); ok && quotedPostBodyCandidate(body) {
			return body, locale
		}
	}
	locales := make([]string, 0, len(document))
	for key, value := range document {
		if key == "zh_cn" || key == "en_us" || key == "ja_jp" || quotedPostEnvelopeKey(key) {
			continue
		}
		if body, ok := value.(map[string]any); ok && quotedPostBodyCandidate(body) {
			locales = append(locales, key)
		}
	}
	sort.Strings(locales)
	if len(locales) == 0 {
		return nil, ""
	}
	return document[locales[0]].(map[string]any), locales[0]
}

func quotedPostEnvelopeKey(key string) bool {
	switch strings.ToLower(key) {
	case "files", "metadata", "meta":
		return true
	default:
		return false
	}
}

func quotedPostBodyCandidate(body map[string]any) bool {
	if _, ok := body["title"].(string); ok {
		return true
	}
	if _, ok := body["content"].([]any); ok {
		return true
	}
	_, ok := body["content_v2"].([]any)
	return ok
}

func renderQuotedPostBody(body map[string]any) (string, bool) {
	blocks, _ := body["content"].([]any)
	if v2, ok := body["content_v2"].([]any); ok && len(v2) > 0 {
		blocks = v2
	}

	parts := make([]string, 0, 1+len(blocks))
	material := false
	if title, ok := body["title"].(string); ok {
		parts = append(parts, title)
		material = title != ""
	}
	for _, rawParagraph := range blocks {
		paragraph, ok := rawParagraph.([]any)
		if !ok {
			parts = append(parts, "[Post paragraph unavailable]")
			continue
		}
		var rendered strings.Builder
		for _, rawElement := range paragraph {
			element, ok := rawElement.(map[string]any)
			if !ok {
				rendered.WriteString("[Post element unavailable]")
				continue
			}
			text, hasMaterial := renderQuotedPostElement(element)
			rendered.WriteString(text)
			material = material || hasMaterial
		}
		parts = append(parts, rendered.String())
	}
	return strings.Join(parts, "\n"), material
}

func renderQuotedPostElement(element map[string]any) (string, bool) {
	tag, ok := element["tag"].(string)
	if !ok || tag == "" {
		return "[Post element unavailable]", false
	}
	text, textOK := element["text"].(string)
	switch tag {
	case "text", "md":
		if !textOK {
			return "[Post element unavailable]", false
		}
		return text, text != ""
	case "a":
		href, hrefOK := element["href"].(string)
		switch {
		case textOK && text != "" && hrefOK && href != "":
			return "[" + text + "](" + href + ")", true
		case textOK && text != "":
			return text, true
		case hrefOK && href != "":
			return href, true
		default:
			return "[Post element unavailable]", false
		}
	case "at":
		userID, idOK := element["user_id"].(string)
		name, nameOK := element["user_name"].(string)
		if userID == "@_all" || userID == "all" {
			return "[Mention: all]", true
		}
		if !idOK && !nameOK {
			return "[Post element unavailable]", false
		}
		if userID == "" && name == "" {
			return "[Post element unavailable]", false
		}
		return fmt.Sprintf("[Mention: user_id=%q name=%q]", userID, name), true
	case "code_block":
		if !textOK {
			return "[Post element unavailable]", false
		}
		language, languageOK := element["language"].(string)
		if languageOK && language != "" {
			return fmt.Sprintf("[Code block language=%q]\n%s\n[/Code block]", language, text), text != ""
		}
		return "[Code block]\n" + text + "\n[/Code block]", text != ""
	case "img":
		key, _ := element["image_key"].(string)
		return "[Image]", key != ""
	case "media":
		key, _ := element["file_key"].(string)
		return "[Media]", key != ""
	case "hr":
		return "\n---\n", false
	case "emotion":
		emoji, ok := element["emoji_type"].(string)
		if !ok || emoji == "" {
			return "[Post element unavailable]", false
		}
		return ":" + emoji + ":", true
	default:
		marker := fmt.Sprintf("[Unsupported post element: %q]", tag)
		if textOK && text != "" {
			return text + marker, true
		}
		return marker, false
	}
}

func quotedPostFiles(document map[string]any) ([]uvim.ResourceRef, []string) {
	rawFiles, _ := document["files"].([]any)
	resources := make([]uvim.ResourceRef, 0, len(rawFiles))
	folders := make([]string, 0)
	for _, rawFile := range rawFiles {
		file, ok := rawFile.(map[string]any)
		if !ok {
			continue
		}
		key, _ := file["file_key"].(string)
		name, _ := file["file_name"].(string)
		isFolder, _ := file["is_folder"].(bool)
		if isFolder {
			if key == "" && name == "" {
				continue
			}
			if name == "" {
				folders = append(folders, "[Folder attachment: request a ZIP archive or individual files; its contents were not retrieved.]")
			} else {
				folders = append(folders, fmt.Sprintf("[Folder attachment name=%q: request a ZIP archive or individual files; its contents were not retrieved.]", name))
			}
			continue
		}
		if key != "" {
			resources = append(resources, uvim.ResourceRef{Kind: uvim.ElementFile, Name: name, Key: key})
		}
	}
	return resources, folders
}

func quotedPostBodyResources(body map[string]any) []uvim.ResourceRef {
	refs := make([]uvim.ResourceRef, 0)
	seen := make(map[string]bool)
	appendResources := func(blocks []any) {
		for _, rawParagraph := range blocks {
			paragraph, ok := rawParagraph.([]any)
			if !ok {
				continue
			}
			for _, rawElement := range paragraph {
				element, ok := rawElement.(map[string]any)
				if !ok {
					continue
				}
				tag, _ := element["tag"].(string)
				kind, keyField := "", ""
				switch tag {
				case "img":
					kind, keyField = uvim.ElementImage, "image_key"
				case "media":
					kind, keyField = uvim.ElementVideo, "file_key"
				default:
					continue
				}
				key, _ := element[keyField].(string)
				if key == "" || seen[kind+"\x00"+key] {
					continue
				}
				seen[kind+"\x00"+key] = true
				refs = append(refs, uvim.ResourceRef{Kind: kind, Key: key})
			}
		}
	}
	if legacy, ok := body["content"].([]any); ok {
		appendResources(legacy)
	}
	if v2, ok := body["content_v2"].([]any); ok {
		appendResources(v2)
	}
	return refs
}
