package lark

import (
	"context"
	"fmt"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

const (
	quotedMessageAncestorMaxReads = 4
	quotedMessageAncestorTimeout  = 5 * time.Second
	quotedMessageAncestorMaxBytes = 24 * 1024
)

// quotedMessageAncestors follows only native parent_id values from the
// selected, validated quote. It produces reference text only: ancestor files
// remain descriptors because the Hub eagerly downloads every flat resource.
func (p *Provider) quotedMessageAncestors(ctx context.Context, token string, selected quotedMessageItem, triggerID, chatID string) string {
	nextID := strings.TrimSpace(selected.ParentID)
	if nextID == "" {
		return ""
	}

	chainCtx, cancel := context.WithTimeout(ctx, quotedMessageAncestorTimeout)
	defer cancel()
	seen := map[string]bool{
		strings.TrimSpace(selected.ID): true,
		strings.TrimSpace(triggerID):   true,
	}
	var rendered strings.Builder
	rendered.WriteString("\nEarlier replies (newest to oldest; media descriptions only):\n")
	usedBytes := 0

	for reads := 0; nextID != ""; {
		if seen[nextID] {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_ancestor_cycle")
			break
		}
		if reads == quotedMessageAncestorMaxReads {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_ancestor_depth_limited")
			break
		}
		if err := chainCtx.Err(); err != nil {
			writeQuotedMessageAncestorStatus(&rendered, quotedMessageAncestorContextStatus(err))
			break
		}

		ancestor, _, err := p.lookupQuotedMessage(chainCtx, token, nextID, chatID)
		reads++
		if err != nil {
			writeQuotedMessageAncestorStatus(&rendered, quotedMessageAncestorLookupStatus(chainCtx, err))
			break
		}
		if err := chainCtx.Err(); err != nil {
			writeQuotedMessageAncestorStatus(&rendered, quotedMessageAncestorContextStatus(err))
			break
		}
		if ancestor.ThreadID != selected.ThreadID {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_ancestor_scope_mismatch")
			break
		}
		if !validQuotedMessageContent(ancestor) {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_invalid_content")
			break
		}

		node, ok := quotedMessageAncestorNode(ancestor)
		if !ok {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_ancestor_content_unavailable")
			break
		}
		if usedBytes+len(node) > quotedMessageAncestorMaxBytes {
			writeQuotedMessageAncestorStatus(&rendered, "quoted_message_ancestor_budget_limited")
			break
		}
		if err := chainCtx.Err(); err != nil {
			writeQuotedMessageAncestorStatus(&rendered, quotedMessageAncestorContextStatus(err))
			break
		}
		rendered.WriteString(node)
		usedBytes += len(node)
		seen[ancestor.ID] = true
		nextID = strings.TrimSpace(ancestor.ParentID)
	}
	return rendered.String()
}

func quotedMessageAncestorLookupStatus(ctx context.Context, err error) string {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return quotedMessageAncestorContextStatus(ctxErr)
	}
	return err.Error()
}

func quotedMessageAncestorContextStatus(err error) string {
	if err == context.DeadlineExceeded {
		return "quoted_message_ancestor_timeout"
	}
	return "quoted_message_ancestor_cancelled"
}

func quotedMessageAncestorNode(item quotedMessageItem) (string, bool) {
	var body string
	var resources []uvim.ResourceRef
	material := false
	if item.Type == "merge_forward" {
		body = "[Earlier merged-forward message: quote this message directly to read forwarded messages and files.]"
		material = true
	} else {
		body, resources, material = quotedItemMaterial(item)
	}
	if !material {
		return "", false
	}
	var rendered strings.Builder
	writeQuotedMessageAncestor(&rendered, item, body, resources)
	return rendered.String(), true
}

func writeQuotedMessageAncestor(context *strings.Builder, item quotedMessageItem, body string, resources []uvim.ResourceRef) {
	fmt.Fprintf(context, "Message ID: %q\n", item.ID)
	if item.ParentID != "" {
		fmt.Fprintf(context, "Parent message ID: %q\n", item.ParentID)
	}
	fmt.Fprintf(context, "Message type: %q\n", item.Type)
	if item.Sender.ID == "" && item.Sender.Kind == "" && item.Sender.Type == "" {
		context.WriteString("Sender: unknown\n")
	} else {
		fmt.Fprintf(context, "Sender: sender_type=%q id_type=%q id=%q\n", item.Sender.Type, item.Sender.Kind, item.Sender.ID)
	}
	if item.CreateTime != "" {
		fmt.Fprintf(context, "Create time: %q\n", item.CreateTime)
	}
	context.WriteString("Body:\n")
	writeQuotedMessageBody(context, body, "")
	for _, resource := range resources {
		fmt.Fprintf(context, "Media description: kind=%q name=%q\n", resource.Kind, resource.Name)
	}
}

func writeQuotedMessageAncestorStatus(context *strings.Builder, reason string) {
	fmt.Fprintf(context, "Earlier reply chain unavailable: %s\n", reason)
}
