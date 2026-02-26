package util

import "strings"

// JoinWithMaxPreview joins items with sep, truncating to maxPreview items
// and appending ", ..." when truncated.
func JoinWithMaxPreview(items []string, maxPreview int) string {
	truncated := len(items) > maxPreview
	if truncated {
		items = items[:maxPreview]
	}
	s := strings.Join(items, ", ")
	if truncated {
		s += ", ..."
	}
	return s
}
