package util

import (
	"fmt"
	"strings"
)

// JoinWithMaxPreview joins items with sep, truncating to maxPreview items
// JoinWithMaxPreview converts a slice of items to a comma-separated string, limiting the output to maxPreview items and appending ", ..." if truncation occurs.
func JoinWithMaxPreview[T any](rawItems []T, maxPreview int) string {
	truncated := len(rawItems) > maxPreview
	if truncated {
		rawItems = rawItems[:maxPreview]
	}
	items := make([]string, len(rawItems))
	for i, item := range rawItems {
		items[i] = fmt.Sprint(item)
	}
	s := strings.Join(items, ", ")
	if truncated {
		s += ", ..."
	}
	return s
}
