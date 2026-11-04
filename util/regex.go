package util

import (
	"regexp"
	"strings"
)

// WildCardToRegexp converts a wildcard pattern to a regular expression pattern.
func WildCardToRegexp(pattern string) string {
	components := strings.Split(pattern, "*")

	var result strings.Builder
	for i, literal := range components {
		// Replace * with .*
		if i > 0 {
			result.WriteString(".*")
		}

		// Quote any regular expression meta characters in the
		// literal text.
		result.WriteString(regexp.QuoteMeta(literal))
	}

	return "^" + result.String() + "$"
}
