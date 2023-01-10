package helpers

import "fmt"

func SplitWithEscaping(s string, separator, escape byte) []string {
	var token []byte
	var tokens []string
	for i := 0; i < len(s); i++ {
		if s[i] == separator {
			tokens = append(tokens, string(token))
			token = token[:0]
		} else if s[i] == escape && i+1 < len(s) {
			i++
			token = append(token, s[i])
		} else {
			token = append(token, s[i])
		}
	}
	tokens = append(tokens, string(token))
	return tokens
}

func FilterStringToIndexFilter(filters []string) map[string]any {
	indexFilters := make(map[string]any)
	for _, filter := range filters {
		split := SplitWithEscaping(filter, ':', '\\')
		if len(split) == 2 {
			indexFilters[fmt.Sprintf("%s.keyword", split[0])] = split[1]
		} else {

			return nil
		}
	}
	return indexFilters
}
