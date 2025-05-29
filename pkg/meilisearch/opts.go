package meilisearch

import (
	meili "github.com/meilisearch/meilisearch-go"
)

type OptHandler func(search *meili.SearchRequest)

func WithLimit(limit int64) OptHandler {
	return func(o *meili.SearchRequest) {
		o.Limit = limit
	}
}

func WithThreshold(threshold float64) OptHandler {
	return func(o *meili.SearchRequest) {
		o.RankingScoreThreshold = threshold
	}
}

func ApplyOpts(search *meili.SearchRequest, opts ...OptHandler) {
	for _, opt := range opts {
		opt(search)
	}
}
