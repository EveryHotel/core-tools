package meilisearch

import (
	"encoding/json"
	"fmt"

	"github.com/meilisearch/meilisearch-go"
)

type MeiliService interface {
	AddDocuments(string, any) error
	Clear(string) error
	DeleteDocument(string, string) error
	GetDocument(string, string, any) error
	SearchDocuments(indexName string, q string, filters map[string]any, opts ...OptHandler) ([]any, error)
	MultipleSearchDocuments(requests []*meilisearch.SearchRequest) ([]any, error)
	UpdateDocuments(string, any) error
	UpdateSettings(string, *meilisearch.Settings) error
}

func NewMeiliService(client meilisearch.ServiceManager) MeiliService {
	return &meiliService{
		client: client,
	}
}

type meiliService struct {
	client meilisearch.ServiceManager
}

func (s meiliService) AddDocuments(indexName string, documents any) error {
	encoded, err := json.Marshal(documents)
	if err != nil {
		return err
	}

	index := s.client.Index(indexName)
	_, err = index.AddDocuments(encoded, "id")
	if err != nil {
		return err
	}

	return nil
}

func (s meiliService) Clear(indexName string) error {
	if _, err := s.client.Index(indexName).DeleteAllDocuments(); err != nil {
		return err
	}

	return nil
}

func (s meiliService) DeleteDocument(indexName string, id string) error {
	if _, err := s.client.Index(indexName).DeleteDocument(id); err != nil {
		return err
	}

	return nil
}

func (s meiliService) GetDocument(indexName string, id string, entity any) error {
	index := s.client.Index(indexName)

	return index.GetDocument(id, nil, entity)
}

func (s meiliService) UpdateDocuments(indexName string, documents any) error {
	encoded, err := json.Marshal(documents)
	if err != nil {
		return err
	}

	index := s.client.Index(indexName)
	_, err = index.UpdateDocuments(encoded, "id")
	if err != nil {
		return err
	}

	return nil
}

func (s meiliService) SearchDocuments(indexName string, q string, filters map[string]any, opts ...OptHandler) ([]any, error) {
	index := s.client.Index(indexName)
	var filter []string
	if filters != nil {
		for k, v := range filters {
			filter = append(filter, fmt.Sprintf("%s = %v", k, v))
		}
	}

	req := meilisearch.SearchRequest{
		Filter: filter,
	}

	ApplyOpts(&req, opts...)

	resp, err := index.Search(q, &req)
	if err != nil {
		return nil, err
	}

	return resp.Hits, nil
}

func (s meiliService) MultipleSearchDocuments(requests []*meilisearch.SearchRequest) ([]any, error) {
	response, err := s.client.MultiSearch(&meilisearch.MultiSearchRequest{
		Queries: requests,
	})
	if err != nil {
		return nil, err
	}

	var res []any
	for _, r := range response.Results {
		res = append(res, r.Hits)
	}

	return res, nil
}

func (s meiliService) UpdateSettings(indexName string, settings *meilisearch.Settings) error {
	index := s.client.Index(indexName)
	_, err := index.UpdateSettings(settings)
	if err != nil {
		return err
	}

	return nil
}
