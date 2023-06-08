package meilisearch

import (
	"encoding/json"
	"strconv"

	"github.com/meilisearch/meilisearch-go"
)

type MeiliService interface {
	AddDocuments(string, any) error
	Clear(string) error
	DeleteDocument(string, int64) error
	GetDocument(string, int64, any) error
	SearchDocuments(indexName string, q string) ([]any, error)
	UpdateDocuments(string, any) error
}

func NewMeiliService(client *meilisearch.Client) MeiliService {
	return &meiliService{
		client: client,
	}
}

type meiliService struct {
	client *meilisearch.Client
}

func (s meiliService) AddDocuments(indexName string, documents any) error {
	encoded, err := json.Marshal(documents)
	if err != nil {
		return err
	}

	index := s.client.Index(indexName)
	_, err = index.AddDocuments(encoded)
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

func (s meiliService) DeleteDocument(indexName string, id int64) error {
	if _, err := s.client.Index(indexName).DeleteDocument(strconv.Itoa(int(id))); err != nil {
		return err
	}

	return nil
}

func (s meiliService) GetDocument(indexName string, id int64, entity any) error {
	index := s.client.Index(indexName)

	return index.GetDocument(strconv.Itoa(int(id)), nil, entity)
}

func (s meiliService) UpdateDocuments(indexName string, documents any) error {
	encoded, err := json.Marshal(documents)
	if err != nil {
		return err
	}

	index := s.client.Index(indexName)
	_, err = index.UpdateDocuments(encoded)
	if err != nil {
		return err
	}

	return nil
}

func (s meiliService) SearchDocuments(indexName string, q string) ([]any, error) {
	index := s.client.Index(indexName)

	resp, err := index.Search(q, &meilisearch.SearchRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Hits, nil
}
