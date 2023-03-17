package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/sirupsen/logrus"
)

type BaseIndexInterface interface {
	Index(documentId string, data interface{}) (IndexResponse, error)
	Get(documentId string) (SearchHit, error)
	Delete(documentId string) error
	Search(request esapi.SearchRequest) (SearchResponse, error)
	SearchBy(term string, fields []string, filters map[string]any) (SearchResponse, error)
	BulkIndex(data map[string]interface{}) error
	Recreate() error
}

func NewBaseIndex(
	client *elasticsearch.Client,
	alias string,
	version string,
	config map[string]interface{},
) *BaseIndex {
	return &BaseIndex{
		client:  client,
		alias:   alias,
		version: version,
		config:  config,
	}
}

type BaseIndex struct {
	client  *elasticsearch.Client
	alias   string // Алиас индекса (например dictionary.city)
	version string // Версия индекса (например v2)
	config  map[string]interface{}
}

// Название индекса (например dictionary.city_v2)
// Когда индекс создается или пересоздается он получает названия например: dictionary.city_v1 dictionary.city_v2 итд
// А обращение к текущему актуальному индексу осуществляется по алиасу например: dictionary.city
func (i *BaseIndex) indexName() string {
	return i.alias + "_" + i.version
}

var (
	AutocompleteIndexConfig = map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"autocomplete_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 2,
						"max_gram": 20,
					},
					"russian_stop": map[string]interface{}{
						"type":      "stop",
						"stopwords": "_russian_",
					},
					"russian_stemmer": map[string]interface{}{
						"type":     "stemmer",
						"language": "light_russian",
					},
					"english_stop": map[string]interface{}{
						"type":      "stop",
						"stopwords": "_english_",
					},
					"english_stemmer": map[string]interface{}{
						"type":     "stemmer",
						"language": "english",
					},
				},
				"analyzer": map[string]interface{}{
					"default": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"char_filter": []string{
							"html_strip",
						},
						"filter": []string{
							"lowercase",
							"russian_stop",
							"russian_stemmer",
							"english_stop",
							"english_stemmer",
							"autocomplete_filter",
						},
					},
				},
			},
		},
	}

	SimpleAutocompleteIndexConfig = map[string]interface{}{
		"settings": map[string]interface{}{
			"analysis": map[string]interface{}{
				"filter": map[string]interface{}{
					"autocomplete_filter": map[string]interface{}{
						"type":     "edge_ngram",
						"min_gram": 2,
						"max_gram": 20,
					},
				},
				"analyzer": map[string]interface{}{
					"default": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"char_filter": []string{
							"html_strip",
						},
						"filter": []string{
							"lowercase",
							"autocomplete_filter",
						},
					},
				},
			},
		},
	}
)

// Index создает/обновляет документ в индексе
func (i *BaseIndex) Index(documentId string, data interface{}) (IndexResponse, error) {
	var response IndexResponse

	// чтобы не создавались автоматически дефолтные индексы, сначала проверяем его наличие и создаем при необходимости с нужными нам параметрами
	exists, err := i.exists()
	if err != nil {
		return response, err
	} else if !exists {
		if err = i.create(); err != nil {
			return response, err
		}
	}

	req := esapi.IndexRequest{
		Index:      i.alias,
		DocumentID: documentId,
		Body:       esutil.NewJSONReader(&data),
		Refresh:    "true",
	}

	result, err := req.Do(context.Background(), i.client)
	if err != nil {
		return response, err
	}
	defer result.Body.Close()

	if result.IsError() {
		return response, errors.New(fmt.Sprintf("[%s] Indexing error: index=%s document=%s", result.Status(), i.indexName(), documentId))
	}

	if err = json.NewDecoder(result.Body).Decode(&response); err != nil {
		return response, errors.New(fmt.Sprintf("[%s] Indexing error (parse response): index=%s document=%s %s", result.Status(), i.indexName(), documentId, err))
	}

	return response, nil
}

// Get возвращает конкретный документ
func (i *BaseIndex) Get(
	documentId string,
) (SearchHit, error) {
	req := esapi.GetRequest{
		Index:      i.alias,
		DocumentID: documentId,
	}

	result, err := req.Do(context.Background(), i.client)
	if err != nil {
		return SearchHit{}, err
	}
	defer result.Body.Close()

	if result.IsError() {
		return SearchHit{}, errors.New(fmt.Sprintf("[%s] Get error: index=%s document=%s", result.Status(), i.alias, documentId))
	}

	var hit SearchHit
	if err = json.NewDecoder(result.Body).Decode(&hit); err != nil {
		return SearchHit{}, err
	}
	return hit, nil
}

// Delete удаляет документ из индекса
func (i *BaseIndex) Delete(
	documentId string,
) error {
	req := esapi.DeleteRequest{
		Index:      i.alias,
		DocumentID: documentId,
	}

	res, err := req.Do(context.Background(), i.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return errors.New(fmt.Sprintf("[%s] Delete error: index=%s document=%s", res.Status(), i.alias, documentId))
	}

	return err
}

// Search осуществляет поиск документов по заданному запросу
func (i *BaseIndex) Search(
	request esapi.SearchRequest,
) (SearchResponse, error) {
	var response SearchResponse

	result, err := request.Do(context.Background(), i.client)
	if err != nil {
		return response, err
	}
	defer result.Body.Close()

	if result.IsError() {
		return response, errors.New(fmt.Sprintf("[%s] Search error: index=%s", result.Status(), request.Index))
	}

	if err = json.NewDecoder(result.Body).Decode(&response); err != nil {
		return response, err
	}

	return response, nil
}

// SearchBy осуществляет поиск в индексе по заданным полям
func (i *BaseIndex) SearchBy(
	term string,
	fields []string,
	filters map[string]interface{},
) (SearchResponse, error) {

	var (
		matches []interface{}
		buf     bytes.Buffer
	)

	for _, field := range fields {
		matches = append(matches, map[string]interface{}{
			"match": map[string]interface{}{
				field: map[string]interface{}{
					"query": term,
				},
			},
		})
	}
	boolQuery := map[string]interface{}{
		"should": matches,
	}
	if len(filters) > 0 {
		boolQuery["filter"] = map[string]interface{}{
			"term": filters,
		}
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": boolQuery,
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return SearchResponse{}, err
	}

	return i.Search(esapi.SearchRequest{
		Index: []string{i.alias},
		Body:  &buf,
	})
}

func (i *BaseIndex) Recreate() error {

	dRes, dErr := i.client.Indices.Delete([]string{i.indexName()}, i.client.Indices.Delete.WithIgnoreUnavailable(true))
	if dErr != nil {
		return dErr
	}
	defer dRes.Body.Close()

	if dRes.IsError() {
		return errors.New(fmt.Sprintf("[%s] Delete index error: index=%s", dRes.Status(), i.indexName()))
	}

	return i.create()
}

func (i *BaseIndex) BulkIndex(data map[string]interface{}) error {

	var (
		countSuccessful uint64
		err             error
	)

	// Create the BulkIndexer
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         i.indexName(),
		Client:        i.client,
		NumWorkers:    10,
		FlushBytes:    1 << 20, // 1MB
		FlushInterval: 30 * time.Second,
	})
	if err != nil {
		return err
	}

	start := time.Now().UTC()

	for k, _ := range data {
		v := data[k]
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: k,
				Body:       esutil.NewJSONReader(&v),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					atomic.AddUint64(&countSuccessful, 1)
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"index": reflect.TypeOf(v).Name(),
						}).Warn(err)
					} else {
						logrus.WithFields(logrus.Fields{
							"index": reflect.TypeOf(v).Name(),
						}).Errorf("%s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			return err
		}
	}

	if err = bi.Close(context.Background()); err != nil {
		return err
	}

	biStats := bi.Stats()
	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		logrus.WithFields(logrus.Fields{
			"index": i.indexName(),
		}).
			Infof("Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
				strconv.Itoa(int(biStats.NumFlushed)),
				strconv.Itoa(int(biStats.NumFailed)),
				dur.Truncate(time.Millisecond),
				strconv.Itoa(int(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
	} else {
		logrus.WithFields(logrus.Fields{
			"index": i.indexName(),
		}).
			Infof("Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
				strconv.Itoa(int(biStats.NumFlushed)),
				dur.Truncate(time.Millisecond),
				strconv.Itoa(int(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
			)
	}

	return nil
}

// exists проверяет наличие индекса
func (i *BaseIndex) exists() (bool, error) {

	req := esapi.IndicesExistsRequest{
		Index: []string{i.indexName()},
	}

	result, err := req.Do(context.Background(), i.client)
	if err != nil {
		return false, err
	}
	defer result.Body.Close()

	if result.IsError() {
		if result.StatusCode == 404 {
			return false, nil
		} else {
			return false, errors.New(fmt.Sprintf("[%s] Check index exists error: index=%s", result.Status(), i.indexName()))
		}
	}

	return true, nil
}

// create создает новую версию индекса и задает ему алиас
func (i *BaseIndex) create() error {

	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(i.config); err != nil {
		return err
	}

	req := esapi.IndicesCreateRequest{
		Index: i.indexName(),
		Body:  &buf,
	}

	result, err := req.Do(context.Background(), i.client)
	if err != nil {
		return err
	}
	defer result.Body.Close()

	if result.IsError() {
		return errors.New(fmt.Sprintf("[%s] Create index error: index=%s", result.Status(), i.indexName()))
	}

	req2 := esapi.IndicesPutAliasRequest{
		Index: []string{i.indexName()},
		Name:  i.alias,
	}

	result2, err2 := req2.Do(context.Background(), i.client)
	if err2 != nil {
		return err2
	}
	defer result2.Body.Close()

	if result2.IsError() {
		return errors.New(fmt.Sprintf("[%s] Create alias error: index=%s", result2.Status(), i.indexName()))
	}

	return nil
}
