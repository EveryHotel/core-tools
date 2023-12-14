package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/elastic/go-elasticsearch/v7"
)

type Index[T any] interface {
	GetIdentity() string
}

type GenericIndex[I Index[T], T any] interface {
	BaseIndexInterface
	Update(T) error
	SearchByName(ctx context.Context, term string, filters map[string]any) ([]I, error)
	GetValue(id int64) (I, error)
}

func NewIndex[I Index[T], T any](client *elasticsearch.Client, transform func(T) (I, error), alias, version string, withStemmer bool) GenericIndex[I, T] {
	var config map[string]interface{}
	if withStemmer {
		config = AutocompleteIndexConfig
	} else {
		config = SimpleAutocompleteIndexConfig
	}
	return &genericIndex[I, T]{
		BaseIndex: *NewBaseIndex(client, alias, version, config),
		transform: transform,
	}
}

type genericIndex[I Index[T], T any] struct {
	BaseIndex
	transform func(T) (I, error)
}

// GetValue Возвращает содержимое индекса
func (i genericIndex[I, T]) GetValue(id int64) (I, error) {
	var idx I
	item, err := i.Get(strconv.FormatInt(id, 10))
	if err != nil {
		return idx, err
	}

	idx, err = i.transformSearchHitToIndex(&item)
	if err != nil {
		slog.Warn("transformSearchHitToIndex json marshal error",
			slog.Any("error", err),
			slog.String("index", fmt.Sprintf("%T", idx)),
		)
	}
	return idx, nil
}

func (i genericIndex[I, T]) transformSearchHitToIndex(hit *SearchHit) (I, error) {
	var idx I
	var bytes []byte
	var err error
	if bytes, err = json.Marshal(hit.Source); err != nil {
		return idx, fmt.Errorf("marshal: %w", err)
	}
	if err = json.Unmarshal(bytes, &idx); err != nil {
		return idx, fmt.Errorf("unmarshal: %w", err)
	}
	return idx, nil
}

// Update создает/обновляет индекс
func (i genericIndex[I, T]) Update(entity T) error {
	idx, err := i.transform(entity)
	if err != nil {
		return err
	}

	_, err = i.Index(idx.GetIdentity(), idx)
	if err != nil {
		return err
	}

	return nil
}

// SearchByName поиск сущности в индексе по названию
func (i genericIndex[I, T]) SearchByName(ctx context.Context, term string, filters map[string]any) ([]I, error) {
	var res []I

	response, err := i.SearchBy(term, []string{"name_ru", "name_en"}, filters)
	if err != nil {
		return res, err
	}

	for _, hit := range response.Hits.Hits {
		item, err := i.transformSearchHitToIndex(hit)
		if err != nil {
			slog.Warn("transformSearchHitToIndex json marshal error",
				slog.Any("error", err),
				slog.String("index", fmt.Sprintf("%T", hit)),
			)
		}
		res = append(res, item)
	}

	return res, nil
}
