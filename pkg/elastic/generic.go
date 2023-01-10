package elastic

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/elastic/go-elasticsearch/v7"
)

type Index[T any] interface {
	GetIdentity() string
}

type GenericIndex[I Index[T], T any] interface {
	BaseIndexInterface
	Update(T) error
	SearchByName(term string, filters map[string]any) ([]I, error)
	GetValue(id int64) (I, error)
}

func NewIndex[I Index[T], T any](client *elasticsearch.Client, transform func(T) (I, error), alias, version string) GenericIndex[I, T] {
	return &genericIndex[I, T]{
		BaseIndex: *NewBaseIndex(client, alias, version, AutocompleteIndexConfig),
		transform: transform,
	}
}

type genericIndex[I Index[T], T any] struct {
	BaseIndex
	transform func(T) (I, error)
}

// GetValue Возвращает содержимое индекса
func (i genericIndex[I, T]) GetValue(id int64) (I, error) {
	idx := *new(I)
	item, err := i.Get(strconv.FormatInt(id, 10))
	if err != nil {
		return idx, err
	}

	bytes, err1 := json.Marshal(item.Source)
	if err2 := json.Unmarshal(bytes, &idx); err1 != nil || err2 != nil {
		log.Println("msg", "cannot convert entity search item", "err", err1, err2)
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
func (i genericIndex[I, T]) SearchByName(term string, filters map[string]any) ([]I, error) {
	var res []I

	response, err := i.SearchBy(term, []string{"name_ru", "name_en"}, filters)
	if err != nil {
		return res, err
	}

	for _, hit := range response.Hits.Hits {
		item := *new(I)
		bytes, err1 := json.Marshal(hit.Source)
		if err2 := json.Unmarshal(bytes, &item); err1 != nil || err2 != nil {
			log.Println("msg", "cannot convert entity search item", "err", err1, err2)
		}
		res = append(res, item)
	}

	return res, nil
}
