package repo

import (
	"context"
	"encoding/json"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/sirupsen/logrus"

	"github.com/EveryHotel/core-tools/pkg/database"
	"github.com/EveryHotel/core-tools/pkg/meilisearch"
)

type IndexableModel[I any] interface {
	GetModelIndex() I
	IsDeleted() bool
}

type IndexableBaseRepo[I any, E IndexableModel[I], ID int64 | string] interface {
	BaseRepo[E, ID]
	Reindex(ctx context.Context) error
	SearchByTerm(string) ([]I, error)
	UpdateIndex(entity E) error
}

type indexableBaseRepo[I any, E IndexableModel[I], ID int64 | string] struct {
	BaseRepo[E, ID]
	meili     meilisearch.MeiliService
	indexName string
	alias     string
	setId     func(ptr *E, id ID)
}

func NewIndexableRepository[I any, E IndexableModel[I], ID int64 | string](
	db database.DBService,
	meili meilisearch.MeiliService,
	indexName, tableName, alias, idColumn string,
	setId func(ptr *E, id ID),
) IndexableBaseRepo[I, E, ID] {
	return &indexableBaseRepo[I, E, ID]{
		BaseRepo:  NewRepository[E, ID](db, tableName, alias, idColumn),
		meili:     meili,
		indexName: indexName,
		alias:     alias,
		setId:     setId,
	}
}

// Create Создает новую сущность
func (r *indexableBaseRepo[I, E, ID]) Create(ctx context.Context, entity E) (ID, error) {
	id, err := r.BaseRepo.Create(ctx, entity)
	if err != nil {
		return id, err
	}

	r.setId(&entity, id)
	_ = r.UpdateIndex(entity)

	return id, nil
}

// Update Обновляет сущность
func (r *indexableBaseRepo[I, E, ID]) Update(ctx context.Context, entity E) error {
	if err := r.BaseRepo.Update(ctx, entity); err != nil {
		return err
	}

	_ = r.UpdateIndex(entity)

	return nil
}

func (r *indexableBaseRepo[I, E, ID]) SearchByTerm(term string) ([]I, error) {
	items, err := r.meili.SearchDocuments(r.indexName, term)
	if err != nil {
		return nil, err
	}

	var res []I
	// TODO: временное решение, чтобы преобразовывать в нужный тип
	encoded, err := json.Marshal(items)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(encoded, &res); err != nil {
		return nil, err
	}

	return res, nil
}

// UpdateIndex обновляет индекс сущности
func (r *indexableBaseRepo[I, E, ID]) UpdateIndex(entity E) error {
	if entity.IsDeleted() {
		return nil
	}

	if err := r.meili.UpdateDocuments(r.indexName, entity.GetModelIndex()); err != nil {
		logrus.WithFields(logrus.Fields{
			"index":  r.indexName,
			"entity": entity,
		}).Error("cannot update entity search index ", err)
	}

	return nil
}

// Reindex переиндексация всех сущностей
func (r *indexableBaseRepo[I, E, ID]) Reindex(ctx context.Context) error {
	var (
		err           error
		limit, offset int64
	)

	err = r.meili.Clear(r.indexName)
	if err != nil {
		return err
	}

	limit = 500
	offset = 0
	criteria := make(map[string]any)

	if IsSoftDeletingEntity(*new(E)) {
		criteria[r.alias+".deleted_at"] = nil
	}
	sortRule := WithSort([]exp.OrderedExpression{goqu.I(r.alias + ".id").Asc()})

	for {
		items, err := r.ListBy(ctx, criteria, WithLimit(limit), WithOffset(offset), sortRule)
		if err != nil {
			return err
		}

		if len(items) <= 0 {
			break
		}

		var data []any
		for _, item := range items {
			data = append(data, item.GetModelIndex())
		}

		if err = r.meili.AddDocuments(r.indexName, data); err != nil {
			return err
		}

		offset += limit
	}

	return nil
}
