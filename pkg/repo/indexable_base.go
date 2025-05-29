package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	meili "github.com/meilisearch/meilisearch-go"

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
	GetValue(id ID) (I, error)
	SearchByTerm(string, map[string]any, ...meilisearch.OptHandler) ([]I, error)
	UpdateIndex(ctx context.Context, entity E) error
	MultipleSearch(requests []*meili.SearchRequest) ([][]I, error)
}

type indexableBaseRepo[I any, E IndexableModel[I], ID int64 | string] struct {
	BaseRepo[E, ID]
	meili          meilisearch.MeiliService
	indexName      string
	alias          string
	setId          func(ptr *E, id ID)
	indexRelations []ListOptionRelation
	meiliSettings  *meili.Settings
}

func NewIndexableRepository[I any, E IndexableModel[I], ID int64 | string](
	db database.DBService,
	meili meilisearch.MeiliService,
	indexName, tableName, alias, idColumn string,
	setId func(ptr *E, id ID),
	indexRelations []ListOptionRelation,
	meiSettings *meili.Settings,
) IndexableBaseRepo[I, E, ID] {
	return &indexableBaseRepo[I, E, ID]{
		BaseRepo:       NewRepository[E, ID](db, tableName, alias, idColumn),
		meili:          meili,
		indexName:      indexName,
		alias:          alias,
		setId:          setId,
		indexRelations: indexRelations,
		meiliSettings:  meiSettings,
	}
}

// Create Создает новую сущность
func (r *indexableBaseRepo[I, E, ID]) Create(ctx context.Context, entity E) (ID, error) {
	id, err := r.BaseRepo.Create(ctx, entity)
	if err != nil {
		return id, err
	}

	r.setId(&entity, id)
	_ = r.UpdateIndex(ctx, entity)

	return id, nil
}

// Update Обновляет сущность
func (r *indexableBaseRepo[I, E, ID]) Update(ctx context.Context, entity E) error {
	if err := r.BaseRepo.Update(ctx, entity); err != nil {
		return err
	}

	_ = r.UpdateIndex(ctx, entity)

	return nil
}

func (r *indexableBaseRepo[I, E, ID]) SearchByTerm(term string, filters map[string]any, opts ...meilisearch.OptHandler) ([]I, error) {

	items, err := r.meili.SearchDocuments(r.indexName, term, filters, opts...)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 && len(term) >= 3 {
		items, err = r.meili.SearchDocuments(r.indexName, ReplaceCorrectLang(term), filters, opts...)
		if err != nil {
			return nil, err
		}
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

func (r *indexableBaseRepo[I, E, ID]) MultipleSearch(requests []*meili.SearchRequest) ([][]I, error) {
	for i := range requests {
		requests[i].IndexUID = r.indexName
	}

	items, err := r.meili.MultipleSearchDocuments(requests)
	if err != nil {
		return nil, err
	}

	var res [][]I
	for _, val := range items {
		var resItems []I
		//TODO: временное решение, чтобы преобразовывать в нужный тип
		encoded, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(encoded, &resItems); err != nil {
			return nil, err
		}

		res = append(res, resItems)
	}

	return res, nil
}

func (r *indexableBaseRepo[I, E, ID]) GetValue(id ID) (I, error) {
	var item I

	sId := fmt.Sprintf("%v", id)

	err := r.meili.GetDocument(r.indexName, sId, &item)
	if err != nil {
		return item, err
	}

	return item, nil
}

// UpdateIndex обновляет индекс сущности
func (r *indexableBaseRepo[I, E, ID]) UpdateIndex(ctx context.Context, entity E) error {
	if entity.IsDeleted() {
		return nil
	}

	if err := r.meili.UpdateDocuments(r.indexName, entity.GetModelIndex()); err != nil {
		slog.ErrorContext(ctx, "update document error",
			slog.Any("error", err),
			slog.String("index", r.indexName),
			slog.Any("entity", entity),
		)
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

	if r.meiliSettings != nil {
		err = r.meili.UpdateSettings(r.indexName, r.meiliSettings)
		if err != nil {
			return err
		}
	}

	for {
		opts := []ListOption{
			WithLimit(limit),
			WithOffset(offset),
			sortRule,
		}
		if r.indexRelations != nil {
			opts = append(opts, WithRelations(r.indexRelations))
		}

		items, err := r.ListBy(ctx, criteria, opts...)
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

// Delete удаляет сущность
func (r *indexableBaseRepo[I, E, ID]) Delete(ctx context.Context, id ID) error {
	if err := r.BaseRepo.Delete(ctx, id); err != nil {
		return err
	}

	sId := fmt.Sprintf("%v", id)

	if err := r.meili.DeleteDocument(r.indexName, sId); err != nil {
		slog.ErrorContext(ctx, "can't delete entity search index",
			slog.Any("error", err),
			slog.String("index", r.indexName),
			slog.String("id", sId),
		)
	}

	return nil
}

func (r *indexableBaseRepo[I, E, ID]) DeleteAndMoveReferences(ctx context.Context, id ID, newId ID) error {
	return r.Delete(ctx, id)
}
