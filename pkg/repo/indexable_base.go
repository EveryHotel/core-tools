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

// TODO everyHotel
//  У методов delete create update добавились SqlQueryOption

type IndexableModel[I any] interface {
	GetModelIndex() I
	IsDeleted() bool
}

type Index[ID int64 | string] interface {
	GetIdentity() ID
}

type IndexableBaseRepo[I Index[ID], E IndexableModel[I], ID int64 | string] interface {
	BaseRepo[E, ID]
	Reindex(ctx context.Context) error
	GetValue(id ID) (I, error)
	SearchByTerm(string, map[string]any, ...meilisearch.OptHandler) ([]I, error)
	UpdateIndex(ctx context.Context, entity E) error
	MultipleSearch(requests []*meili.SearchRequest) ([][]I, error)
}

type indexableBaseRepo[I Index[ID], E IndexableModel[I], ID int64 | string] struct {
	BaseRepo[E, ID]
	meili                meilisearch.MeiliService
	indexName            string
	alias                string
	idColumn             string
	setId                func(ptr *E, id ID)
	extendIndexableItems func([]E) ([]E, error)
	indexRelations       []ListOptionRelation
	meiliSettings        *meili.Settings
}

func NewIndexableRepository[I Index[ID], E IndexableModel[I], ID int64 | string](
	db database.DBService,
	meili meilisearch.MeiliService,
	indexName, tableName, alias, idColumn string,
	setId func(ptr *E, id ID),
	extendIndexableItems func([]E) ([]E, error),
	indexRelations []ListOptionRelation,
	meiSettings *meili.Settings,
) IndexableBaseRepo[I, E, ID] {
	return &indexableBaseRepo[I, E, ID]{
		BaseRepo:             NewRepository[E, ID](db, tableName, alias, idColumn),
		meili:                meili,
		indexName:            indexName,
		alias:                alias,
		idColumn:             idColumn,
		setId:                setId,
		extendIndexableItems: extendIndexableItems,
		indexRelations:       indexRelations,
		meiliSettings:        meiSettings,
	}
}

// List возвращает список сущностей
func (r *indexableBaseRepo[I, E, ID]) List(ctx context.Context, options ...SqlQueryOption) ([]E, error) {
	res, err := r.BaseRepo.List(ctx, options...)
	if err != nil {
		return res, err
	}

	return res, nil
}

// ListBy возвращает список сущностей по критерию
func (r *indexableBaseRepo[I, E, ID]) ListBy(ctx context.Context, criteria map[string]any, options ...ListOption) ([]E, error) {
	res, err := r.BaseRepo.ListBy(ctx, criteria, options...)
	if err != nil {
		return res, err
	}

	return res, nil
}

// ListByExpression возвращает список сущностей по выражению
func (r *indexableBaseRepo[I, E, ID]) ListByExpression(ctx context.Context, criteria exp.ExpressionList, options ...ListOption) ([]E, error) {
	res, err := r.BaseRepo.ListByExpression(ctx, criteria, options...)
	if err != nil {
		return res, err
	}

	return res, nil
}

// Create Создает новую сущность
func (r *indexableBaseRepo[I, E, ID]) Create(ctx context.Context, entity E, options ...SqlQueryOption) (ID, error) {
	id, err := r.BaseRepo.Create(ctx, entity, options...)
	if err != nil {
		return id, err
	}

	r.updateIndexByIds(ctx, []ID{id})

	return id, nil
}

// CreateMultiple Создает новые сущности
func (r *indexableBaseRepo[I, E, ID]) CreateMultiple(ctx context.Context, entities []E, options ...SqlQueryOption) ([]ID, error) {
	ids, err := r.BaseRepo.CreateMultiple(ctx, entities, options...)
	if err != nil {
		return ids, err
	}

	r.updateIndexByIds(ctx, ids)

	return ids, nil
}

// Update Обновляет сущность
func (r *indexableBaseRepo[I, E, ID]) Update(ctx context.Context, entity E, options ...SqlQueryOption) error {
	if err := r.BaseRepo.Update(ctx, entity, options...); err != nil {
		return err
	}

	r.updateIndexByIds(ctx, []ID{entity.GetModelIndex().GetIdentity()})

	return nil
}

// UpdateMultiple Обновляет сущности
// TODO: при conflict_target, отличном от ID, входные сущности могут не содержать ID,
// поэтому updateIndexByIds не сможет перечитать и обновить документы в индексе.
func (r *indexableBaseRepo[I, E, ID]) UpdateMultiple(ctx context.Context, entities []E, options ...SqlQueryOption) error {
	if err := r.BaseRepo.UpdateMultiple(ctx, entities, options...); err != nil {
		return err
	}

	ids := make([]ID, 0, len(entities))
	for _, entity := range entities {
		ids = append(ids, entity.GetModelIndex().GetIdentity())
	}
	r.updateIndexByIds(ctx, ids)

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
	r.updateIndexByIds(ctx, []ID{entity.GetModelIndex().GetIdentity()})
	return nil
}

func (r *indexableBaseRepo[I, E, ID]) updateIndexByIds(ctx context.Context, ids []ID) {
	if len(ids) == 0 {
		return
	}

	options := []ListOption{}
	if len(r.indexRelations) > 0 {
		options = append(options, WithRelations(r.indexRelations))
	}
	entities, err := r.BaseRepo.ListBy(ctx, map[string]any{r.alias + "." + r.idColumn: ids}, options...)
	if err != nil {
		slog.ErrorContext(ctx, "load documents for index error",
			slog.Any("error", err),
			slog.String("index", r.indexName),
			slog.Int("count", len(ids)),
		)
		return
	}

	if r.extendIndexableItems != nil {
		entities, err = r.extendIndexableItems(entities)
		if err != nil {
			slog.ErrorContext(ctx, "extend documents for index error",
				slog.Any("error", err),
				slog.String("index", r.indexName),
				slog.Int("count", len(ids)),
			)
			return
		}
	}

	r.updateIndexMultiple(ctx, entities)
}

func (r *indexableBaseRepo[I, E, ID]) updateIndexMultiple(ctx context.Context, entities []E) {
	documents := make([]I, 0, len(entities))
	deleteIds := make([]string, 0)
	for _, entity := range entities {
		document := entity.GetModelIndex()
		if entity.IsDeleted() {
			deleteIds = append(deleteIds, fmt.Sprintf("%v", document.GetIdentity()))
			continue
		}
		documents = append(documents, document)
	}

	if len(documents) > 0 {
		if err := r.meili.UpdateDocuments(r.indexName, documents); err != nil {
			slog.ErrorContext(ctx, "update documents error",
				slog.Any("error", err),
				slog.String("index", r.indexName),
				slog.Int("count", len(documents)),
			)
		}
	}

	if len(deleteIds) > 0 {
		if err := r.meili.DeleteDocuments(r.indexName, deleteIds); err != nil {
			slog.ErrorContext(ctx, "delete documents error",
				slog.Any("error", err),
				slog.String("index", r.indexName),
				slog.Int("count", len(deleteIds)),
			)
		}
	}
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

		if r.extendIndexableItems != nil {
			items, err = r.extendIndexableItems(items)
			if err != nil {
				return err
			}
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
func (r *indexableBaseRepo[I, E, ID]) Delete(ctx context.Context, id ID, options ...SqlQueryOption) error {
	if err := r.BaseRepo.Delete(ctx, id, options...); err != nil {
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
