package repo

import (
	"context"
	"log"

	"github.com/doug-martin/goqu/v9"

	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/database"
	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/elastic"
)

type IndexableBaseRepo[E any, I elastic.Index[E], ID int64 | string] interface {
	BaseRepo[E, ID]
	Reindex(ctx context.Context) error
	UpdateIndex(entity E) error
}

type indexableBaseRepo[E any, I elastic.Index[E], ID int64 | string] struct {
	BaseRepo[E, ID]
	db              database.DBService
	Index           elastic.GenericIndex[I, E]
	tableName       string
	alias           string
	setId           func(ptr *E, id ID)
	needUpdateIndex func(entity E) bool
}

func NewIndexableRepository[E any, I elastic.Index[E], ID int64 | string](
	db database.DBService,
	index elastic.GenericIndex[I, E],
	tableName string,
	alias string,
	idColumn string,
	setId func(ptr *E, id ID),
	needUpdateIndex func(entity E) bool,
) IndexableBaseRepo[E, I, ID] {

	return &indexableBaseRepo[E, I, ID]{
		BaseRepo:        NewRepository[E, ID](db, tableName, alias, idColumn),
		Index:           index,
		db:              db,
		tableName:       tableName,
		alias:           alias,
		setId:           setId,
		needUpdateIndex: needUpdateIndex,
	}
}

// Create Создает новую сущность
func (r *indexableBaseRepo[E, I, ID]) Create(ctx context.Context, entity E) (ID, error) {
	id, err := r.BaseRepo.Create(ctx, entity)
	if err != nil {
		return id, err
	}

	r.setId(&entity, id)

	err = r.Index.Update(entity)
	if err != nil {
		log.Println("msg", "cannot create entity search index", "err", err)
	}

	return id, nil
}

// Update Обновляет сущность
func (r *indexableBaseRepo[E, I, ID]) Update(ctx context.Context, entity E) error {
	if err := r.BaseRepo.Update(ctx, entity); err != nil {
		return err
	}

	if r.needUpdateIndex(entity) {
		err := r.Index.Update(entity)
		if err != nil {
			log.Println("msg", "cannot update entity search index", "err", err)
		}
	}

	return nil
}

// UpdateIndex обновляет индекс сущности
func (r *indexableBaseRepo[E, I, ID]) UpdateIndex(entity E) error {
	err := r.Index.Update(entity)
	if err != nil {
		log.Println("msg", "cannot update entity search index", "err", err)
	}
	return nil
}

// Reindex переиндексация всех сущностей
func (r *indexableBaseRepo[E, I, ID]) Reindex(ctx context.Context) error {

	var (
		err           error
		limit, offset uint
		sql           string
		index         I
	)

	err = r.Index.Recreate()
	if err != nil {
		return err
	}

	ds := goqu.Select(database.Sanitize(index, database.WithPrefix(r.alias))...).
		From(database.GetTableName(r.tableName).As(r.alias)).
		Where(goqu.Ex{r.alias + ".deleted_at": nil}).
		Order(goqu.I(r.alias + ".id").Asc())

	limit = 500
	offset = 0

	for {
		var res []I

		sql, _, err = ds.Limit(limit).Offset(offset).ToSQL()
		if err != nil {
			return err
		}

		if err = r.db.Select(ctx, sql, nil, &res); err != nil {
			return err
		}

		if len(res) <= 0 {
			break
		}

		data := make(map[string]interface{})
		for k := range res {
			data[res[k].GetIdentity()] = res[k]
		}

		err = r.Index.BulkIndex(data)
		if err != nil {
			return err
		}

		offset += limit
	}

	return nil
}
