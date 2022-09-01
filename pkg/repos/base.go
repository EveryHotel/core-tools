package repos

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/doug-martin/goqu/v9"

	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/database"
	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/elastic"
)

type BaseRepoInterface interface {
	Delete(ctx context.Context, id int64) error
	BulkUpdate(ctx context.Context, updateFields, where map[string]interface{}) error
}

func NewBaseRepo(DB database.DBService, tableName string) *BaseRepo {
	return &BaseRepo{
		DB:        DB,
		TableName: tableName,
	}
}

type BaseRepo struct {
	DB        database.DBService
	Index     elastic.BaseIndexInterface
	TableName string
}

// Delete soft удаление записи из таблицы
func (r BaseRepo) Delete(ctx context.Context, id int64) error {
	ds := goqu.Update(r.TableName).
		Where(goqu.C("id").Eq(id)).
		Set(goqu.Record{
			"deleted_at": time.Now(),
		})

	sql, args, err := ds.ToSQL()
	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return err
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec delete sql", "err", err)
		return err
	}

	if r.Index != nil {
		err = r.Index.Delete(strconv.Itoa(int(id)))
		if err != nil {
			log.Println("msg", "cannot delete search index", "err", err)
		}
	}

	return nil
}

// BulkUpdate обновляет записи в таблице по заданному условию
func (r BaseRepo) BulkUpdate(ctx context.Context, updateFields, where map[string]interface{}) error {
	whereClause := goqu.Ex(where)
	record := goqu.Record(updateFields)

	ds := goqu.Update(r.TableName).
		Where(whereClause).
		Set(record)
	sql, args, err := ds.ToSQL()

	if err != nil {
		log.Println("msg", "cannot build SQL query", "err", err)
		return err
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		log.Println("msg", "cannot Exec bulk update city sql", "err", err)
		return err
	}

	return nil
}
