package repos

import (
	"context"
	"strconv"
	"time"

	"github.com/doug-martin/goqu/v9"
	logrus "github.com/sirupsen/logrus"

	"github.com/EveryHotel/core-tools/pkg/database"
	"github.com/EveryHotel/core-tools/pkg/elastic"
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
		logrus.WithFields(logrus.Fields{
			"table": r.TableName,
			"id":    id,
		}).Error("Cannot build sql query for delete", err)
		return err
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"table": r.TableName,
			"id":    id,
		}).Error("Cannot exec delete", err)
		return err
	}

	if r.Index != nil {
		err = r.Index.Delete(strconv.Itoa(int(id)))
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"table": r.TableName,
				"id":    id,
			}).Error("Cannot delete search index", err)
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
		logrus.WithFields(logrus.Fields{
			"table":  r.TableName,
			"update": updateFields,
			"where":  where,
		}).Error("Cannot build sql query for bulk update", err)
		return err
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"table":  r.TableName,
			"update": updateFields,
			"where":  where,
		}).Error("Cannot exec bulk update", err)
		return err
	}

	return nil
}
