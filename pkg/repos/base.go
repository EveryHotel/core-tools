package repos

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/EveryHotel/core-tools/pkg/database"
	"github.com/EveryHotel/core-tools/pkg/elastic"
)

// TODO everyHotel
//  не возвращаются ошибки в методах, но вроде как этот Repo  нигде не используется

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
		slog.ErrorContext(ctx, "Cannot build SQL query for delete",
			slog.Any("error", err),
			slog.String("table", r.TableName),
			slog.String("sql", sql),
			slog.Any("id", id),
		)
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec query for delete",
			slog.Any("error", err),
			slog.String("table", r.TableName),
			slog.Any("id", id),
		)
	}

	if r.Index != nil {
		err = r.Index.Delete(strconv.Itoa(int(id)))
		if err != nil {
			slog.ErrorContext(ctx, "Cannot delete search index",
				slog.Any("error", err),
				slog.String("table", r.TableName),
				slog.Any("id", id),
			)
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
		slog.ErrorContext(ctx, "Cannot build SQL query for bulk update",
			slog.Any("error", err),
			slog.String("table", r.TableName),
			slog.String("sql", sql),
		)
		return err
	}

	err = r.DB.Exec(ctx, sql, args)
	if err != nil {
		slog.ErrorContext(ctx, "Error during exec query for bulk update",
			slog.Any("error", err),
			slog.String("table", r.TableName),
			slog.Any("update", updateFields),
			slog.Any("where", where),
		)
		return err
	}

	return nil
}
