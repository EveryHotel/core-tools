package repo

import (
	"reflect"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/EveryHotel/core-tools/pkg/helpers"
)

// BuildConflictUpdate возвращает conflict_target и поля для обновления в случае конфликта
func BuildConflictUpdate(entity any) (string, map[string]any) {
	updateFields := make(map[string]any)
	var pKey string
	var conflictTarget string

	vEntity := reflect.ValueOf(entity)

	for i := 0; i < vEntity.NumField(); i++ {
		tag := vEntity.Type().Field(i).Tag

		dbFieldName := tag.Get("db")
		if dbFieldName == "" {
			continue
		}

		// пропускаем PK
		if pkTag := tag.Get("primary"); pkTag != "" {
			pKey = dbFieldName
			continue
		}

		// пропускаем колонку, отмеченную как conflict_target
		if ctTag := tag.Get("conflict_target"); ctTag != "" {
			conflictTarget = dbFieldName
			continue
		}

		// не обновляем created_at
		if helpers.StrArrContains([]string{"created_at"}, dbFieldName) {
			continue
		}

		if dbFieldName == "updated_at" {
			updateFields[dbFieldName] = time.Now()
			continue
		}

		updateFields[dbFieldName] = goqu.C(dbFieldName).Table("excluded")
	}

	if conflictTarget == "" {
		conflictTarget = pKey
	}

	return conflictTarget, updateFields
}
