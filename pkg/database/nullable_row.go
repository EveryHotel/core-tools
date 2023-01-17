package database

import (
	"reflect"
	"strings"
	"time"

	"github.com/guregu/null"
)

// DestHasNullableRelations проверяем, есть ли сущности join с nullable
func DestHasNullableRelations(vDest reflect.Value, relations ...string) bool {
	if len(relations) == 0 {
		return false
	}

	for i := 0; i < vDest.NumField(); i++ {
		field := vDest.Field(i)
		tag := vDest.Type().Field(i).Tag

		if (field.Kind() == reflect.Struct || field.Kind() == reflect.Pointer) && tag.Get("relation") != "" {
			relationsField := strings.Split(tag.Get("relation"), ",")
			for _, relation := range relations {
				if len(relationsField) > 0 && relation == relationsField[0] && isNullableField(relationsField) {
					return true
				}
			}
		}
	}

	return false
}

// TransformDestToNullable переделываем изачальное поле с go типом в nullable тип
//  ввод int64
//  результат null.Int
func TransformDestToNullable(field reflect.Value) any {
	newField := reflect.New(field.Type()).Interface()

	switch field.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64:
		newField = reflect.New(reflect.TypeOf(null.Int{})).Interface()
	case reflect.Float32, reflect.Float64:
		newField = reflect.New(reflect.TypeOf(null.Float{})).Interface()
	case reflect.Bool:
		newField = reflect.New(reflect.TypeOf(null.Bool{})).Interface()
	case reflect.String:
		newField = reflect.New(reflect.TypeOf(null.String{})).Interface()
	default:
		if field.Type().Name() == reflect.TypeOf(time.Time{}).Name() {
			newField = reflect.New(reflect.TypeOf(null.Time{})).Interface()
		}
	}

	return newField
}

// GetNullableRowFromOrigDest получаем слайс интерфейсов с nullable полями в join сущностях
// ввод
//	Id      int64     `db:"id" primary:"1"`
//	PhotoId null.Int  `db:"photo_id"`
//	Photo   RoomPhoto `relation:"rp,nullable"`
//
//  результат
// 	[
//		1.Id      int64    `db:"id" primary:"1"`
//		2.PhotoId null.Int `db:"photo_id"`
//		3.[
//			1.	Id            null.Int    `db:"id" primary:"1"`
//			2.	DescriptionRu null.String `db:"description_ru"`
//			3.	DescriptionEn null.String `db:"description_en"`
//		]
//  ]
func GetNullableRowFromOrigDest(vDest reflect.Value, nullable bool, relations ...string) (newItem []any) {
	for i := 0; i < vDest.NumField(); i++ {
		field := vDest.Field(i)
		tag := vDest.Type().Field(i).Tag

		if tag.Get("db") != "" {
			// если vDest - nullable, то переводим его поля в null тип(для рекурсивного вызова)
			if !nullable {
				newItem = append(newItem, reflect.New(field.Type()).Interface())
			} else {
				newItem = append(newItem, TransformDestToNullable(field))
			}
		} else {
			// определяем что поле входит в массив передаваемых джоинов
			if tag.Get("relation") != "" && len(relations) > 0 {
				kind := field.Kind()
				var fieldValue reflect.Value
				// обрабатываем только 2 типа - указатель и структуру
				// если структура - передаем нарямую Value
				// если указатель - делаем новое Value
				if kind == reflect.Struct || kind == reflect.Pointer {
					if kind == reflect.Struct {
						fieldValue = field
					}

					if kind == reflect.Pointer {
						// создаем новое Value из типа указателя
						fieldValue = reflect.New(field.Type().Elem()).Elem()
					}
				}

				relationsField := strings.Split(tag.Get("relation"), ",")
				for _, relation := range relations {
					if len(relationsField) > 0 && relation == relationsField[0] {
						newItem = append(newItem, GetNullableRowFromOrigDest(fieldValue, isNullableField(relationsField))...)
					}
				}
			}
		}
	}

	return newItem
}

// SetNullableDestFields сканируем слайс с nullable интерфейсами
// ввод
// 	[
//		1.Id int64 `db:"id" primary:"1"`
//		2.PhotoId null.Int `db:"photo_id"`
//		3.[
//			1.	Id            null.Int    `db:"id" primary:"1"`
//			2.	DescriptionRu null.String `db:"description_ru"`
//			3.	DescriptionEn null.String `db:"description_en"`
//		]
//  ]
//  результат
// 	[
//		1.Id int64 `db:"id" primary:"1"`
//		2.PhotoId null.Int `db:"photo_id"`
//		3.Id null.Int `db:"id" primary:"1"`
//		4.DescriptionRu null.String `db:"description_ru"`
//		5.DescriptionEn null.String `db:"description_en"`
//  ]
func SetNullableDestFields(vDest reflect.Value, scanFields []any) []any {
	for i := 0; i < vDest.Len(); i++ {
		field := vDest.Index(i)

		//
		if field.Kind() == reflect.Slice {
			scanFields = SetNullableDestFields(field, scanFields)
		} else {
			scanFields = append(scanFields, field.Interface())
		}
	}

	return scanFields
}

// TransformNullableToDest передаем значение из ранее переделанного nullable поля в изначальный тип
//  ввод int64, null.Int
//  результат int64
func TransformNullableToDest(fieldOrig reflect.Value, field any) reflect.Value {
	switch fieldOrig.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		fieldOrig.SetUint(uint64((*(field.(*null.Int))).Int64))
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64:
		fieldOrig.SetInt((*(field.(*null.Int))).Int64)
	case reflect.Float32:
		fieldOrig.SetFloat((*(field.(*null.Float))).Float64)
	case reflect.Bool:
		fieldOrig.SetBool((*(field.(*null.Bool))).Bool)
	case reflect.String:
		fieldOrig.SetString((*(field.(*null.String))).String)
	default:
		if fieldOrig.Type().String() == reflect.TypeOf(time.Time{}).String() {
			fieldOrig = reflect.ValueOf((*(field.(*null.Time))).Time)
		}
	}

	return fieldOrig
}

// SetDestFromNullable проставляем изначальной сущности поля из nullable слайса
// ввод
// 	[
//		1.	Id      int64    `db:"id" primary:"1"`
//		2.	PhotoId null.Int `db:"photo_id"`
//		3.	Id            null.Int    `db:"id" primary:"1"`
//		4.	DescriptionRu null.String `db:"description_ru"`
//		5.	DescriptionEn null.String `db:"description_en"`
//
//  ]
//  результат
//	Id      int64     `db:"id" primary:"1"`
//	PhotoId null.Int  `db:"photo_id"`
//	Photo   RoomPhoto `relation:"rp,nullable"`
func SetDestFromNullable(vDestOrig reflect.Value, vDest []any, nullable bool, iDest int, relations ...string) (reflect.Value, int) {
	for i := 0; i < vDestOrig.NumField(); i++ {
		fieldOrig := vDestOrig.Field(i)
		tag := vDestOrig.Type().Field(i).Tag

		if len(vDest) == iDest {
			continue
		}
		field := vDest[iDest]

		if tag.Get("db") != "" {
			if nullable {
				fieldOrig.Set(TransformNullableToDest(fieldOrig, field))
			} else {
				fieldOrig.Set(reflect.ValueOf(field).Elem())
			}
			iDest++
		} else if tag.Get("relation") != "" && len(relations) > 0 {
			kind := fieldOrig.Kind()
			var fieldValue reflect.Value
			if kind == reflect.Struct || kind == reflect.Pointer {
				if kind == reflect.Struct {
					fieldValue = fieldOrig
				}

				if kind == reflect.Pointer {
					// создаем новое Value из типа указателя
					fieldValue = reflect.New(fieldOrig.Type().Elem()).Elem()
				}

				relationsField := strings.Split(tag.Get("relation"), ",")
				for _, relation := range relations {
					if len(relationsField) > 0 && relation == relationsField[0] {
						setField, newIDest := SetDestFromNullable(fieldValue, vDest, isNullableField(relationsField), iDest)
						iDest = newIDest

						if kind == reflect.Pointer {
							// проставляем для указателя, указатель из Value
							fieldOrig.Set(setField.Addr())
						} else {
							fieldOrig.Set(setField)
						}
					}
				}
			}
		}
	}

	return vDestOrig, iDest
}

func isNullableField(relationsField []string) bool {
	count := 0
	for _, relationField := range relationsField {
		// первый relation игнорим, тк нужен параметр nullable
		if count == 0 {
			count++
			continue
		}
		if relationField == "nullable" {
			return true
		}
	}

	return false
}
