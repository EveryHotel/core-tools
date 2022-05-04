package database

import (
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
)

// GetTableName формирует имя таблицы как goqu IdentifierExpression
func GetTableName(name string) exp.IdentifierExpression {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return goqu.T(parts[1]).Schema(parts[0])
	}

	return goqu.T(name)
}
