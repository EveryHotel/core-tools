package database_test

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/EveryHotel/core-tools/pkg/database"
)

var _ = Describe("Helpers", func() {
	Context("GetTableName", func() {
		It("when empty table name", func() {
			tableIdentifier := database.GetTableName("")
			Expect(tableIdentifier.IsEmpty()).Should(Equal(true))
		})

		It("when table name has schema", func() {
			tableName := gofakeit.Name()
			schemaName := gofakeit.Name()
			tableIdentifier := database.GetTableName(fmt.Sprintf("%s.%s", schemaName, tableName))
			Expect(tableIdentifier.GetSchema()).Should(Equal(schemaName))
			Expect(tableIdentifier.GetTable()).Should(Equal(tableName))
		})
		It("when table name is regular string", func() {
			tableName := gofakeit.Name()
			tableIdentifier := database.GetTableName(tableName)
			Expect(tableIdentifier.GetTable()).Should(Equal(tableName))
			Expect(tableIdentifier.GetSchema()).Should(Equal(""))
		})
		It("when table name has several dot", func() {
			//todo add error in orig function
			parts := []string{
				gofakeit.Name(),
				gofakeit.Name(),
				gofakeit.Name(),
			}

			tableIdentifier := database.GetTableName(strings.Join(parts, "."))
			Expect(tableIdentifier.GetTable()).Should(Equal(strings.Join(parts, ".")))
		})
	})
})
