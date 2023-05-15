package database_test

import (
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/EveryHotel/core-tools/pkg/database"
)

var _ = Describe("NullableRow", func() {
	Describe("DestHasNullableRelations", func() {
		It("empty relationship", func() {
			result := database.DestHasNullableRelations(reflect.Value{})
			Expect(result).Should(Equal(false))
		})
		It("relation without nullable", func() {
			dest := &struct {
				Value struct {
				} `relation:"v"`
			}{
				Value: struct{}{},
			}
			reflectValue := reflect.ValueOf(dest).Elem()
			result := database.DestHasNullableRelations(reflectValue, "v")
			Expect(result).Should(Equal(false))
		})
		It("relation with nullable", func() {
			dest := &struct {
				Value struct {
				} `relation:"v,nullable"`
			}{
				Value: struct{}{},
			}
			reflectValue := reflect.ValueOf(dest).Elem()
			result := database.DestHasNullableRelations(reflectValue, "v")
			Expect(result).Should(Equal(true))
		})
		It("relation with nullable", func() {
			dest := &struct {
				Value struct {
				} `relation:"v,nullable"`
			}{
				Value: struct{}{},
			}
			reflectValue := reflect.ValueOf(dest).Elem()
			result := database.DestHasNullableRelations(reflectValue, "v")
			Expect(result).Should(Equal(true))
		})
		It("relation with nullable on another field", func() {
			dest := &struct {
				Value struct {
				} `relation:"v,nullable"`
				Value2 struct {
				} `relation:"v2"`
			}{
				Value: struct{}{},
			}
			reflectValue := reflect.ValueOf(dest).Elem()
			result := database.DestHasNullableRelations(reflectValue, "v2")
			Expect(result).Should(Equal(false))
		})
	})

	//TODO сделать тесты для других методов, там логика не очень ясна, пусть егор делает
})
