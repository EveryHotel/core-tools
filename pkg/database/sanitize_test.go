package database_test

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jonboulle/clockwork"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"git.esphere.local/SberbankTravel/hotels/core-tools/pkg/database"
)

type sanitizeTestCase struct {
	Id        int64     `db:"id" primary:"1" fake:"{uint64}"`
	Sentence  string    `db:"sentence" fake:"{sentence:2}"`
	ManyTags  string    `db:"many_tags" json:"many_tags" fake:"{sentence:1}"`
	CreatedAt time.Time `db:"created_at" fake:"{Date}"`
	UpdatedAt time.Time `db:"updated_at"  fake:"{Date}"`
	Ignore    int64     `fake:"{int64}"`
}

type objectWithoutDbTags struct {
	Param1 int64
	Param2 string    `json:"param2"`
	Param3 time.Time `json:"param3" yaml:"param3"`
}

var _ = Describe("Sanitize", func() {
	var testObjWithTags sanitizeTestCase
	//var testObjWithOutTags objectWithoutDbTags
	var clock clockwork.FakeClock
	var fakeNow time.Time

	BeforeEach(func() {
		gofakeit.Struct(&testObjWithTags)

		clock = clockwork.NewFakeClock()
		fakeNow = clock.Now()
	})

	Describe("Sanitize", func() {
		Context("when empty options", func() {
			It("should return values from db tag", func() {
				res := database.Sanitize(testObjWithTags)

				Expect(res).Should(Equal([]interface{}{"id", "sentence", "many_tags", "created_at", "updated_at"}))
			})
		})
		Context("when prefix options", func() {
			Context("when prefix is empty string", func() {
				//но это херь какая-то, надо бы ошибку
				It("should return values from db tag with prefix", func() {
					res := database.Sanitize(testObjWithTags, database.WithPrefix(""))
					Expect(res).Should(Equal([]interface{}{".id", ".sentence", ".many_tags", ".created_at", ".updated_at"}))
				})
			})
			Context("when prefix is not empty string", func() {
				It("should return values from db tag with prefix", func() {
					res := database.Sanitize(testObjWithTags, database.WithPrefix("prefix"))

					Expect(res).Should(Equal([]interface{}{"prefix.id", "prefix.sentence", "prefix.many_tags", "prefix.created_at", "prefix.updated_at"}))
				})
			})
		})
	})

	Describe("SanitizeRows", func() {
		It("when rows options is empty", func() {
			id, res := database.SanitizeRows(testObjWithTags, clock)
			Expect(id).Should(Equal(testObjWithTags.Id))

			Expect(res).Should(Equal(map[string]any{
				"sentence":   testObjWithTags.Sentence,
				"many_tags":  testObjWithTags.ManyTags,
				"created_at": testObjWithTags.CreatedAt,
				"updated_at": testObjWithTags.UpdatedAt,
			}))
		})
		Describe("DefaultTimestamps option", func() {
			It("when DefaultTimestamps is empty slice", func() {
				opts := []database.SanitizeRowsOption{database.WithDefaultTimestamps()}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":   testObjWithTags.Sentence,
					"many_tags":  testObjWithTags.ManyTags,
					"created_at": testObjWithTags.CreatedAt,
					"updated_at": testObjWithTags.UpdatedAt,
				}))
			})
			It("when DefaultTimestamps has values", func() {
				opts := []database.SanitizeRowsOption{database.WithDefaultTimestamps("created_at", "updated_at")}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":   testObjWithTags.Sentence,
					"many_tags":  testObjWithTags.ManyTags,
					"created_at": fakeNow,
					"updated_at": fakeNow,
				}))
			})
			It("when DefaultTimestamps has values which not exist in target", func() {
				opts := []database.SanitizeRowsOption{database.WithDefaultTimestamps("not_exist")}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":   testObjWithTags.Sentence,
					"many_tags":  testObjWithTags.ManyTags,
					"created_at": testObjWithTags.CreatedAt,
					"updated_at": testObjWithTags.UpdatedAt,
				}))
			})
		})

		Describe("WithSkippingFields option", func() {
			It("when WithSkippingFields is empty slice", func() {
				opts := []database.SanitizeRowsOption{database.WithSkippingFields()}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":   testObjWithTags.Sentence,
					"many_tags":  testObjWithTags.ManyTags,
					"created_at": testObjWithTags.CreatedAt,
					"updated_at": testObjWithTags.UpdatedAt,
				}))
			})
			It("when WithSkippingFields has values", func() {
				opts := []database.SanitizeRowsOption{database.WithSkippingFields("created_at", "updated_at")}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":  testObjWithTags.Sentence,
					"many_tags": testObjWithTags.ManyTags,
				}))
			})
			It("when WithSkippingFields has values which not exist in target", func() {
				opts := []database.SanitizeRowsOption{database.WithSkippingFields("not_exist")}

				id, res := database.SanitizeRows(testObjWithTags, clock, opts...)
				Expect(id).Should(Equal(testObjWithTags.Id))

				Expect(res).Should(Equal(map[string]any{
					"sentence":   testObjWithTags.Sentence,
					"many_tags":  testObjWithTags.ManyTags,
					"created_at": testObjWithTags.CreatedAt,
					"updated_at": testObjWithTags.UpdatedAt,
				}))
			})
		})
	})

	Describe("SanitizeRowsForInsert", func() {
		It("when object doesn't has db tags", func() {
			id, res := database.SanitizeRowsForInsert(testObjWithTags, clock)
			Expect(id).Should(Equal(testObjWithTags.Id))

			Expect(res).Should(Equal(map[string]any{
				"sentence":   testObjWithTags.Sentence,
				"many_tags":  testObjWithTags.ManyTags,
				"created_at": fakeNow,
				"updated_at": fakeNow,
			}))
		})
		It("when object has db tags", func() {
			id, res := database.SanitizeRowsForInsert(testObjWithTags, clock)
			Expect(id).Should(Equal(testObjWithTags.Id))

			Expect(res).Should(Equal(map[string]any{
				"sentence":   testObjWithTags.Sentence,
				"many_tags":  testObjWithTags.ManyTags,
				"created_at": fakeNow,
				"updated_at": fakeNow,
			}))
		})
	})

	Describe("SanitizeRowsForUpdate", func() {
		It("when object has db tags", func() {
			id, res := database.SanitizeRowsForUpdate(testObjWithTags, clock)
			Expect(id).Should(Equal(testObjWithTags.Id))

			Expect(res).Should(Equal(map[string]any{
				"sentence":   testObjWithTags.Sentence,
				"many_tags":  testObjWithTags.ManyTags,
				"updated_at": fakeNow,
			}))
		})
	})
})
