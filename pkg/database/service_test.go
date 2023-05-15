package database_test

import (
	"context"

	"github.com/driftprogramming/pgxpoolmock"
	"github.com/golang/mock/gomock"
	"github.com/jackc/pgx/v4/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/EveryHotel/core-tools/pkg/database"
)

var _ = Describe("Service", func() {
	var mockCtrl *gomock.Controller
	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	Describe("Count", func() {
		It("Count from inner select", func() {
			// given
			mockPool := pgxpoolmock.NewMockPgxPool(mockCtrl)

			pgxRows := pgxpoolmock.NewRow([]string{"count"}, int64(1))

			mockPool.EXPECT().QueryRow(gomock.Any(), "select count(*) from (select 1) t", gomock.Any()).Return(pgxRows)
			service := database.NewDBService(mockPool)

			count, err := service.Count(context.Background(), "select count(*) from (select 1) t", nil)

			Expect(err).Should(Succeed())
			Expect(count).Should(Equal(int64(1)))
		})
	})

	Describe("Begin", func() {
		It("Begin", func() {
			mockPool := pgxpoolmock.NewMockPgxPool(mockCtrl)
			mockPool.EXPECT().Begin(gomock.Any()).Return(&pgxpool.Tx{}, nil)
			service := database.NewDBService(mockPool)

			ctx, err := service.Begin(context.Background())

			Expect(err).Should(Succeed())
			Expect(ctx.Value(database.CtxDbTxKey)).ShouldNot(BeNil())
		})
	})

})
