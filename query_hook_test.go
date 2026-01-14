package repository

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

type countHook struct {
	before int32
	after  int32
}

func (h *countHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	atomic.AddInt32(&h.before, 1)
	return ctx
}

func (h *countHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	atomic.AddInt32(&h.after, 1)
}

func TestWithQueryHooksRegistersOnce(t *testing.T) {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	if err != nil {
		return
	}
	t.Cleanup(func() {
		_ = sqldb.Close()
	})

	bunDB := bun.NewDB(sqldb, sqlitedialect.New())

	ctx := context.Background()
	if err := createSchema(ctx, bunDB); err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	hook := &countHook{}
	_ = newTestUserRepository(bunDB, WithQueryHooks(hook))
	_ = newTestUserRepository(bunDB, WithQueryHooks(hook))

	var users []TestUser
	err = bunDB.NewSelect().Model(&users).Scan(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int32(1), atomic.LoadInt32(&hook.before))
	assert.Equal(t, int32(1), atomic.LoadInt32(&hook.after))
}

func TestWithQueryHookErrorHandlerPanicsOnNilHook(t *testing.T) {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	if err != nil {
		return
	}
	t.Cleanup(func() {
		_ = sqldb.Close()
	})

	bunDB := bun.NewDB(sqldb, sqlitedialect.New())

	var hook *countHook
	assert.Panics(t, func() {
		_ = newTestUserRepository(
			bunDB,
			WithQueryHookErrorHandler(PanicQueryHookErrorHandler),
			WithQueryHooks(hook),
		)
	})
}
