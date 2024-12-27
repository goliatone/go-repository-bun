package repository

import (
	"fmt"

	"github.com/uptrace/bun"
)

// InsertSetColumn will set the column to be updated
func InsertSetColumn(col string, val any) InsertCriteria {
	return func(q *bun.InsertQuery) *bun.InsertQuery {
		return q.Set(col, "?", val)
	}
}

func InsertOnConflictIgnore() InsertCriteria {
	return func(iq *bun.InsertQuery) *bun.InsertQuery {
		return iq.Ignore()
	}
}

func InsertOnConflictUpdate(cols ...string) InsertCriteria {
	return func(iq *bun.InsertQuery) *bun.InsertQuery {
		return iq.On(fmt.Sprintf("CONFLICT (%s) DO UPDATE", quoateAll(cols...)))
	}
}
