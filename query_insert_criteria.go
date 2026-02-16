package repository

import (
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

// InsertSetColumn will set the column to be updated
func InsertSetColumn(col string, val any) InsertCriteria {
	return func(q *bun.InsertQuery) *bun.InsertQuery {
		return q.Set("? = ?", bun.Ident(col), val)
	}
}

func InsertOnConflictIgnore() InsertCriteria {
	return func(iq *bun.InsertQuery) *bun.InsertQuery {
		return iq.Ignore()
	}
}

func InsertOnConflictUpdate(cols ...string) InsertCriteria {
	return func(iq *bun.InsertQuery) *bun.InsertQuery {
		safe := make([]string, 0, len(cols))
		for _, col := range cols {
			if normalized, ok := normalizeSQLIdentifier(col); ok {
				safe = append(safe, normalized)
			}
		}
		if len(safe) == 0 {
			return iq
		}
		return iq.On(fmt.Sprintf("CONFLICT (%s) DO UPDATE", strings.Join(safe, ",")))
	}
}

var insertReturnOrderByIDMarker InsertCriteria = func(iq *bun.InsertQuery) *bun.InsertQuery {
	return iq
}

// InsertReturnOrderByID requests that bulk create results are reordered to match input IDs.
func InsertReturnOrderByID() InsertCriteria {
	return insertReturnOrderByIDMarker
}
