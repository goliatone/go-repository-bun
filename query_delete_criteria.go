package repository

import (
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// DeleteByID will delete by the given ID
func DeleteByID(id string) DeleteCriteria {
	return DeleteBy("id", "=", id)
}

// DeleteBy will delete by a given property
func DeleteBy(column, operator, value string) DeleteCriteria {
	return func(q *bun.DeleteQuery) *bun.DeleteQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), value)
	}
}

// DeleteByTimetz will format the time provided
func DeleteByTimetz(column, operator string, value time.Time) DeleteCriteria {
	return func(q *bun.DeleteQuery) *bun.DeleteQuery {
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), ts)
	}
}

// DeleteForReal will set the force delete flag to really remove
// items.
func DeleteForReal() DeleteCriteria {
	return func(q *bun.DeleteQuery) *bun.DeleteQuery {
		return q.ForceDelete()
	}
}
