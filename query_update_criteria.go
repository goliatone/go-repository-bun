package repository

import (
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// UpdateRawProcessor will execute the passed in function
func UpdateRawProcessor(fn func(q *bun.UpdateQuery) *bun.UpdateQuery) UpdateCriteria {
	return fn
}

// UpdateBy will select by the given column where: column operator value
// id = 23 or id <= 23
func UpdateBy(column, operator, value string) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), value)
	}
}

// UpdateByTimetz will take a time value and format for postgres
func UpdateByTimetz(column, operator string, value time.Time) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), ts)
	}
}

// UpdateMaybeByTimetz will only update the select criteria if value is defined
func UpdateMaybeByTimetz(column, operator string, value *time.Time) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		if value == nil || value.IsZero() {
			return q
		}
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), ts)
	}
}

// UpdateOrBy OR selector
func UpdateOrBy(column, operator, value string) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), value)
	}
}

// UpdateOrIsNull OR IS NULL
func UpdateOrIsNull(column string) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NULL", column))
	}
}

// UpdateByID using ID
func UpdateByID(id string) UpdateCriteria {
	return UpdateBy("id", "=", id)
}

// UpdateDeletedOnly will include deleted only
func UpdateDeletedOnly() UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.WhereDeleted()
	}
}

// UpdateDeletedAlso will include deleted and non deleted
func UpdateDeletedAlso() UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.WhereAllWithDeleted()
	}
}

// UpdateColumns will select columns
func UpdateColumns(columns ...string) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.Column(columns...)
	}
}

// UpdateExcludeColumns will select columns
func UpdateExcludeColumns(columns ...string) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.ExcludeColumn(columns...)
	}
}

// UpdateSetColumn will set the column to be updated
func UpdateSetColumn(col string, val any) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.SetColumn(col, "?", val)
	}
}
