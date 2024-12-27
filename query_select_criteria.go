package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

func quote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s
	}
	return "\"" + s + "\""
}

func quoateAll(s ...string) string {
	o := []string{}
	for i, e := range s {
		o[i] = quote(e)
	}
	return strings.Join(o, ", ")
}

// SelectPaginate will paginate through a result set
func SelectPaginate(limit, offset int) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Limit(limit).Offset(offset)
	}
}

// SelectRelation will add a LEFT JOIN relation
func SelectRelation(model string, criteria ...SelectCriteria) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		selector := []func(*bun.SelectQuery) *bun.SelectQuery{}

		for _, sel := range criteria {
			selector = append(selector, sel)
		}

		return q.Relation(model, selector...)
	}
}

// SelectRawProcessor will execute the passed in function
func SelectRawProcessor(fn func(q *bun.SelectQuery) *bun.SelectQuery) SelectCriteria {
	return fn
}

// SelectColumns will select columns
func SelectColumns(columns ...string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Column(columns...)
	}
}

// SelectColumnExpr will select columns
func SelectColumnExpr(expr string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.ColumnExpr(expr)
	}
}

// ExcludeColumns will select columns
func ExcludeColumns(columns ...string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.ExcludeColumn(columns...)
	}
}

// SelectBy will select by the given column where: column operator value
// id = 23 or id <= 23
func SelectBy(column, operator, value string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", quote(column), operator), value)
	}
}

// SelectByTimetz will take a time value and format for postgres
func SelectByTimetz(column, operator string, value time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", quote(column), operator), ts)
	}
}

// SelectMaybeByTimetz will only update the select criteria if value is defined
func SelectMaybeByTimetz(column, operator string, value *time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		if value == nil || value.IsZero() {
			return q
		}
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", quote(column), operator), ts)
	}
}

// SelectOrBy OR selector
func SelectOrBy(column, operator, value string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s %s ?", quote(column), operator), value)
	}
}

// OrderBy expression
func OrderBy(expression ...string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Order(expression...)
	}
}

// SelectIsNull IS NULL
func SelectIsNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NULL", quote(column)))
	}
}

// SelectOrIsNull OR IS NULL
func SelectOrIsNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NULL", quote(column)))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NOT NULL", quote(column)))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectOrNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NOT NULL", quote(column)))
	}
}

// SelectByID using ID
func SelectByID(id string) SelectCriteria {
	return SelectBy("id", "=", id)
}

// SelectDeletedOnly will include deleted only
func SelectDeletedOnly() SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereDeleted()
	}
}

// SelectDeletedAlso will include deleted and non deleted
func SelectDeletedAlso() SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereAllWithDeleted()
	}
}

// SelectOrderDesc sort by column
func SelectOrderDesc(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Order(fmt.Sprintf("%s %s", quote(column), "DESC"))
	}
}

// SelectOrderAsc sort by column
func SelectOrderAsc(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Order(fmt.Sprintf("%s %s", quote(column), "ASC"))
	}
}

// SelectColumnIn will make an array select
func SelectColumnIn(column string, values ...any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("%s IN ?", quote(column)), bun.In(values))
	}
}
