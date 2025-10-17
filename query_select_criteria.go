package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// SelectPaginate will paginate through a result set
func SelectPaginate(limit, offset int) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Limit(limit).Offset(offset)
	}
}

func SelectSubquery(subq *bun.SelectQuery, aliases ...string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		alias := "subquery"
		if len(aliases) > 0 {
			if a := strings.TrimSpace(aliases[0]); a == "" {
				alias = a
			}
		}
		return sq.TableExpr("(?) AS ?", subq, bun.Ident(alias))
	}
}

func SelectColumnCompare(col1, operator, col2 string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Where(fmt.Sprintf("?TableAlias.%s %s ?TableAlias.%s", col1, operator, col2))
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
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), value)
	}
}

// SelectByTimetz will take a time value and format for postgres
func SelectByTimetz(column, operator string, value time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), ts)
	}
}

// SelectMaybeByTimetz will only update the select criteria if value is defined
func SelectMaybeByTimetz(column, operator string, value *time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		if value == nil || value.IsZero() {
			return q
		}
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), ts)
	}
}

// SelectOrBy OR selector
func SelectOrBy(column, operator, value string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s %s ?", column, operator), value)
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
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NULL", column))
	}
}

// SelectOrIsNull OR IS NULL
func SelectOrIsNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NULL", column))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NOT NULL", column))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectOrNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NOT NULL", column))
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
		return q.Order(fmt.Sprintf("%s %s", column, "DESC"))
	}
}

// SelectOrderAsc sort by column
func SelectOrderAsc(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Order(fmt.Sprintf("%s %s", column, "ASC"))
	}
}

// SelectColumnIn will make an array select.
// - values: It should be a slice i.e. of IDs
func SelectColumnIn[T any](column string, slice []T) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		// fmt.Sprintf("?TableAlias.%s
		return q.Where(fmt.Sprintf("?TableAlias.%s IN (?)", column), bun.In(slice))
	}
}

// SelectColumnNotIn will make an array select
// - values: It should be a slice i.e. of IDs
func SelectColumnNotIn[T any](column string, slice []T) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s NOT IN (?)", column), bun.In(slice))
	}
}

// SelectColumnInSubq will make an array select
func SelectColumnInSubq(column string, query string, args ...any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s IN (?)", column), bun.SafeQuery(query, args...))
	}
}

// SelectColumnNotInSubq will make an array select
// Note that when using `NOT IN` you should ensure that none of the values are NULL:
//
//	   AND email NOT IN (
//		  	SELECT email
//		  	FROM users
//				WHERE email is NOT NULL
//	  )
func SelectColumnNotInSubq(column string, query string, args ...any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("?TableAlias.%s NOT IN (?)", column), bun.SafeQuery(query, args...))
	}
}

// SelectDistinct will add DISTINCT or DISTINCT ON cols
func SelectDistinct(columns ...string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		if len(columns) == 0 {
			return sq.Distinct()
		}
		return sq.DistinctOn(strings.Join(columns, ", "))
	}
}

func SelectGroupBy(columns ...string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Group(columns...)
	}
}

func SelectHaving(expr string, args ...any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Having(expr, args...)
	}
}

func SelectBetween(column string, start, end any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Where(fmt.Sprintf("?TableAlias.%s BETWEEN ? AND ?", column), start, end)
	}
}

func SelectTimeRange(column string, start, end time.Time) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Where(fmt.Sprintf("?TableAlias.%s >= ? AND ?TableAlias.%s <= ?", column, column), start, end)
	}
}

func SelectILike(column, pattern string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Where(fmt.Sprintf("?TableAlias.%s ILIKE ?", column), pattern)
	}
}

func SelectJSONContains(column string, jsonVal any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		// TODO: This is Postgres specific, how to generalize?
		return sq.Where(fmt.Sprintf("?TableAlias.%s @> ?", column), jsonVal)
	}
}
