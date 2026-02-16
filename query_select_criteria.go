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
		alias := defaultSubqueryAlias(subq)
		if len(aliases) > 0 {
			if override := strings.TrimSpace(aliases[0]); override != "" {
				alias = override
			}
		}
		return sq.TableExpr("(?) AS ?", subq, bun.Ident(alias))
	}
}

func defaultSubqueryAlias(subq *bun.SelectQuery) string {
	if subq == nil {
		return "subquery"
	}

	tableName := strings.TrimSpace(subq.GetTableName())
	if tableName == "" {
		return "subquery"
	}

	fields := strings.Fields(tableName)
	if len(fields) > 0 {
		tableName = fields[0]
	}

	if dot := strings.LastIndex(tableName, "."); dot != -1 {
		tableName = tableName[dot+1:]
	}

	tableName = strings.Trim(tableName, "`\"'")
	if tableName == "" {
		return "subquery"
	}

	return tableName
}

func SelectColumnCompare(col1, operator, col2 string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		left, leftOK := normalizeSQLIdentifier(col1)
		right, rightOK := normalizeSQLIdentifier(col2)
		op, opOK := normalizeComparisonOperator(operator)
		if !leftOK || !rightOK || !opOK {
			return sq.Where("1=0")
		}
		return sq.Where(fmt.Sprintf("?TableAlias.%s %s ?TableAlias.%s", left, op, right))
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
		col, colOK := normalizeSQLIdentifier(column)
		op, opOK := normalizeComparisonOperator(operator)
		if !colOK || !opOK {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", col, op), value)
	}
}

// SelectByTimetz will take a time value and format for postgres
func SelectByTimetz(column, operator string, value time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, colOK := normalizeSQLIdentifier(column)
		op, opOK := normalizeComparisonOperator(operator)
		if !colOK || !opOK {
			return q.Where("1=0")
		}
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", col, op), ts)
	}
}

// SelectMaybeByTimetz will only update the select criteria if value is defined
func SelectMaybeByTimetz(column, operator string, value *time.Time) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		if value == nil || value.IsZero() {
			return q
		}
		col, colOK := normalizeSQLIdentifier(column)
		op, opOK := normalizeComparisonOperator(operator)
		if !colOK || !opOK {
			return q.Where("1=0")
		}
		ts := value.Format(time.RFC3339)
		return q.Where(fmt.Sprintf("?TableAlias.%s %s ?", col, op), ts)
	}
}

// SelectOrBy OR selector
func SelectOrBy(column, operator, value string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, colOK := normalizeSQLIdentifier(column)
		op, opOK := normalizeComparisonOperator(operator)
		if !colOK || !opOK {
			return q
		}
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s %s ?", col, op), value)
	}
}

// OrderBy expression
func OrderBy(expression ...string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		var safe []string
		for _, expr := range expression {
			if normalized, ok := normalizeOrderExpr(expr); ok {
				safe = append(safe, normalized)
			}
		}
		if len(safe) == 0 {
			return q
		}
		return q.Order(safe...)
	}
}

// SelectIsNull IS NULL
func SelectIsNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NULL", col))
	}
}

// SelectOrIsNull OR IS NULL
func SelectOrIsNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q
		}
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NULL", col))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s IS NOT NULL", col))
	}
}

// SelectNotNull adds IS NOT NULL
func SelectOrNotNull(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q
		}
		return q.WhereOr(fmt.Sprintf("?TableAlias.%s IS NOT NULL", col))
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
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q
		}
		return q.Order(fmt.Sprintf("%s %s", col, "DESC"))
	}
}

// SelectOrderAsc sort by column
func SelectOrderAsc(column string) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q
		}
		return q.Order(fmt.Sprintf("%s %s", col, "ASC"))
	}
}

// SelectColumnIn will make an array select.
// - values: It should be a slice i.e. of IDs
func SelectColumnIn[T any](column string, slice []T) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		if len(slice) == 0 {
			return q
		}
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s IN (?)", col), bun.In(slice))
	}
}

// SelectColumnNotIn will make an array select
// - values: It should be a slice i.e. of IDs
func SelectColumnNotIn[T any](column string, slice []T) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		if len(slice) == 0 {
			return q
		}
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s NOT IN (?)", col), bun.In(slice))
	}
}

// SelectColumnInSubq will make an array select
func SelectColumnInSubq(column string, query string, args ...any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, colOK := normalizeSQLIdentifier(column)
		subq, subqOK := normalizeSubquery(query)
		if !colOK || !subqOK {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s IN (?)", col), bun.SafeQuery(subq, args...))
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
	trimmed, queryOK := normalizeSubquery(query)
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		col, colOK := normalizeSQLIdentifier(column)
		if !colOK || !queryOK {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("?TableAlias.%s NOT IN (?)", col), bun.SafeQuery(trimmed, args...))
	}
}

// SelectDistinct will add DISTINCT or DISTINCT ON cols
func SelectDistinct(columns ...string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		if len(columns) == 0 {
			return sq.Distinct()
		}
		safe := make([]string, 0, len(columns))
		for _, column := range columns {
			if normalized, ok := normalizeSQLIdentifier(column); ok {
				safe = append(safe, normalized)
			}
		}
		if len(safe) == 0 {
			return sq.Distinct()
		}
		return sq.DistinctOn(strings.Join(safe, ", "))
	}
}

func SelectGroupBy(columns ...string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		safe := make([]string, 0, len(columns))
		for _, column := range columns {
			if normalized, ok := normalizeSQLIdentifier(column); ok {
				safe = append(safe, normalized)
			}
		}
		if len(safe) == 0 {
			return sq
		}
		return sq.Group(safe...)
	}
}

func SelectHaving(expr string, args ...any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		return sq.Having(expr, args...)
	}
}

func SelectBetween(column string, start, end any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return sq.Where("1=0")
		}
		return sq.Where(fmt.Sprintf("?TableAlias.%s BETWEEN ? AND ?", col), start, end)
	}
}

func SelectTimeRange(column string, start, end time.Time) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return sq.Where("1=0")
		}
		return sq.Where(fmt.Sprintf("?TableAlias.%s >= ? AND ?TableAlias.%s <= ?", col, col), start, end)
	}
}

func SelectILike(column, pattern string) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return sq.Where("1=0")
		}
		return sq.Where(fmt.Sprintf("?TableAlias.%s ILIKE ?", col), pattern)
	}
}

func SelectJSONContains(column string, jsonVal any) SelectCriteria {
	return func(sq *bun.SelectQuery) *bun.SelectQuery {
		col, ok := normalizeSQLIdentifier(column)
		if !ok {
			return sq.Where("1=0")
		}
		// TODO: This is Postgres specific, how to generalize?
		return sq.Where(fmt.Sprintf("?TableAlias.%s @> ?", col), jsonVal)
	}
}
