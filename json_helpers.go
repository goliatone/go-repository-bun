package repository

import (
	"fmt"

	"github.com/uptrace/bun"
)

// WhereJSONContains applies a JSON containment check on the provided expression.
// expr should be a JSON expression (e.g., metadata->'key' or json_extract(...)).
func WhereJSONContains(expr string, value any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		safeExpr, ok := normalizeJSONExpression(expr)
		if !ok {
			return q.Where("1=0")
		}
		return q.Where(fmt.Sprintf("%s @> ?", safeExpr), value)
	}
}

// OrderByJSONText orders by a text-extracting JSON expression.
// expr should return text (e.g., metadata->>'key' or JSON_UNQUOTE(JSON_EXTRACT(...))).
func OrderByJSONText(expr, direction string) SelectCriteria {
	safeDirection, ok := normalizeOrderDirection(direction)
	if !ok {
		safeDirection = "ASC"
	}
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		safeExpr, ok := normalizeJSONExpression(expr)
		if !ok {
			return q
		}
		return q.Order(fmt.Sprintf("%s %s", safeExpr, safeDirection))
	}
}
