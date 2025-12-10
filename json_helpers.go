package repository

import (
	"fmt"

	"github.com/uptrace/bun"
)

// WhereJSONContains applies a JSON containment check on the provided expression.
// expr should be a JSON expression (e.g., metadata->'key' or json_extract(...)).
func WhereJSONContains(expr string, value any) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where(fmt.Sprintf("%s @> ?", expr), value)
	}
}

// OrderByJSONText orders by a text-extracting JSON expression.
// expr should return text (e.g., metadata->>'key' or JSON_UNQUOTE(JSON_EXTRACT(...))).
func OrderByJSONText(expr, direction string) SelectCriteria {
	if direction == "" {
		direction = "ASC"
	}
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Order(fmt.Sprintf("%s %s", expr, direction))
	}
}
