package repository

import (
	"regexp"
	"strings"
	"unicode"
)

var (
	sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	sqlJSONExprPattern   = regexp.MustCompile(`^[A-Za-z0-9_.$,\(\)\[\]'":>\-]+$`)
)

var comparisonOperators = map[string]struct{}{
	"=":                    {},
	"!=":                   {},
	"<>":                   {},
	">":                    {},
	">=":                   {},
	"<":                    {},
	"<=":                   {},
	"LIKE":                 {},
	"ILIKE":                {},
	"NOT LIKE":             {},
	"NOT ILIKE":            {},
	"IS":                   {},
	"IS NOT":               {},
	"IS DISTINCT FROM":     {},
	"IS NOT DISTINCT FROM": {},
}

var orderDirections = map[string]struct{}{
	"ASC":              {},
	"DESC":             {},
	"ASC NULLS FIRST":  {},
	"ASC NULLS LAST":   {},
	"DESC NULLS FIRST": {},
	"DESC NULLS LAST":  {},
}

func normalizeSQLIdentifier(identifier string) (string, bool) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", false
	}

	parts := strings.Split(identifier, ".")
	for _, part := range parts {
		if !sqlIdentifierPattern.MatchString(part) {
			return "", false
		}
	}

	return strings.Join(parts, "."), true
}

func normalizeComparisonOperator(operator string) (string, bool) {
	operator = normalizeSQLOperator(operator)
	if _, ok := comparisonOperators[operator]; !ok {
		return "", false
	}
	return operator, true
}

func normalizeOrderDirection(direction string) (string, bool) {
	direction = normalizeSQLOperator(direction)
	if _, ok := orderDirections[direction]; !ok {
		return "", false
	}
	return direction, true
}

func normalizeSQLOperator(operator string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(operator)), " "))
}

func normalizeOrderExpr(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false
	}

	parts := strings.Fields(expr)
	if len(parts) == 0 || len(parts) > 4 {
		return "", false
	}

	column, ok := normalizeSQLIdentifier(parts[0])
	if !ok {
		return "", false
	}

	if len(parts) == 1 {
		return column, true
	}

	direction, ok := normalizeOrderDirection(strings.Join(parts[1:], " "))
	if !ok {
		return "", false
	}

	return column + " " + direction, true
}

func normalizeSubquery(query string) (string, bool) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", false
	}

	lower := strings.ToLower(query)
	if !strings.HasPrefix(lower, "select ") {
		return "", false
	}

	if strings.Contains(query, ";") || strings.Contains(lower, "--") || strings.Contains(lower, "/*") || strings.Contains(lower, "*/") {
		return "", false
	}

	return query, true
}

func normalizeJSONExpression(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false
	}

	if strings.Contains(expr, ";") {
		return "", false
	}

	for _, r := range expr {
		if unicode.IsSpace(r) {
			return "", false
		}
	}

	if !sqlJSONExprPattern.MatchString(expr) {
		return "", false
	}

	return expr, true
}
