package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type scopeContextKey struct{}

type scopeContextConfig struct {
	useDefaults bool

	all     []string
	selects []string
	updates []string
	inserts []string
	deletes []string

	data map[string]any
}

// ScopeOperation identifies the repository operation type used when applying scopes.
type ScopeOperation int

const (
	ScopeOperationSelect ScopeOperation = iota
	ScopeOperationUpdate
	ScopeOperationInsert
	ScopeOperationDelete
)

// ScopeState captures the effective scope configuration for a given operation.
type ScopeState struct {
	Operation   ScopeOperation `json:"operation"`
	UseDefaults bool           `json:"use_defaults"`
	Names       []string       `json:"names"`
	Data        map[string]any `json:"data,omitempty"`
}

// IsZero returns true when no scope names or data are configured.
func (s ScopeState) IsZero() bool {
	return len(s.Names) == 0 && len(s.Data) == 0 && s.UseDefaults
}

func WithScopes(ctx context.Context, names ...string) context.Context {
	return withScopeNames(ctx, names, func(cfg *scopeContextConfig) *[]string {
		return &cfg.all
	})
}

func WithSelectScopes(ctx context.Context, names ...string) context.Context {
	return withScopeNames(ctx, names, func(cfg *scopeContextConfig) *[]string {
		return &cfg.selects
	})
}

func WithUpdateScopes(ctx context.Context, names ...string) context.Context {
	return withScopeNames(ctx, names, func(cfg *scopeContextConfig) *[]string {
		return &cfg.updates
	})
}

func WithInsertScopes(ctx context.Context, names ...string) context.Context {
	return withScopeNames(ctx, names, func(cfg *scopeContextConfig) *[]string {
		return &cfg.inserts
	})
}

func WithDeleteScopes(ctx context.Context, names ...string) context.Context {
	return withScopeNames(ctx, names, func(cfg *scopeContextConfig) *[]string {
		return &cfg.deletes
	})
}

func WithScopeData(ctx context.Context, name string, data any) context.Context {
	cfg := cloneScopeContextConfig(scopeConfigFromContext(ctx))
	if cfg.data == nil {
		cfg.data = make(map[string]any)
	}
	cfg.data[strings.TrimSpace(name)] = data
	return context.WithValue(ctx, scopeContextKey{}, cfg)
}

func ScopeData(ctx context.Context, name string) (any, bool) {
	cfg := scopeConfigFromContext(ctx)
	if cfg == nil || cfg.data == nil {
		return nil, false
	}
	val, ok := cfg.data[strings.TrimSpace(name)]
	return val, ok
}

func ScopeDataSnapshot(ctx context.Context) map[string]any {
	cfg := scopeConfigFromContext(ctx)
	if cfg == nil || cfg.data == nil {
		return nil
	}

	snapshot := make(map[string]any, len(cfg.data))
	for k, v := range cfg.data {
		snapshot[k] = v
	}
	return snapshot
}

func WithoutDefaultScopes(ctx context.Context) context.Context {
	cfg := cloneScopeContextConfig(scopeConfigFromContext(ctx))
	cfg.useDefaults = false
	return context.WithValue(ctx, scopeContextKey{}, cfg)
}

func ResolveScopeState(ctx context.Context, defaults ScopeDefaults, op ScopeOperation) ScopeState {
	contextNames, useDefaults := scopeNamesForOperation(ctx, op)

	var names []string
	if useDefaults {
		names = append(names, defaults.All...)
		switch op {
		case ScopeOperationSelect:
			names = append(names, defaults.Select...)
		case ScopeOperationUpdate:
			names = append(names, defaults.Update...)
		case ScopeOperationInsert:
			names = append(names, defaults.Insert...)
		case ScopeOperationDelete:
			names = append(names, defaults.Delete...)
		}
	}

	if len(contextNames) > 0 {
		names = append(names, contextNames...)
	}

	return ScopeState{
		Operation:   op,
		UseDefaults: useDefaults,
		Names:       uniqueStrings(names),
		Data:        ScopeDataSnapshot(ctx),
	}
}

// CloneScopeDefaults creates a deep copy of the provided scope defaults.
func CloneScopeDefaults(defaults ScopeDefaults) ScopeDefaults {
	return ScopeDefaults{
		All:    copyStrings(defaults.All),
		Select: copyStrings(defaults.Select),
		Update: copyStrings(defaults.Update),
		Insert: copyStrings(defaults.Insert),
		Delete: copyStrings(defaults.Delete),
	}
}

func scopeConfigFromContext(ctx context.Context) *scopeContextConfig {
	if ctx == nil {
		return nil
	}
	if cfg, ok := ctx.Value(scopeContextKey{}).(*scopeContextConfig); ok {
		return cfg
	}
	return nil
}

func scopeNamesForOperation(ctx context.Context, op ScopeOperation) ([]string, bool) {
	cfg := scopeConfigFromContext(ctx)
	if cfg == nil {
		return nil, true
	}

	var selection []string
	switch op {
	case ScopeOperationSelect:
		selection = cfg.selects
	case ScopeOperationUpdate:
		selection = cfg.updates
	case ScopeOperationInsert:
		selection = cfg.inserts
	case ScopeOperationDelete:
		selection = cfg.deletes
	default:
		selection = nil
	}

	combined := append([]string{}, cfg.all...)
	if len(selection) > 0 {
		combined = append(combined, selection...)
	}

	return combined, cfg.useDefaults
}

func cloneScopeContextConfig(cfg *scopeContextConfig) *scopeContextConfig {
	if cfg == nil {
		return &scopeContextConfig{
			useDefaults: true,
		}
	}

	cloned := &scopeContextConfig{
		useDefaults: cfg.useDefaults,
		all:         copyStrings(cfg.all),
		selects:     copyStrings(cfg.selects),
		updates:     copyStrings(cfg.updates),
		inserts:     copyStrings(cfg.inserts),
		deletes:     copyStrings(cfg.deletes),
	}

	if cfg.data != nil {
		cloned.data = make(map[string]any, len(cfg.data))
		for k, v := range cfg.data {
			cloned.data[k] = v
		}
	}

	return cloned
}

func withScopeNames(ctx context.Context, names []string, target func(*scopeContextConfig) *[]string) context.Context {
	if len(names) == 0 {
		return ctx
	}

	cfg := cloneScopeContextConfig(scopeConfigFromContext(ctx))
	dest := target(cfg)

	for _, name := range names {
		if trimmed := strings.TrimSpace(name); trimmed != "" && !containsString(*dest, trimmed) {
			*dest = append(*dest, trimmed)
		}
	}

	return context.WithValue(ctx, scopeContextKey{}, cfg)
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func copyStrings(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ScopeByField returns a ScopeDefinition that constrains repository operations to
// records whose `field` matches the value stored under `scopeName` in the context.
// Scope data may be provided as uuid.UUID, *uuid.UUID, or any fmt.Stringer/string
// whose String() value is not blank.
func ScopeByField(scopeName, field string) ScopeDefinition {
	return scopeByField(scopeName, field, true)
}

// ScopeByFieldOptional keeps the legacy fail-open behavior of ScopeByField:
// missing or invalid scope data results in no scope criteria being applied.
func ScopeByFieldOptional(scopeName, field string) ScopeDefinition {
	return scopeByField(scopeName, field, false)
}

// ScopeByFieldRequired behaves like ScopeByField, but it fails closed: if scope
// data is missing or invalid, it applies a no-match criterion.
func ScopeByFieldRequired(scopeName, field string) ScopeDefinition {
	return ScopeByField(scopeName, field)
}

func scopeByField(scopeName, field string, required bool) ScopeDefinition {
	scopeName = strings.TrimSpace(scopeName)
	field = strings.TrimSpace(field)
	if scopeName == "" || field == "" {
		return ScopeDefinition{}
	}

	valueFor := func(ctx context.Context) (string, bool) {
		val, ok := ScopeData(ctx, scopeName)
		if !ok {
			return "", false
		}

		switch v := val.(type) {
		case uuid.UUID:
			if v == uuid.Nil {
				return "", false
			}
			return v.String(), true
		case *uuid.UUID:
			if v == nil || *v == uuid.Nil {
				return "", false
			}
			return v.String(), true
		case fmt.Stringer:
			s := strings.TrimSpace(v.String())
			if s == "" {
				return "", false
			}
			return s, true
		case string:
			s := strings.TrimSpace(v)
			if s == "" {
				return "", false
			}
			return s, true
		default:
			return "", false
		}
	}

	return ScopeDefinition{
		Select: func(ctx context.Context) []SelectCriteria {
			if value, ok := valueFor(ctx); ok {
				return []SelectCriteria{SelectBy(field, "=", value)}
			}
			if required {
				return []SelectCriteria{
					func(q *bun.SelectQuery) *bun.SelectQuery {
						return q.Where("1=0")
					},
				}
			}
			return nil
		},
		Update: func(ctx context.Context) []UpdateCriteria {
			if value, ok := valueFor(ctx); ok {
				return []UpdateCriteria{UpdateBy(field, "=", value)}
			}
			if required {
				return []UpdateCriteria{
					func(q *bun.UpdateQuery) *bun.UpdateQuery {
						return q.Where("1=0")
					},
				}
			}
			return nil
		},
		Delete: func(ctx context.Context) []DeleteCriteria {
			if value, ok := valueFor(ctx); ok {
				return []DeleteCriteria{DeleteBy(field, "=", value)}
			}
			if required {
				return []DeleteCriteria{
					func(q *bun.DeleteQuery) *bun.DeleteQuery {
						return q.Where("1=0")
					},
				}
			}
			return nil
		},
	}
}

// ScopeValues carries the common tenant and organization scope values used by
// callers that need explicit row-level predicates.
type ScopeValues struct {
	TenantID string
	OrgID    string
}

// ScopeColumns identifies the tenant and organization columns for a scoped
// table. Column names are caller-provided so table aliases such as cf.tenant_id
// can be used without the repository package knowing model details.
type ScopeColumns struct {
	Tenant string
	Org    string
}

// ScopePredicateMode selects how ScopeWhere should match scoped rows.
type ScopePredicateMode string

const (
	// ScopeExact matches rows with the exact provided scope values.
	ScopeExact ScopePredicateMode = "exact"
	// ScopeGlobal matches rows where the configured scope columns are blank.
	ScopeGlobal ScopePredicateMode = "global"
	// ScopeExactOrGlobal matches exact scoped rows or explicit global rows.
	ScopeExactOrGlobal ScopePredicateMode = "exact_or_global"
)

// ScopedRecord is a minimal mutation contract for records with tenant/org
// fields. It keeps defaulting independent from any app-specific model type.
type ScopedRecord interface {
	GetTenantID() string
	SetTenantID(string)
	GetOrgID() string
	SetOrgID(string)
}

// ScopeWhere builds a safe SQL predicate and arguments for common tenant/org
// scope matching. Invalid columns fail closed; missing columns return ok=false.
func ScopeWhere(cols ScopeColumns, scope ScopeValues, mode ScopePredicateMode) (expr string, args []any, ok bool) {
	scope = normalizeScopeValues(scope)
	columnPairs := normalizeScopeColumnPairs(cols)
	if len(columnPairs) == 0 {
		return "", nil, false
	}

	switch mode {
	case "", ScopeExact:
		return exactScopeWhere(columnPairs, scope), exactScopeArgs(columnPairs, scope), true
	case ScopeGlobal:
		return globalScopeWhere(columnPairs), nil, true
	case ScopeExactOrGlobal:
		if !hasExactScopeValues(columnPairs, scope) {
			return globalScopeWhere(columnPairs), nil, true
		}
		exact := exactScopeWhere(columnPairs, scope)
		global := globalScopeWhere(columnPairs)
		return fmt.Sprintf("((%s) OR (%s))", exact, global), exactScopeArgs(columnPairs, scope), true
	default:
		return "1=0", nil, true
	}
}

// SelectScope returns a SelectCriteria that applies ScopeWhere.
func SelectScope(cols ScopeColumns, scope ScopeValues, mode ScopePredicateMode) SelectCriteria {
	return func(q *bun.SelectQuery) *bun.SelectQuery {
		return ApplyScope(q, cols, scope, mode)
	}
}

// UpdateScope returns an UpdateCriteria that applies exact scope matching.
func UpdateScope(cols ScopeColumns, scope ScopeValues) UpdateCriteria {
	return func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return ApplyScopeToUpdate(q, cols, scope)
	}
}

// DeleteScope returns a DeleteCriteria that applies exact scope matching.
func DeleteScope(cols ScopeColumns, scope ScopeValues) DeleteCriteria {
	return func(q *bun.DeleteQuery) *bun.DeleteQuery {
		return ApplyScopeToDelete(q, cols, scope)
	}
}

// ApplyScope applies a select predicate generated by ScopeWhere.
func ApplyScope(q *bun.SelectQuery, cols ScopeColumns, scope ScopeValues, mode ScopePredicateMode) *bun.SelectQuery {
	if q == nil {
		return q
	}
	expr, args, ok := ScopeWhere(cols, scope, mode)
	if !ok {
		return q
	}
	return q.Where(expr, args...)
}

// ApplyScopeToUpdate applies exact scope matching to an update query.
func ApplyScopeToUpdate(q *bun.UpdateQuery, cols ScopeColumns, scope ScopeValues) *bun.UpdateQuery {
	if q == nil {
		return q
	}
	expr, args, ok := ScopeWhere(cols, scope, ScopeExact)
	if !ok {
		return q
	}
	return q.Where(expr, args...)
}

// ApplyScopeToDelete applies exact scope matching to a delete query.
func ApplyScopeToDelete(q *bun.DeleteQuery, cols ScopeColumns, scope ScopeValues) *bun.DeleteQuery {
	if q == nil {
		return q
	}
	expr, args, ok := ScopeWhere(cols, scope, ScopeExact)
	if !ok {
		return q
	}
	return q.Where(expr, args...)
}

// DefaultScope fills blank tenant/org record values from scope and preserves
// values already present on the record.
func DefaultScope(record ScopedRecord, scope ScopeValues) bool {
	if record == nil {
		return false
	}
	scope = normalizeScopeValues(scope)

	changed := false
	if strings.TrimSpace(record.GetTenantID()) == "" && scope.TenantID != "" {
		record.SetTenantID(scope.TenantID)
		changed = true
	}
	if strings.TrimSpace(record.GetOrgID()) == "" && scope.OrgID != "" {
		record.SetOrgID(scope.OrgID)
		changed = true
	}
	return changed
}

// DefaultScopeMap fills blank tenant_id/org_id values from scope in a metadata
// map and preserves explicit non-blank values.
func DefaultScopeMap(values map[string]any, scope ScopeValues) bool {
	if values == nil {
		return false
	}
	scope = normalizeScopeValues(scope)

	changed := false
	if isBlankMapScopeValue(values["tenant_id"]) && scope.TenantID != "" {
		values["tenant_id"] = scope.TenantID
		changed = true
	}
	if isBlankMapScopeValue(values["org_id"]) && scope.OrgID != "" {
		values["org_id"] = scope.OrgID
		changed = true
	}
	return changed
}

type scopeColumnPair struct {
	column string
	value  string
}

func normalizeScopeValues(scope ScopeValues) ScopeValues {
	return ScopeValues{
		TenantID: strings.TrimSpace(scope.TenantID),
		OrgID:    strings.TrimSpace(scope.OrgID),
	}
}

func normalizeScopeColumnPairs(cols ScopeColumns) []scopeColumnPair {
	pairs := make([]scopeColumnPair, 0, 2)
	if col, ok := normalizeSQLIdentifier(cols.Tenant); ok {
		pairs = append(pairs, scopeColumnPair{column: col, value: "tenant"})
	}
	if strings.TrimSpace(cols.Tenant) != "" && len(pairs) == 0 {
		return []scopeColumnPair{{column: "1=0", value: "invalid"}}
	}
	if col, ok := normalizeSQLIdentifier(cols.Org); ok {
		pairs = append(pairs, scopeColumnPair{column: col, value: "org"})
	}
	if strings.TrimSpace(cols.Org) != "" {
		lastValid := len(pairs) > 0 && pairs[len(pairs)-1].value == "org"
		if !lastValid && strings.TrimSpace(cols.Tenant) == "" {
			return []scopeColumnPair{{column: "1=0", value: "invalid"}}
		}
		if !lastValid && strings.TrimSpace(cols.Tenant) != "" {
			return []scopeColumnPair{{column: "1=0", value: "invalid"}}
		}
	}
	return pairs
}

func exactScopeWhere(pairs []scopeColumnPair, scope ScopeValues) string {
	if !hasExactScopeValues(pairs, scope) {
		return "1=0"
	}

	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if pair.value == "invalid" {
			return "1=0"
		}
		parts = append(parts, fmt.Sprintf("%s = ?", pair.column))
	}
	return strings.Join(parts, " AND ")
}

func exactScopeArgs(pairs []scopeColumnPair, scope ScopeValues) []any {
	if !hasExactScopeValues(pairs, scope) {
		return nil
	}

	args := make([]any, 0, len(pairs))
	for _, pair := range pairs {
		switch pair.value {
		case "tenant":
			args = append(args, scope.TenantID)
		case "org":
			args = append(args, scope.OrgID)
		}
	}
	return args
}

func globalScopeWhere(pairs []scopeColumnPair) string {
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if pair.value == "invalid" {
			return "1=0"
		}
		parts = append(parts, fmt.Sprintf("(%s IS NULL OR %s = '')", pair.column, pair.column))
	}
	if len(parts) == 0 {
		return "1=0"
	}
	return strings.Join(parts, " AND ")
}

func hasExactScopeValues(pairs []scopeColumnPair, scope ScopeValues) bool {
	for _, pair := range pairs {
		switch pair.value {
		case "tenant":
			if scope.TenantID == "" {
				return false
			}
		case "org":
			if scope.OrgID == "" {
				return false
			}
		default:
			return false
		}
	}
	return len(pairs) > 0
}

func isBlankMapScopeValue(value any) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v) == ""
	case fmt.Stringer:
		return strings.TrimSpace(v.String()) == ""
	default:
		return strings.TrimSpace(fmt.Sprint(v)) == ""
	}
}
