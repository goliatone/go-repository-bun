package repository

import (
	"context"
	"strings"
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
