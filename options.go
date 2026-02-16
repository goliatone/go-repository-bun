package repository

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/uptrace/bun"
)

type Option func(*bun.DB)

type RepoOption func(*repoConfig)

type repoConfig struct {
	defaultListPaginationConfigured bool
	defaultListLimit                int
	defaultListOffset               int
	allowFullTableDelete            bool
	recordLookupResolver            any
	recordLookupResolverType        reflect.Type
}

// RecordLookupResolver resolves select criteria used to find an existing record
// during Upsert/GetOrCreate when ID and identifier lookups miss.
type RecordLookupResolver[T any] func(record T) []SelectCriteria

// WithRecordLookupResolver configures an optional custom existing-record lookup.
// A nil resolver disables custom lookup.
func WithRecordLookupResolver[T any](resolver RecordLookupResolver[T]) RepoOption {
	return func(cfg *repoConfig) {
		if cfg == nil {
			return
		}

		if resolver == nil {
			cfg.recordLookupResolver = nil
			cfg.recordLookupResolverType = nil
			return
		}

		cfg.recordLookupResolver = resolver
		cfg.recordLookupResolverType = reflect.TypeOf((*T)(nil)).Elem()
	}
}

// WithDefaultListPagination configures repository-level default pagination.
// Use this during repository initialization.
func WithDefaultListPagination(limit, offset int) RepoOption {
	return func(cfg *repoConfig) {
		if cfg == nil {
			return
		}

		cfg.defaultListPaginationConfigured = true
		cfg.defaultListLimit = limit
		cfg.defaultListOffset = offset
	}
}

// WithAllowFullTableDelete enables DeleteWhere/DeleteMany calls without criteria.
// Defaults to false for safety.
func WithAllowFullTableDelete(enabled bool) RepoOption {
	return func(cfg *repoConfig) {
		if cfg == nil {
			return
		}
		cfg.allowFullTableDelete = enabled
	}
}

// QueryHookKeyer allows hooks to provide a stable identity for deduplication.
type QueryHookKeyer interface {
	QueryHookKey() string
}

// QueryHookErrorHandler handles invalid query hook registrations.
type QueryHookErrorHandler func(db *bun.DB, hook bun.QueryHook, err error)

var (
	ErrQueryHookNil        = errors.New("query hook is nil")
	ErrQueryHookNilPointer = errors.New("query hook is a nil pointer")
)

// LogQueryHookErrorHandler logs invalid hook registrations.
func LogQueryHookErrorHandler(db *bun.DB, hook bun.QueryHook, err error) {
	log.Printf("repository: query hook skipped: %v (type=%T)", err, hook)
}

// PanicQueryHookErrorHandler panics on invalid hook registrations.
func PanicQueryHookErrorHandler(db *bun.DB, hook bun.QueryHook, err error) {
	panic(fmt.Sprintf("repository: query hook error: %v (type=%T)", err, hook))
}

// WithQueryHooks registers query hooks on the provided bun.DB, skipping duplicates.
func WithQueryHooks(hooks ...bun.QueryHook) Option {
	return func(db *bun.DB) {
		registerQueryHooks(db, hooks...)
	}
}

// WithQueryHookErrorHandler sets how invalid hooks are handled for the provided bun.DB.
func WithQueryHookErrorHandler(handler QueryHookErrorHandler) Option {
	return func(db *bun.DB) {
		setQueryHookErrorHandler(db, handler)
	}
}

type hookRegistryEntry struct {
	mu      sync.Mutex
	keys    map[string]struct{}
	handler QueryHookErrorHandler
}

var hookRegistry sync.Map

func registerQueryHooks(db *bun.DB, hooks ...bun.QueryHook) {
	if db == nil || len(hooks) == 0 {
		return
	}

	entry := getHookRegistryEntry(db)
	if entry == nil {
		return
	}

	entry.mu.Lock()
	handler := entry.handler
	entry.mu.Unlock()
	if handler == nil {
		handler = LogQueryHookErrorHandler
	}

	validHooks := make([]bun.QueryHook, 0, len(hooks))
	for _, hook := range hooks {
		if err := validateQueryHook(hook); err != nil {
			handler(db, hook, err)
			continue
		}
		validHooks = append(validHooks, hook)
	}

	if len(validHooks) == 0 {
		return
	}

	localKeys := make(map[string]struct{}, len(validHooks))

	entry.mu.Lock()
	defer entry.mu.Unlock()

	for _, hook := range validHooks {
		if key, ok := queryHookKey(hook); ok {
			if _, seen := localKeys[key]; seen {
				continue
			}
			if _, exists := entry.keys[key]; exists {
				continue
			}
			localKeys[key] = struct{}{}
			entry.keys[key] = struct{}{}
		}

		db.AddQueryHook(hook)
	}
}

func getHookRegistryEntry(db *bun.DB) *hookRegistryEntry {
	if db == nil {
		return nil
	}

	if entry, ok := hookRegistry.Load(db); ok {
		return entry.(*hookRegistryEntry)
	}

	entry := &hookRegistryEntry{
		keys:    make(map[string]struct{}),
		handler: LogQueryHookErrorHandler,
	}
	actual, _ := hookRegistry.LoadOrStore(db, entry)
	return actual.(*hookRegistryEntry)
}

func setQueryHookErrorHandler(db *bun.DB, handler QueryHookErrorHandler) {
	if db == nil {
		return
	}

	entry := getHookRegistryEntry(db)
	if entry == nil {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if handler == nil {
		entry.handler = LogQueryHookErrorHandler
		return
	}

	entry.handler = handler
}

func validateQueryHook(hook bun.QueryHook) error {
	if hook == nil {
		return ErrQueryHookNil
	}

	value := reflect.ValueOf(hook)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return ErrQueryHookNilPointer
	}

	return nil
}

func queryHookKey(hook bun.QueryHook) (string, bool) {
	if hook == nil {
		return "", false
	}

	if keyer, ok := hook.(QueryHookKeyer); ok {
		key := strings.TrimSpace(keyer.QueryHookKey())
		if key != "" {
			return fmt.Sprintf("%T:%s", hook, key), true
		}
	}

	value := reflect.ValueOf(hook)
	if value.Kind() == reflect.Ptr && !value.IsNil() {
		return fmt.Sprintf("%T:%x", hook, value.Pointer()), true
	}

	return "", false
}
