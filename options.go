package repository

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/uptrace/bun"
)

type Option func(*bun.DB)

// QueryHookKeyer allows hooks to provide a stable identity for deduplication.
type QueryHookKeyer interface {
	QueryHookKey() string
}

// WithQueryHooks registers query hooks on the provided bun.DB, skipping duplicates.
func WithQueryHooks(hooks ...bun.QueryHook) Option {
	return func(db *bun.DB) {
		registerQueryHooks(db, hooks...)
	}
}

type hookRegistryEntry struct {
	mu   sync.Mutex
	keys map[string]struct{}
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

	localKeys := make(map[string]struct{}, len(hooks))

	entry.mu.Lock()
	defer entry.mu.Unlock()

	for _, hook := range hooks {
		if hook == nil {
			continue
		}

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
		keys: make(map[string]struct{}),
	}
	actual, _ := hookRegistry.LoadOrStore(db, entry)
	return actual.(*hookRegistryEntry)
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
