package repository

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/goliatone/go-errors"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type SelectCriteria func(*bun.SelectQuery) *bun.SelectQuery

// UpdateCriteria is the function we use to create select queries
type UpdateCriteria func(*bun.UpdateQuery) *bun.UpdateQuery

// DeleteCriteria is the function we use to create select queries
type DeleteCriteria func(*bun.DeleteQuery) *bun.DeleteQuery

// InsertCriteria is the function we use to create insert queries
type InsertCriteria func(*bun.InsertQuery) *bun.InsertQuery

type ScopeSelectFunc func(context.Context) []SelectCriteria
type ScopeUpdateFunc func(context.Context) []UpdateCriteria
type ScopeInsertFunc func(context.Context) []InsertCriteria
type ScopeDeleteFunc func(context.Context) []DeleteCriteria

type ScopeDefinition struct {
	Select ScopeSelectFunc
	Update ScopeUpdateFunc
	Insert ScopeInsertFunc
	Delete ScopeDeleteFunc
}

type ScopeDefaults struct {
	All    []string
	Select []string
	Update []string
	Insert []string
	Delete []string
}

type Repository[T any] interface {
	Raw(ctx context.Context, sql string, args ...any) ([]T, error)
	RawTx(ctx context.Context, tx bun.IDB, sql string, args ...any) ([]T, error)
	Get(ctx context.Context, criteria ...SelectCriteria) (T, error)
	GetTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) (T, error)
	GetByID(ctx context.Context, id string, criteria ...SelectCriteria) (T, error)
	GetByIDTx(ctx context.Context, tx bun.IDB, id string, criteria ...SelectCriteria) (T, error)
	List(ctx context.Context, criteria ...SelectCriteria) ([]T, int, error)
	ListTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) ([]T, int, error)
	Count(ctx context.Context, criteria ...SelectCriteria) (int, error)
	CountTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) (int, error)

	Create(ctx context.Context, record T, criteria ...InsertCriteria) (T, error)
	CreateTx(ctx context.Context, tx bun.IDB, record T, criteria ...InsertCriteria) (T, error)
	CreateMany(ctx context.Context, records []T, criteria ...InsertCriteria) ([]T, error)
	CreateManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...InsertCriteria) ([]T, error)

	GetOrCreate(ctx context.Context, record T) (T, error)
	GetOrCreateTx(ctx context.Context, tx bun.IDB, record T) (T, error)
	GetByIdentifier(ctx context.Context, identifier string, criteria ...SelectCriteria) (T, error)
	GetByIdentifierTx(ctx context.Context, tx bun.IDB, identifier string, criteria ...SelectCriteria) (T, error)

	Update(ctx context.Context, record T, criteria ...UpdateCriteria) (T, error)
	UpdateTx(ctx context.Context, tx bun.IDB, record T, criteria ...UpdateCriteria) (T, error)
	UpdateMany(ctx context.Context, records []T, criteria ...UpdateCriteria) ([]T, error)
	UpdateManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...UpdateCriteria) ([]T, error)

	Upsert(ctx context.Context, record T, criteria ...UpdateCriteria) (T, error)
	UpsertTx(ctx context.Context, tx bun.IDB, record T, criteria ...UpdateCriteria) (T, error)
	UpsertMany(ctx context.Context, records []T, criteria ...UpdateCriteria) ([]T, error)
	UpsertManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...UpdateCriteria) ([]T, error)
	// UpsertMany(ctx context.Context, records []T, conflictColumns []string, criteria ...InsertCriteria) ([]T, error)
	// UpsertManyTx(ctx context.Context, tx bun.IDB, records []T, conflictColumns []string, criteria ...InsertCriteria) ([]T, error)

	Delete(ctx context.Context, record T) error
	DeleteTx(ctx context.Context, tx bun.IDB, record T) error
	DeleteMany(ctx context.Context, criteria ...DeleteCriteria) error
	DeleteManyTx(ctx context.Context, tx bun.IDB, criteria ...DeleteCriteria) error

	DeleteWhere(ctx context.Context, criteria ...DeleteCriteria) error
	DeleteWhereTx(ctx context.Context, tx bun.IDB, criteria ...DeleteCriteria) error
	ForceDelete(ctx context.Context, record T) error
	ForceDeleteTx(ctx context.Context, tx bun.IDB, record T) error

	Handlers() ModelHandlers[T]
	RegisterScope(name string, scope ScopeDefinition)
	SetScopeDefaults(defaults ScopeDefaults) error
	GetScopeDefaults() ScopeDefaults
}

type Meta[T any] interface {
	TableName() string
}

// DefaultListPaginationConfigurer is an optional capability for repository instances that support
// mutating default list pagination at runtime.
//
// Prefer configuring defaults at initialization via NewRepositoryWithConfig(..., WithDefaultListPagination(...)).
// Runtime mutation in live concurrent systems can lead to mixed pagination behavior across requests.
type DefaultListPaginationConfigurer interface {
	SetDefaultListPagination(limit, offset int)
}

type repo[T any] struct {
	db       *bun.DB
	handlers ModelHandlers[T]
	fields   map[reflect.Type][]ModelField
	driver   string
	fieldsMu sync.Mutex
	scopes   map[string]ScopeDefinition
	scopesMu sync.RWMutex

	scopeDefaults ScopeDefaults

	listDefaultsMu               sync.RWMutex
	defaultListPaginationEnabled bool
	defaultListLimit             int
	defaultListOffset            int
}

func (r *repo[T]) resetScopes() {
	r.scopesMu.Lock()
	defer r.scopesMu.Unlock()
	r.scopes = nil
	r.scopeDefaults = ScopeDefaults{}
}

type ModelHandlers[T any] struct {
	NewRecord     func() T
	GetID         func(T) uuid.UUID
	SetID         func(T, uuid.UUID)
	GetIdentifier func() string
	// GetIdentifierValue returns the value for the identifier column.
	// Return an empty string to indicate that the identifier is not available.
	GetIdentifierValue func(T) string
	// ResolveIdentifier allows callers to customize how identifiers are resolved.
	// It can inspect the provided identifier and return one or more IdentifierOptions
	// to try (e.g., try email, username, and id for flexible lookups). Returning nil
	// or an empty slice falls back to GetIdentifier/GetIdentifierValue.
	ResolveIdentifier func(identifier string) []IdentifierOption
}

// IdentifierOption describes a single identifier lookup attempt.
type IdentifierOption struct {
	Column string
	Value  string
}

const (
	legacyDefaultListLimit  = 25
	legacyDefaultListOffset = 0
)

func NewRepository[T any](db *bun.DB, handlers ModelHandlers[T]) Repository[T] {
	return NewRepositoryWithOptions(db, handlers)
}

func NewRepositoryWithOptions[T any](db *bun.DB, handlers ModelHandlers[T], opts ...Option) Repository[T] {
	return NewRepositoryWithConfig(
		db,
		handlers,
		opts,
		WithDefaultListPagination(legacyDefaultListLimit, legacyDefaultListOffset),
	)
}

func NewRepositoryWithConfig[T any](db *bun.DB, handlers ModelHandlers[T], dbOpts []Option, repoOpts ...RepoOption) Repository[T] {
	for _, opt := range dbOpts {
		if opt == nil {
			continue
		}
		opt(db)
	}

	cfg := &repoConfig{}
	for _, opt := range repoOpts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	instance := &repo[T]{
		db:       db,
		handlers: handlers,
		driver:   DetectDriver(db),
	}

	if cfg.defaultListPaginationConfigured {
		instance.SetDefaultListPagination(cfg.defaultListLimit, cfg.defaultListOffset)
	}

	return instance
}

func MustNewRepository[T any](db *bun.DB, handlers ModelHandlers[T]) Repository[T] {
	if err := validateRepositoryConfig(db, handlers); err != nil {
		panic(err)
	}

	return NewRepository(db, handlers)
}

func MustNewRepositoryWithOptions[T any](db *bun.DB, handlers ModelHandlers[T], opts ...Option) Repository[T] {
	if err := validateRepositoryConfig(db, handlers); err != nil {
		panic(err)
	}

	return NewRepositoryWithOptions(db, handlers, opts...)
}

func MustNewRepositoryWithConfig[T any](db *bun.DB, handlers ModelHandlers[T], dbOpts []Option, repoOpts ...RepoOption) Repository[T] {
	if err := validateRepositoryConfig(db, handlers); err != nil {
		panic(err)
	}

	return NewRepositoryWithConfig(db, handlers, dbOpts, repoOpts...)
}

func (r *repo[T]) Validate() error {
	return validateRepositoryConfig(r.db, r.handlers)
}

func (r *repo[T]) MustValidate() {
	if err := r.Validate(); err != nil {
		panic(err)
	}
}

func (r *repo[T]) mapError(err error) error {
	if err == nil {
		return nil
	}

	if errors.IsWrapped(err) {
		return err
	}

	return MapDatabaseError(err, r.driver)
}

func (r *repo[T]) GetModelFields() []ModelField {
	r.fieldsMu.Lock()
	defer r.fieldsMu.Unlock()

	record := r.handlers.NewRecord()
	if isNilValue(any(record)) {
		panic("repository: handlers.NewRecord returned nil; cannot determine model fields")
	}

	modelType := reflect.TypeOf(record)

	if r.fields == nil {
		r.fields = make(map[reflect.Type][]ModelField)
	}

	if fields, ok := r.fields[modelType]; ok {
		return fields
	}

	fields := GetModelFields(r.db, record)
	r.fields[modelType] = fields
	return fields
}

func (r *repo[T]) ResetModelFields() {
	r.fieldsMu.Lock()
	defer r.fieldsMu.Unlock()
	r.fields = nil
}

func (r *repo[T]) Raw(ctx context.Context, sql string, args ...any) ([]T, error) {
	return r.RawTx(ctx, r.db, sql, args...)
}

func (r *repo[T]) RawTx(ctx context.Context, tx bun.IDB, sql string, args ...any) ([]T, error) {
	records := []T{}

	if err := tx.NewRaw(sql, args...).Scan(ctx, &records); err != nil {
		return nil, r.mapError(err)
	}

	return records, nil
}

func (r *repo[T]) Handlers() ModelHandlers[T] {
	return r.handlers
}

func (r *repo[T]) DB() *bun.DB {
	return r.db
}

func (r *repo[T]) RegisterScope(name string, scope ScopeDefinition) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}

	r.scopesMu.Lock()
	if r.scopes == nil {
		r.scopes = make(map[string]ScopeDefinition)
	}
	r.scopes[name] = scope
	r.scopesMu.Unlock()
}

func (r *repo[T]) SetScopeDefaults(defaults ScopeDefaults) error {
	r.scopesMu.Lock()
	defer r.scopesMu.Unlock()

	if err := r.validateScopeDefaultsLocked(defaults); err != nil {
		return err
	}

	r.scopeDefaults = CloneScopeDefaults(defaults)
	return nil
}

func (r *repo[T]) GetScopeDefaults() ScopeDefaults {
	r.scopesMu.RLock()
	defer r.scopesMu.RUnlock()

	return CloneScopeDefaults(r.scopeDefaults)
}

// SetDefaultListPagination mutates default list pagination for this repository instance.
// Prefer configuring this at initialization via NewRepositoryWithConfig(..., WithDefaultListPagination(...))
// and avoid changing it at runtime in live systems: even though writes are synchronized, callers can observe
// different defaults across concurrent requests.
func (r *repo[T]) SetDefaultListPagination(limit, offset int) {
	r.listDefaultsMu.Lock()
	defer r.listDefaultsMu.Unlock()

	if limit <= 0 {
		r.defaultListPaginationEnabled = false
		r.defaultListLimit = 0
		r.defaultListOffset = 0
		return
	}

	if offset < 0 {
		offset = 0
	}

	r.defaultListPaginationEnabled = true
	r.defaultListLimit = limit
	r.defaultListOffset = offset
}

func (r *repo[T]) defaultListPagination() (int, int, bool) {
	r.listDefaultsMu.RLock()
	defer r.listDefaultsMu.RUnlock()

	if !r.defaultListPaginationEnabled {
		return 0, 0, false
	}

	return r.defaultListLimit, r.defaultListOffset, true
}

func (r *repo[T]) Get(ctx context.Context, criteria ...SelectCriteria) (T, error) {
	return r.GetTx(ctx, r.db, criteria...)
}

func (r *repo[T]) GetTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) (T, error) {
	record := r.handlers.NewRecord()
	q := tx.NewSelect().Model(record)

	q = r.applySelectScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}

	if err := q.Limit(1).Scan(ctx); err != nil {
		var zero T
		return zero, r.mapError(err)
	}
	return record, nil
}

func (r *repo[T]) GetByID(ctx context.Context, id string, criteria ...SelectCriteria) (T, error) {
	return r.GetByIDTx(ctx, r.db, id, criteria...)
}

func (r *repo[T]) GetByIDTx(ctx context.Context, tx bun.IDB, id string, criteria ...SelectCriteria) (T, error) {
	criteria = append([]SelectCriteria{SelectByID(id)}, criteria...)
	return r.GetTx(ctx, tx, criteria...)
}

func (r *repo[T]) List(ctx context.Context, criteria ...SelectCriteria) ([]T, int, error) {
	return r.ListTx(ctx, r.db, criteria...)
}

func (r *repo[T]) ListTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) ([]T, int, error) {
	records := []T{}

	q := tx.NewSelect().
		Model(&records)

	if limit, offset, ok := r.defaultListPagination(); ok {
		q.Limit(limit).Offset(offset)
	}

	q = r.applySelectScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}

	var total int
	var err error

	if total, err = q.ScanAndCount(ctx); err != nil {
		return nil, total, r.mapError(err)
	}

	return records, total, nil
}

func (r *repo[T]) Count(ctx context.Context, criteria ...SelectCriteria) (int, error) {
	return r.CountTx(ctx, r.db, criteria...)
}

func (r *repo[T]) CountTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) (int, error) {
	record := r.handlers.NewRecord()

	q := tx.NewSelect().
		Model(record)

	q = r.applySelectScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}

	var total int
	var err error

	if total, err = q.Count(ctx); err != nil {
		return total, r.mapError(err)
	}

	return total, nil
}

func (r *repo[T]) Create(ctx context.Context, record T, criteria ...InsertCriteria) (T, error) {
	return r.CreateTx(ctx, r.db, record, criteria...)
}

func (r *repo[T]) CreateTx(ctx context.Context, tx bun.IDB, record T, criteria ...InsertCriteria) (T, error) {
	id := r.handlers.GetID(record)
	if id == uuid.Nil {
		newID := uuid.New()
		r.handlers.SetID(record, newID)
	}
	q := tx.NewInsert().Model(record)

	q = r.applyInsertScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}

	// TODO: what would be the proper way to getting the returned records from the insert?
	_, err := q.Returning("*").Exec(ctx)
	if err != nil {
		var zero T
		return zero, r.mapError(err)
	}
	return record, nil
}

func (r *repo[T]) CreateMany(ctx context.Context, records []T, criteria ...InsertCriteria) ([]T, error) {
	return r.CreateManyTx(ctx, r.db, records, criteria...)
}

func (r *repo[T]) CreateManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...InsertCriteria) ([]T, error) {
	reorderByID, insertCriteria := splitInsertCriteriaForReturnOrder(criteria)
	if len(records) == 0 {
		return nil, nil
	}

	for _, record := range records {
		id := r.handlers.GetID(record)
		if id == uuid.Nil {
			newID := uuid.New()
			r.handlers.SetID(record, newID)
		}
	}

	var order []uuid.UUID
	if reorderByID {
		order = make([]uuid.UUID, len(records))
		for i, record := range records {
			order[i] = r.handlers.GetID(record)
		}
	}

	q := tx.NewInsert().Model(&records)

	q = r.applyInsertScopes(ctx, q)

	for _, c := range insertCriteria {
		q.Apply(c)
	}

	_, err := q.Returning("*").Exec(ctx)
	if err != nil {
		return records, r.mapError(fmt.Errorf("create many error: %w", err))
	}
	if reorderByID {
		if reordered, ok := reorderRecordsByID(records, order, r.handlers.GetID); ok {
			return reordered, nil
		}
	}
	return records, nil
}

func splitInsertCriteriaForReturnOrder(criteria []InsertCriteria) (bool, []InsertCriteria) {
	if len(criteria) == 0 {
		return false, criteria
	}

	markerPtr := reflect.ValueOf(insertReturnOrderByIDMarker).Pointer()
	filtered := make([]InsertCriteria, 0, len(criteria))
	reorder := false

	for _, c := range criteria {
		if c == nil {
			filtered = append(filtered, c)
			continue
		}
		if reflect.ValueOf(c).Pointer() == markerPtr {
			reorder = true
			continue
		}
		filtered = append(filtered, c)
	}

	if !reorder {
		return false, criteria
	}
	return true, filtered
}

func reorderRecordsByID[T any](records []T, order []uuid.UUID, getID func(T) uuid.UUID) ([]T, bool) {
	if len(records) == 0 {
		return records, true
	}
	if getID == nil {
		return records, false
	}
	if len(order) != len(records) {
		return records, false
	}

	indexes := make(map[uuid.UUID]int, len(order))
	for i, id := range order {
		if id == uuid.Nil {
			return records, false
		}
		if _, exists := indexes[id]; exists {
			return records, false
		}
		indexes[id] = i
	}

	reordered := make([]T, len(records))
	filled := make([]bool, len(records))
	for _, record := range records {
		id := getID(record)
		if id == uuid.Nil {
			return records, false
		}
		idx, ok := indexes[id]
		if !ok {
			return records, false
		}
		if filled[idx] {
			return records, false
		}
		reordered[idx] = record
		filled[idx] = true
	}

	for _, ok := range filled {
		if !ok {
			return records, false
		}
	}

	return reordered, true
}

func splitUpdateCriteriaForReturnOrder(criteria []UpdateCriteria) (bool, []UpdateCriteria) {
	if len(criteria) == 0 {
		return false, criteria
	}

	markerPtr := reflect.ValueOf(updateReturnOrderByIDMarker).Pointer()
	filtered := make([]UpdateCriteria, 0, len(criteria))
	reorder := false

	for _, c := range criteria {
		if c == nil {
			filtered = append(filtered, c)
			continue
		}
		if reflect.ValueOf(c).Pointer() == markerPtr {
			reorder = true
			continue
		}
		filtered = append(filtered, c)
	}

	if !reorder {
		return false, criteria
	}
	return true, filtered
}

func (r *repo[T]) findExistingRecord(ctx context.Context, tx bun.IDB, record T) (T, bool, error) {
	var zero T

	if r.handlers.GetID != nil {
		if id := r.handlers.GetID(record); id != uuid.Nil {
			existing, err := r.GetByIDTx(ctx, tx, id.String())
			if err == nil {
				return existing, true, nil
			}
			if !IsRecordNotFound(err) {
				return zero, false, err
			}
		}
	}

	if r.handlers.GetIdentifierValue != nil {
		if value := strings.TrimSpace(r.handlers.GetIdentifierValue(record)); value != "" {
			existing, err := r.GetByIdentifierTx(ctx, tx, value)
			if err == nil {
				return existing, true, nil
			}
			if !IsRecordNotFound(err) {
				return zero, false, err
			}
		}
	}

	return zero, false, nil
}

func (r *repo[T]) GetOrCreate(ctx context.Context, record T) (T, error) {
	return r.GetOrCreateTx(ctx, r.db, record)
}

func (r *repo[T]) GetOrCreateTx(ctx context.Context, tx bun.IDB, record T) (T, error) {
	existing, found, err := r.findExistingRecord(ctx, tx, record)
	if err != nil {
		var zero T
		return zero, r.mapError(err)
	}

	if found {
		return existing, nil
	}

	created, err := r.CreateTx(ctx, tx, record)
	if err != nil {
		if IsDuplicatedKey(err) {
			existing, found, lookupErr := r.findExistingRecord(ctx, tx, record)
			if lookupErr != nil {
				var zero T
				return zero, lookupErr
			}
			if found {
				return existing, nil
			}
		}
		var zero T
		return zero, err
	}

	return created, nil
}

func (r *repo[T]) GetByIdentifier(ctx context.Context, identifier string, criteria ...SelectCriteria) (T, error) {
	return r.GetByIdentifierTx(ctx, r.db, identifier, criteria...)
}

func (r *repo[T]) resolveIdentifierOptions(identifier string) []IdentifierOption {
	trimmed := strings.TrimSpace(identifier)
	var options []IdentifierOption

	if r.handlers.ResolveIdentifier != nil {
		for _, opt := range r.handlers.ResolveIdentifier(identifier) {
			column := strings.TrimSpace(opt.Column)
			if column == "" {
				continue
			}

			value := strings.TrimSpace(opt.Value)
			if value == "" {
				value = trimmed
			}
			if value == "" {
				continue
			}

			options = append(options, IdentifierOption{
				Column: column,
				Value:  value,
			})
		}
	}

	if len(options) == 0 {
		column := "id"
		if r.handlers.GetIdentifier != nil {
			if col := strings.TrimSpace(r.handlers.GetIdentifier()); col != "" {
				column = col
			}
		}

		options = append(options, IdentifierOption{
			Column: column,
			Value:  trimmed,
		})
	}

	return options
}

func (r *repo[T]) GetByIdentifierTx(ctx context.Context, tx bun.IDB, identifier string, criteria ...SelectCriteria) (T, error) {
	var zero T
	var lastErr error

	options := r.resolveIdentifierOptions(identifier)

	for _, opt := range options {
		record := r.handlers.NewRecord()

		q := tx.NewSelect().Model(record)
		q = r.applySelectScopes(ctx, q)

		for _, c := range criteria {
			q.Apply(c)
		}

		q = q.Where(fmt.Sprintf("?TableAlias.%s = ?", opt.Column), opt.Value).Limit(1)

		if err := q.Scan(ctx); err != nil {
			if IsRecordNotFound(err) {
				lastErr = err
				continue
			}
			return zero, r.mapError(err)
		}

		return record, nil
	}

	if lastErr == nil {
		lastErr = sql.ErrNoRows
	}

	return zero, r.mapError(lastErr)
}

func (r *repo[T]) Update(ctx context.Context, record T, criteria ...UpdateCriteria) (T, error) {
	return r.UpdateTx(ctx, r.db, record, criteria...)
}

func (r *repo[T]) UpdateTx(ctx context.Context, tx bun.IDB, record T, criteria ...UpdateCriteria) (T, error) {
	q := tx.NewUpdate().Model(record)

	q = r.applyUpdateScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}
	// TODO: WherePK will auto generate "ws"."id" = '44a3e9dc-0381-37a6-9652-99ea14057af5'
	// so we can call it with model having the ID and we don't need criteria
	res, err := q.WherePK().Returning("*").Exec(ctx)

	if err != nil {
		var zero T
		return zero, r.mapError(err)
	}

	if err = SQLExpectedCount(res, 1); err != nil {
		var zero T
		return zero, err
	}

	return record, nil
}

func (r *repo[T]) UpdateMany(ctx context.Context, records []T, criteria ...UpdateCriteria) ([]T, error) {
	return r.UpdateManyTx(ctx, r.db, records, criteria...)
}

func (r *repo[T]) UpdateManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...UpdateCriteria) ([]T, error) {
	reorderByID, updateCriteria := splitUpdateCriteriaForReturnOrder(criteria)
	if len(records) == 0 {
		return nil, nil
	}

	var order []uuid.UUID
	if reorderByID {
		order = make([]uuid.UUID, len(records))
		for i, record := range records {
			order[i] = r.handlers.GetID(record)
		}
	}

	q := tx.NewUpdate().Model(&records).Bulk()

	q = r.applyUpdateScopes(ctx, q)

	for _, c := range updateCriteria {
		q.Apply(c)
	}

	_, err := q.
		WherePK().
		Returning("*").
		Exec(ctx)

	if err != nil {
		var zero []T
		return zero, r.mapError(err)
	}

	if reorderByID {
		if reordered, ok := reorderRecordsByID(records, order, r.handlers.GetID); ok {
			return reordered, nil
		}
	}

	return records, nil
}

func (r *repo[T]) Upsert(ctx context.Context, record T, criteria ...UpdateCriteria) (T, error) {
	return r.UpsertTx(ctx, r.db, record, criteria...)
}

func (r *repo[T]) UpsertTx(ctx context.Context, tx bun.IDB, record T, criteria ...UpdateCriteria) (T, error) {
	existing, found, err := r.findExistingRecord(ctx, tx, record)
	if err != nil {
		var zero T
		return zero, r.mapError(err)
	}

	if found {
		r.handlers.SetID(record, r.handlers.GetID(existing))
		return r.UpdateTx(ctx, tx, record, criteria...)
	}

	return r.CreateTx(ctx, tx, record)
}

func (r *repo[T]) UpsertMany(ctx context.Context, records []T, criteria ...UpdateCriteria) ([]T, error) {
	return r.UpsertManyTx(ctx, r.db, records, criteria...)
}

func (r *repo[T]) UpsertManyTx(ctx context.Context, tx bun.IDB, records []T, criteria ...UpdateCriteria) ([]T, error) {
	var upsertedRecords []T

	for _, record := range records {
		existing, found, err := r.findExistingRecord(ctx, tx, record)
		if err != nil {
			return nil, r.mapError(err)
		}

		if found {
			r.handlers.SetID(record, r.handlers.GetID(existing))
			updatedRecord, updateErr := r.UpdateTx(ctx, tx, record, criteria...)
			if updateErr != nil {
				return nil, r.mapError(updateErr)
			}
			upsertedRecords = append(upsertedRecords, updatedRecord)
			continue
		}

		createdRecord, createErr := r.CreateTx(ctx, tx, record)
		if createErr != nil {
			return nil, r.mapError(createErr)
		}
		upsertedRecords = append(upsertedRecords, createdRecord)
	}

	return upsertedRecords, nil
}

// func (r *repo[T]) UpsertMany(ctx context.Context, records []T, conflictColumns []string, criteria ...InsertCriteria) ([]T, error) {
// 	return r.UpsertManyTx(ctx, r.db, records, conflictColumns, criteria...)
// }

// func (r *repo[T]) UpsertManyTx(ctx context.Context, tx bun.IDB, records []T, conflictColumns []string, criteria ...InsertCriteria) ([]T, error) {
// 	if len(records) == 0 {
// 		return nil, nil
// 	}

// 	if len(conflictColumns) == 0 {
// 		conflictColumns = []string{"id"}
// 	}

// 	conflictClause := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE", strings.Join(conflictColumns, ", "))

// 	q := tx.NewInsert().Model(&records).On(conflictClause)

// 	// Apply each UpdateCriteria to the query
// 	for _, c := range criteria {
// 		q.Apply(c)
// 	}

// 	// Execute the query with Returning to fetch updated/created records
// 	_, err := q.Returning("*").Exec(ctx)
// 	if err != nil {
// 		var zero []T
// 		return zero, err
// 	}

// 	// Return the upserted records
// 	return records, nil
// }

func (r *repo[T]) Delete(ctx context.Context, record T) error {
	return r.DeleteTx(ctx, r.db, record)
}

func (r *repo[T]) DeleteTx(ctx context.Context, tx bun.IDB, record T) error {
	q := tx.NewDelete().Model(record).WherePK()

	q = r.applyDeleteScopes(ctx, q)

	_, err := q.Exec(ctx)
	return r.mapError(err)
}

func (r *repo[T]) DeleteMany(ctx context.Context, criteria ...DeleteCriteria) error {
	return r.DeleteManyTx(ctx, r.db, criteria...)
}

func (r *repo[T]) DeleteManyTx(ctx context.Context, tx bun.IDB, criteria ...DeleteCriteria) error {
	return r.DeleteWhereTx(ctx, tx, criteria...)
}

func (r *repo[T]) DeleteWhere(ctx context.Context, criteria ...DeleteCriteria) error {
	return r.DeleteWhereTx(ctx, r.db, criteria...)
}

func (r *repo[T]) DeleteWhereTx(ctx context.Context, tx bun.IDB, criteria ...DeleteCriteria) error {
	record := r.handlers.NewRecord()
	q := tx.NewDelete().Model(record)

	q = r.applyDeleteScopes(ctx, q)

	for _, c := range criteria {
		q.Apply(c)
	}
	_, err := q.Exec(ctx)
	return r.mapError(err)
}

func (r *repo[T]) ForceDelete(ctx context.Context, record T) error {
	return r.ForceDeleteTx(ctx, r.db, record)
}

func (r *repo[T]) ForceDeleteTx(ctx context.Context, tx bun.IDB, record T) error {
	q := tx.NewDelete().Model(record).WherePK().ForceDelete()

	q = r.applyDeleteScopes(ctx, q)

	_, err := q.Exec(ctx)
	return r.mapError(err)
}

func (r *repo[T]) TableName() string {
	var model T
	return r.db.NewCreateTable().Model(model).GetTableName()
}

func (r *repo[T]) applySelectScopes(ctx context.Context, q *bun.SelectQuery) *bun.SelectQuery {
	for _, scope := range r.resolveSelectScopes(ctx) {
		q = scope(q)
	}
	return q
}

func (r *repo[T]) applyUpdateScopes(ctx context.Context, q *bun.UpdateQuery) *bun.UpdateQuery {
	for _, scope := range r.resolveUpdateScopes(ctx) {
		q = scope(q)
	}
	return q
}

func (r *repo[T]) applyInsertScopes(ctx context.Context, q *bun.InsertQuery) *bun.InsertQuery {
	for _, scope := range r.resolveInsertScopes(ctx) {
		q = scope(q)
	}
	return q
}

func (r *repo[T]) applyDeleteScopes(ctx context.Context, q *bun.DeleteQuery) *bun.DeleteQuery {
	for _, scope := range r.resolveDeleteScopes(ctx) {
		q = scope(q)
	}
	return q
}

func (r *repo[T]) resolveSelectScopes(ctx context.Context) []SelectCriteria {
	defs := r.resolveScopeDefinitions(ctx, ScopeOperationSelect)
	var result []SelectCriteria
	for _, def := range defs {
		if def.Select == nil {
			continue
		}
		result = append(result, def.Select(ctx)...)
	}
	return result
}

func (r *repo[T]) resolveUpdateScopes(ctx context.Context) []UpdateCriteria {
	defs := r.resolveScopeDefinitions(ctx, ScopeOperationUpdate)
	var result []UpdateCriteria
	for _, def := range defs {
		if def.Update == nil {
			continue
		}
		result = append(result, def.Update(ctx)...)
	}
	return result
}

func (r *repo[T]) resolveInsertScopes(ctx context.Context) []InsertCriteria {
	defs := r.resolveScopeDefinitions(ctx, ScopeOperationInsert)
	var result []InsertCriteria
	for _, def := range defs {
		if def.Insert == nil {
			continue
		}
		result = append(result, def.Insert(ctx)...)
	}
	return result
}

func (r *repo[T]) resolveDeleteScopes(ctx context.Context) []DeleteCriteria {
	defs := r.resolveScopeDefinitions(ctx, ScopeOperationDelete)
	var result []DeleteCriteria
	for _, def := range defs {
		if def.Delete == nil {
			continue
		}
		result = append(result, def.Delete(ctx)...)
	}
	return result
}

func (r *repo[T]) resolveScopeDefinitions(ctx context.Context, op ScopeOperation) []ScopeDefinition {
	r.scopesMu.RLock()
	defaults := CloneScopeDefaults(r.scopeDefaults)
	defsMap := make(map[string]ScopeDefinition, len(r.scopes))
	maps.Copy(defsMap, r.scopes)
	r.scopesMu.RUnlock()

	state := ResolveScopeState(ctx, defaults, op)

	defs := make([]ScopeDefinition, 0, len(state.Names))
	for _, raw := range state.Names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if def, ok := defsMap[name]; ok {
			defs = append(defs, def)
		}
	}

	return defs
}

func DetectDriver(db *bun.DB) string {
	switch db.Dialect().Name() {
	case dialect.PG:
		return "postgres"
	case dialect.SQLite:
		return "sqlite"
	case dialect.MSSQL:
		return "mssql"
	case dialect.MySQL:
		return "mysql"
	default:
		return "unknown"
	}
}

func validateRepositoryConfig[T any](db *bun.DB, handlers ModelHandlers[T]) error {
	var validationErrors errors.ValidationErrors

	if db == nil {
		validationErrors = append(validationErrors, errors.FieldError{
			Field:   "db",
			Message: "db instance is required",
		})
	}

	if handlers.NewRecord == nil {
		validationErrors = append(validationErrors, errors.FieldError{
			Field:   "handlers.NewRecord",
			Message: "handler is required",
		})
	}

	if handlers.GetID == nil {
		validationErrors = append(validationErrors, errors.FieldError{
			Field:   "handlers.GetID",
			Message: "handler is required",
		})
	}

	if handlers.SetID == nil {
		validationErrors = append(validationErrors, errors.FieldError{
			Field:   "handlers.SetID",
			Message: "handler is required",
		})
	}

	if (handlers.GetIdentifier == nil) != (handlers.GetIdentifierValue == nil) {
		validationErrors = append(validationErrors, errors.FieldError{
			Field:   "handlers",
			Message: "GetIdentifier and GetIdentifierValue must both be provided or both nil",
		})
	}

	if handlers.GetIdentifier != nil {
		column := strings.TrimSpace(handlers.GetIdentifier())
		if column == "" {
			validationErrors = append(validationErrors, errors.FieldError{
				Field:   "handlers.GetIdentifier",
				Message: "must return a non-empty column name",
			})
		}
	}

	if len(validationErrors) > 0 {
		return errors.NewValidation("repository configuration invalid", validationErrors...)
	}

	return nil
}

func (r *repo[T]) validateScopeDefaultsLocked(defaults ScopeDefaults) error {
	unknown := make(map[string]struct{})
	check := func(names []string) {
		for _, raw := range names {
			name := strings.TrimSpace(raw)
			if name == "" {
				continue
			}
			if _, ok := r.scopes[name]; !ok {
				unknown[name] = struct{}{}
			}
		}
	}

	check(defaults.All)
	check(defaults.Select)
	check(defaults.Update)
	check(defaults.Insert)
	check(defaults.Delete)

	if len(unknown) == 0 {
		return nil
	}

	names := make([]string, 0, len(unknown))
	for name := range unknown {
		names = append(names, name)
	}
	sort.Strings(names)

	fieldErrors := make([]errors.FieldError, 0, len(names))
	for _, name := range names {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "scopeDefaults",
			Message: fmt.Sprintf("scope %q is not registered", name),
		})
	}

	return errors.NewValidation("repository: scope defaults reference unknown scopes", fieldErrors...)
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}

	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func:
		return val.IsNil()
	default:
		return false
	}
}
