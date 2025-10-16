package repository

import (
	"context"
	"fmt"
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
}

type Meta[T any] interface {
	TableName() string
}

type repo[T any] struct {
	db       *bun.DB
	handlers ModelHandlers[T]
	fields   []ModelField
	driver   string
	fieldsMu sync.Mutex
}

type ModelHandlers[T any] struct {
	NewRecord     func() T
	GetID         func(T) uuid.UUID
	SetID         func(T, uuid.UUID)
	GetIdentifier func() string
	// GetIdentifierValue returns the value for the identifier column.
	// Return an empty string to indicate that the identifier is not available.
	GetIdentifierValue func(T) string
}

func NewRepository[T any](db *bun.DB, handlers ModelHandlers[T]) Repository[T] {
	return &repo[T]{
		db:       db,
		handlers: handlers,
		driver:   DetectDriver(db),
	}
}

func MustNewRepository[T any](db *bun.DB, handlers ModelHandlers[T]) Repository[T] {
	if err := validateRepositoryConfig(db, handlers); err != nil {
		panic(err)
	}

	return NewRepository(db, handlers)
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

	if len(r.fields) == 0 {
		r.fields = GetModelFields(r.db, r.handlers.NewRecord())
	}
	return r.fields
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

func (r *repo[T]) Get(ctx context.Context, criteria ...SelectCriteria) (T, error) {
	return r.GetTx(ctx, r.db, criteria...)
}

func (r *repo[T]) GetTx(ctx context.Context, tx bun.IDB, criteria ...SelectCriteria) (T, error) {
	record := r.handlers.NewRecord()
	q := tx.NewSelect().Model(record)

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

	// Set Limit Offset default values
	// if we apply again in criteria, we override
	q.Limit(25).Offset(0)

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

	q := tx.NewInsert().Model(&records)

	for _, c := range criteria {
		q.Apply(c)
	}

	_, err := q.Returning("*").Exec(ctx)
	if err != nil {
		return records, r.mapError(fmt.Errorf("create many error: %w", err))
	}
	return records, nil
}

func (r *repo[T]) findExistingRecord(ctx context.Context, tx bun.IDB, record T) (T, bool, error) {
	var zero T

	if r.handlers.GetID != nil {
		if id := r.handlers.GetID(record); id != uuid.Nil {
			existing, err := r.GetByIdentifierTx(ctx, tx, id.String())
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

	return r.CreateTx(ctx, tx, record)
}

func (r *repo[T]) GetByIdentifier(ctx context.Context, identifier string, criteria ...SelectCriteria) (T, error) {
	return r.GetByIdentifierTx(ctx, r.db, identifier, criteria...)
}

func (r *repo[T]) GetByIdentifierTx(ctx context.Context, tx bun.IDB, identifier string, criteria ...SelectCriteria) (T, error) {
	column := "id"
	if r.handlers.GetIdentifier != nil {
		if col := strings.TrimSpace(r.handlers.GetIdentifier()); col != "" {
			column = col
		}
	}
	if isUUID(identifier) {
		column = "id"
	}
	record := r.handlers.NewRecord()
	q := tx.NewSelect().Model(record)

	for _, c := range criteria {
		q.Apply(c)
	}

	q = q.Where(fmt.Sprintf("?TableAlias.%s = ?", column), identifier).Limit(1)
	if err := q.Scan(ctx); err != nil {
		var zero T
		return zero, r.mapError(err)
	}
	return record, nil
}

func (r *repo[T]) Update(ctx context.Context, record T, criteria ...UpdateCriteria) (T, error) {
	return r.UpdateTx(ctx, r.db, record, criteria...)
}

func (r *repo[T]) UpdateTx(ctx context.Context, tx bun.IDB, record T, criteria ...UpdateCriteria) (T, error) {
	q := tx.NewUpdate().Model(record)
	for _, c := range criteria {
		q.Apply(c)
	}
	// TODO: WherePK will auto generate "ws"."id" = '44a3e9dc-0381-37a6-9652-99ea14057af5'
	// so we can call it with model having the ID and we don't need criteria
	res, err := q.OmitZero().WherePK().Returning("*").Exec(ctx)

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
	if len(records) == 0 {
		return nil, nil
	}

	q := tx.NewUpdate().Model(&records).Bulk()
	for _, c := range criteria {
		q.Apply(c)
	}

	_, err := q.
		OmitZero().
		WherePK().
		Returning("*").
		Exec(ctx)

	if err != nil {
		var zero []T
		return zero, r.mapError(err)
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
	_, err := q.Exec(ctx)
	return r.mapError(err)
}

func (r *repo[T]) TableName() string {
	var model T
	return r.db.NewCreateTable().Model(model).GetTableName()
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
