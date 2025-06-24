package repository

import (
	"database/sql"
	"net/http"
	"regexp"
	"strings"

	"github.com/goliatone/go-errors"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

const (
	CategoryDatabase              = errors.Category("database")
	CategoryDatabaseNotFound      = errors.Category("database_not_found")
	CategoryDatabaseConstraint    = errors.Category("database_constraint")
	CategoryDatabaseDuplicate     = errors.Category("database_duplicate")
	CategoryDatabaseConnection    = errors.Category("database_connection")
	CategoryDatabaseTimeout       = errors.Category("database_timeout")
	CategoryDatabaseLock          = errors.Category("database_lock")
	CategoryDatabasePermission    = errors.Category("database_permission")
	CategoryDatabaseSyntax        = errors.Category("database_syntax")
	CategoryDatabaseExpectedCount = errors.Category("database_expected_count")
)

// DatabaseErrorMapper maps database specific errors to standardized errors
type DatabaseErrorMapper func(error) error

func GetDatabaseErrorMappers(driver string) []DatabaseErrorMapper {
	switch driver {
	case "postgres", "pgx":
		return []DatabaseErrorMapper{MapPostgresErrors, MapCommonDatabaseErrors}
	case "sqlite3", "sqlite":
		return []DatabaseErrorMapper{MapSQLiteErrors, MapCommonDatabaseErrors}
	case "sqlserver", "mssql":
		return []DatabaseErrorMapper{MapMSSQLErrors, MapCommonDatabaseErrors}
	default:
		return []DatabaseErrorMapper{MapCommonDatabaseErrors}
	}
}

func MapDatabaseError(err error, driver string) error {
	if err == nil {
		return nil
	}

	mappers := GetDatabaseErrorMappers(driver)
	for _, mapper := range mappers {
		if mappedErr := mapper(err); mappedErr != nil {
			return mappedErr
		}
	}

	return errors.WrapRetryable(err, CategoryDatabase, "Database operation failed").
		WithCode(errors.CodeInternal).
		WithTextCode("DATABASE_ERROR")
}

func MapPostgresErrors(err error) error {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return nil
	}

	switch pqErr.Code {
	case "23505": // unique violation
		return errors.NewNonRetryable("Duplicate key value violates unique constraint", CategoryDatabaseDuplicate).
			WithCode(errors.CodeConflict).
			WithTextCode("DUPLICATE_KEY").
			WithMetadata(map[string]any{
				"constraint": pqErr.Constraint,
				"detail":     pqErr.Detail,
			})

	case "23503": // foreign key violation
		return errors.NewNonRetryable("Foreign key constraint violation", CategoryDatabaseConstraint).
			WithCode(errors.CodeBadRequest).
			WithTextCode("FOREIGN_KEY_VIOLATION").
			WithMetadata(map[string]any{
				"constraint": pqErr.Constraint,
				"detail":     pqErr.Detail,
			})

	case "23514": // check constraint violation
		return errors.NewNonRetryable("Check constraint violation", CategoryDatabaseConstraint).
			WithCode(errors.CodeBadRequest).
			WithTextCode("CHECK_CONSTRAINT_VIOLATION")

	case "23502": // not null violation
		return errors.NewNonRetryable("Not null constraint violation", CategoryDatabaseConstraint).
			WithCode(errors.CodeBadRequest).
			WithTextCode("NOT_NULL_VIOLATION").
			WithMetadata(map[string]any{
				"column": pqErr.Column,
			})

	case "40001": // serialization failure, we could retry
		return errors.NewRetryableOperation("Serialization failure, retry transaction", 1000).
			WithCode(errors.CodeConflict).
			WithTextCode("SERIALIZATION_FAILURE")

	case "40P01": // deadlock, we could retry
		return errors.NewRetryableOperation("Deadlock detected", 500).
			WithCode(errors.CodeConflict).
			WithTextCode("DEADLOCK_DETECTED")

	case "08000", "08003", "08006": // connection errors, retryable
		return errors.NewRetryableExternal("Database connection error").
			WithTextCode("CONNECTION_ERROR")

	case "42501": // insufficient privilege
		return errors.NewNonRetryable("Insufficient privilege", CategoryDatabasePermission).
			WithCode(errors.CodeForbidden).
			WithTextCode("INSUFFICIENT_PRIVILEGE")

	case "42601": // syntax error
		return errors.NewNonRetryable("SQL syntax error", CategoryDatabaseSyntax).
			WithCode(errors.CodeBadRequest).
			WithTextCode("SYNTAX_ERROR")
	}

	return nil
}

func MapCommonDatabaseErrors(err error) error {
	switch {
	case err == sql.ErrNoRows:
		return errors.NewNonRetryable("Record not found", CategoryDatabaseNotFound).
			WithCode(errors.CodeNotFound).
			WithTextCode("RECORD_NOT_FOUND")
	case err == sql.ErrTxDone:
		return errors.NewNonRetryable("Transaction has already been committed or rolled back", CategoryDatabase).
			WithCode(errors.CodeBadRequest).
			WithTextCode("TRANSACTION_DONE")
	case err == sql.ErrConnDone:
		return errors.NewRetryableExternal("Database connection is closed").
			WithTextCode("CONNECTION_CLOSED")
	case strings.Contains(err.Error(), "connection refused"):
		return errors.NewRetryableExternal("Database connection refused").
			WithTextCode("CONNECTION_REFUSED")
	case strings.Contains(err.Error(), "timeout"):
		return errors.NewRetryableOperation("Database operation timeout", 2000).
			WithCode(errors.CodeRequestTimeout).
			WithTextCode("DATABASE_TIMEOUT")
	}
	return nil
}

func MapSQLiteErrors(err error) error {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return nil
	}

	switch sqliteErr.Code {
	case sqlite3.ErrConstraint:
		msg := sqliteErr.Error()
		switch {
		case strings.Contains(msg, "UNIQUE"):
			return errors.NewNonRetryable("Duplicate key value violates unique constraint", CategoryDatabaseDuplicate).
				WithCode(errors.CodeConflict).
				WithTextCode("DUPLICATE_KEY")

		case strings.Contains(msg, "FOREIGN KEY"):
			return errors.NewNonRetryable("Foreign key constraint violation", CategoryDatabaseConstraint).
				WithCode(errors.CodeBadRequest).
				WithTextCode("FOREIGN_KEY_VIOLATION")

		case strings.Contains(msg, "NOT NULL"):
			return errors.NewNonRetryable("Not null constraint violation", CategoryDatabaseConstraint).
				WithCode(errors.CodeBadRequest).
				WithTextCode("NOT_NULL_VIOLATION")

		default:
			return errors.NewNonRetryable("Constraint violation", CategoryDatabaseConstraint).
				WithCode(errors.CodeBadRequest).
				WithTextCode("CONSTRAINT_VIOLATION")
		}
	case sqlite3.ErrBusy:
		return errors.NewRetryableOperation("Database is locked", 100).
			WithCode(errors.CodeConflict).
			WithTextCode("DATABASE_LOCKED")

	case sqlite3.ErrLocked:
		return errors.NewRetryableOperation("Database table is locked", 100).
			WithCode(errors.CodeConflict).
			WithTextCode("TABLE_LOCKED")

	case sqlite3.ErrAuth:
		return errors.NewNonRetryable("Authorization denied", CategoryDatabasePermission).
			WithCode(errors.CodeForbidden).
			WithTextCode("AUTHORIZATION_DENIED")
	}

	return nil
}

func MapMSSQLErrors(err error) error {
	msg := err.Error()

	// TODO: more comprehensive patterns :/
	patterns := map[*regexp.Regexp]error{
		regexp.MustCompile(`(?i)duplicate key|unique.*constraint`): errors.NewNonRetryable("Duplicate key violation", CategoryDatabaseDuplicate).
			WithCode(http.StatusConflict).
			WithTextCode("DUPLICATE_KEY"),

		regexp.MustCompile(`(?i)foreign key.*constraint`): errors.NewNonRetryable("Foreign key constraint violation", CategoryDatabaseConstraint).
			WithCode(http.StatusBadRequest).
			WithTextCode("FOREIGN_KEY_VIOLATION"),

		regexp.MustCompile(`(?i)deadlock|was deadlocked`): errors.NewRetryableOperation("Deadlock detected", 500).
			WithCode(http.StatusConflict).
			WithTextCode("DEADLOCK_DETECTED"),

		regexp.MustCompile(`(?i)timeout|query timeout`): errors.NewRetryableOperation("Query timeout", 2000).
			WithCode(http.StatusRequestTimeout).
			WithTextCode("QUERY_TIMEOUT"),

		regexp.MustCompile(`(?i)permission denied|access denied`): errors.NewNonRetryable("Permission denied", CategoryDatabasePermission).
			WithCode(http.StatusForbidden).
			WithTextCode("PERMISSION_DENIED"),
	}

	for pattern, errorTemplate := range patterns {
		if pattern.MatchString(msg) {
			return errorTemplate
		}
	}

	return nil
}

func IsRecordNotFound(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, sql.ErrNoRows) {
		return true
	}

	if errors.IsCategory(err, CategoryDatabaseNotFound) {
		return true
	}

	var retryableErr *errors.RetryableError
	if errors.As(err, &retryableErr) {
		if retryableErr.BaseError != nil {
			return errors.IsCategory(retryableErr.BaseError, CategoryDatabaseNotFound)
		}
	}

	return false
}

func IsDuplicatedKey(err error) bool {
	return errors.IsCategory(err, CategoryDatabaseDuplicate)
}

func IsConstraintViolation(err error) bool {
	return errors.IsCategory(err, CategoryDatabaseConstraint) ||
		errors.IsCategory(err, CategoryDatabaseDuplicate)
}

func IsConnectionError(err error) bool {
	return errors.IsCategory(err, CategoryDatabaseConnection)
}

func IsRetryableDatabase(err error) bool {
	return errors.IsRetryableError(err)
}
