package repository

import (
	"database/sql"

	"fmt"

	"github.com/goliatone/go-errors"
	"github.com/google/uuid"
)

func SQLExpectedCount(res sql.Result, expected int64) error {
	total, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, CategoryDatabase, "Failed to get rows affected count")
	}

	if total != expected {
		return errors.NewNonRetryable(
			fmt.Sprintf("Expected %d affected rows, got %d", expected, total),
			CategoryDatabaseExpectedCount,
		).WithCode(errors.CodeInternal).
			WithTextCode("SQL_EXPECTED_COUNT_VIOLATION").
			WithMetadata(map[string]any{
				"expected": expected,
				"actual":   total,
			})
	}
	return nil
}

// And add a category checker:
func IsSQLExpectedCountViolation(err error) bool {
	return errors.IsCategory(err, CategoryDatabaseExpectedCount)
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

func isUUID(identifier string) bool {
	_, err := uuid.Parse(identifier)
	return err == nil
}
