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

// IsNoRowError checks if the error is caused by no row found in the database.
func IsNoRowError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func isUUID(identifier string) bool {
	_, err := uuid.Parse(identifier)
	return err == nil
}
