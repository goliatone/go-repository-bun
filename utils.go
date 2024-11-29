package repository

import (
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

// ErrRecordNotFound means the query returned no results
var ErrRecordNotFound = errors.New("record not found")

// ErrSQLExpectedCountViolation when returned rows do not match expectation
var ErrSQLExpectedCountViolation = errors.New("SQL expected count violation")

// SQLExpectedCount ensure we have the expected number of results
func SQLExpectedCount(res sql.Result, expected int64) error {
	total, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if total != expected {
		return ErrSQLExpectedCountViolation
	}

	return nil
}

// IsSQLExpectedCountViolation checks for expected violation error
func IsSQLExpectedCountViolation(err error) bool {
	return errors.Is(err, ErrSQLExpectedCountViolation)
}

func IsRecordNotFound(err error) bool {
	return errors.Is(err, ErrRecordNotFound) ||
		errors.Is(err, sql.ErrNoRows) ||
		errors.Is(err, ErrSQLExpectedCountViolation)
}

// IsNoRowError checks if the error is caused by no row found in the database.
func IsNoRowError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func isUUID(identifier string) bool {
	_, err := uuid.Parse(identifier)
	return err == nil
}
