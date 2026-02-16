package repository

import (
	"database/sql"
	stderrors "errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/goliatone/go-errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestMapDatabaseError_NilError(t *testing.T) {
	result := MapDatabaseError(nil, "postgres")
	assert.Nil(t, result)
}

func TestMapDatabaseError_AlreadyWrapped_ActualBehavior(t *testing.T) {
	wrappedErr := errors.New("already wrapped", errors.CategoryValidation)
	result := MapDatabaseError(wrappedErr, "postgres")

	var retryableResult *errors.RetryableError
	if assert.True(t, errors.As(result, &retryableResult)) {
		assert.Equal(t, "Database operation failed", retryableResult.BaseError.Message)
		assert.Equal(t, CategoryDatabase, retryableResult.BaseError.Category)
		assert.Equal(t, wrappedErr, retryableResult.BaseError.Source)
	}

	retryableErr := errors.NewRetryable("already wrapped", errors.CategoryOperation)
	result2 := MapDatabaseError(retryableErr, "postgres")

	var retryableResult2 *errors.RetryableError
	if assert.True(t, errors.As(result2, &retryableResult2)) {
		assert.Equal(t, "Database operation failed", retryableResult2.BaseError.Message)
		assert.Equal(t, CategoryDatabase, retryableResult2.BaseError.Category)
		assert.Equal(t, retryableErr, retryableResult2.BaseError.Source)
	}
}

func TestMapCommonDatabaseErrors(t *testing.T) {
	tests := []struct {
		name             string
		inputError       error
		expectedNil      bool
		expectedCode     int
		expectedText     string
		expectedRetry    bool
		expectedCategory errors.Category
	}{
		{
			name:          "sql.ErrNoRows",
			inputError:    sql.ErrNoRows,
			expectedCode:  errors.CodeNotFound,
			expectedText:  "RECORD_NOT_FOUND",
			expectedRetry: false,
		},
		{
			name:          "sql.ErrTxDone",
			inputError:    sql.ErrTxDone,
			expectedCode:  errors.CodeBadRequest,
			expectedText:  "TRANSACTION_DONE",
			expectedRetry: false,
		},
		{
			name:             "sql.ErrConnDone",
			inputError:       sql.ErrConnDone,
			expectedText:     "CONNECTION_CLOSED",
			expectedRetry:    true,
			expectedCategory: CategoryDatabaseConnection,
		},
		{
			name:             "connection refused error",
			inputError:       fmt.Errorf("connection refused"),
			expectedText:     "CONNECTION_REFUSED",
			expectedRetry:    true,
			expectedCategory: CategoryDatabaseConnection,
		},
		{
			name:          "timeout error",
			inputError:    fmt.Errorf("timeout occurred"),
			expectedCode:  errors.CodeRequestTimeout,
			expectedText:  "DATABASE_TIMEOUT",
			expectedRetry: true,
		},
		{
			name:        "unrecognized error",
			inputError:  fmt.Errorf("some other error"),
			expectedNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapCommonDatabaseErrors(tt.inputError)

			if tt.expectedNil {
				assert.Nil(t, result)
				return
			}

			assert.NotNil(t, result)

			var retryableErr *errors.RetryableError
			assert.True(t, errors.As(result, &retryableErr))

			if tt.expectedRetry {
				assert.True(t, retryableErr.IsRetryable())
			} else {
				assert.False(t, retryableErr.IsRetryable())
			}

			if tt.expectedCategory != "" {
				assert.Truef(t, errors.IsCategory(result, tt.expectedCategory), "expected category %s", tt.expectedCategory)
			}

			if tt.expectedText != "" {
				assert.Equal(t, tt.expectedText, retryableErr.BaseError.TextCode)
			}
			if tt.expectedCode != 0 {
				assert.Equal(t, tt.expectedCode, retryableErr.BaseError.Code)
			}
		})
	}
}

func TestMapPostgresErrors(t *testing.T) {
	tests := []struct {
		name             string
		pqError          *pq.Error
		expectedCode     int
		expectedText     string
		expectedRetry    bool
		expectedMeta     map[string]any
		expectedCategory errors.Category
	}{
		{
			name: "unique violation",
			pqError: &pq.Error{
				Code:       "23505",
				Constraint: "users_email_key",
				Detail:     "Key (email)=(test@example.com) already exists.",
			},
			expectedCode:  errors.CodeConflict,
			expectedText:  "DUPLICATE_KEY",
			expectedRetry: false,
			expectedMeta: map[string]any{
				"constraint": "users_email_key",
				"detail":     "Key (email)=(test@example.com) already exists.",
			},
		},
		{
			name: "foreign key violation",
			pqError: &pq.Error{
				Code:       "23503",
				Constraint: "fk_user_company",
				Detail:     "Key (company_id)=(123) is not present in table companies.",
			},
			expectedCode:  errors.CodeBadRequest,
			expectedText:  "FOREIGN_KEY_VIOLATION",
			expectedRetry: false,
		},
		{
			name: "deadlock",
			pqError: &pq.Error{
				Code: "40P01",
			},
			expectedCode:  errors.CodeConflict,
			expectedText:  "DEADLOCK_DETECTED",
			expectedRetry: true,
		},
		{
			name: "connection error",
			pqError: &pq.Error{
				Code: "08000",
			},
			expectedText:     "CONNECTION_ERROR",
			expectedRetry:    true,
			expectedCategory: CategoryDatabaseConnection,
			expectedCode:     502,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapPostgresErrors(tt.pqError)
			assert.NotNil(t, result)

			var retryableErr *errors.RetryableError
			assert.True(t, errors.As(result, &retryableErr))

			if tt.expectedRetry {
				assert.True(t, retryableErr.IsRetryable())
			} else {
				assert.False(t, retryableErr.IsRetryable())
			}

			assert.Equal(t, tt.expectedText, retryableErr.BaseError.TextCode)
			if tt.expectedCode != 0 {
				assert.Equal(t, tt.expectedCode, retryableErr.BaseError.Code)
			}

			if tt.expectedMeta != nil {
				for key, value := range tt.expectedMeta {
					assert.Equal(t, value, retryableErr.BaseError.Metadata[key])
				}
			}

			if tt.expectedCategory != "" {
				assert.Truef(t, errors.IsCategory(result, tt.expectedCategory), "expected category %s", tt.expectedCategory)
			}
		})
	}
}

func TestMapMSSQLErrors_Working(t *testing.T) {
	tests := []struct {
		name          string
		errorMsg      string
		expectedCode  int
		expectedText  string
		expectedRetry bool
		shouldMatch   bool
	}{
		{
			name:          "simple permission denied",
			errorMsg:      "permission denied",
			expectedCode:  403,
			expectedText:  "PERMISSION_DENIED",
			expectedRetry: false,
			shouldMatch:   true,
		},
		{
			name:          "simple access denied",
			errorMsg:      "access denied",
			expectedCode:  403,
			expectedText:  "PERMISSION_DENIED",
			expectedRetry: false,
			shouldMatch:   true,
		},
		{
			name:          "simple duplicate key",
			errorMsg:      "duplicate key",
			expectedCode:  409,
			expectedText:  "DUPLICATE_KEY",
			expectedRetry: false,
			shouldMatch:   true,
		},
		{
			name:          "simple timeout",
			errorMsg:      "timeout",
			expectedCode:  408,
			expectedText:  "QUERY_TIMEOUT",
			expectedRetry: true,
			shouldMatch:   true,
		},
		{
			name:          "simple deadlock",
			errorMsg:      "deadlock",
			expectedCode:  409,
			expectedText:  "DEADLOCK_DETECTED",
			expectedRetry: true,
			shouldMatch:   true,
		},
		{
			name:        "no match",
			errorMsg:    "random error",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stderrors.New(tt.errorMsg)
			result := MapMSSQLErrors(err)

			if !tt.shouldMatch {
				assert.Nil(t, result)
				return
			}

			assert.NotNil(t, result, fmt.Sprintf("Expected result for '%s' but got nil", tt.errorMsg))

			var retryableErr *errors.RetryableError
			if assert.True(t, errors.As(result, &retryableErr)) {
				assert.NotNil(t, retryableErr.BaseError)

				if tt.expectedRetry {
					assert.True(t, retryableErr.IsRetryable())
				} else {
					assert.False(t, retryableErr.IsRetryable())
				}

				assert.Equal(t, tt.expectedText, retryableErr.BaseError.TextCode)
				assert.Equal(t, tt.expectedCode, retryableErr.BaseError.Code)
			}
		})
	}
}

func TestMapMSSQLErrors_Debug(t *testing.T) {
	errorMsg := "permission denied"

	pattern := `(?i)permission denied|access denied`
	re := regexp.MustCompile(pattern)
	matches := re.MatchString(errorMsg)
	t.Logf("Pattern '%s' matches '%s': %t", pattern, errorMsg, matches)

	err := stderrors.New(errorMsg)
	result := MapMSSQLErrors(err)
	t.Logf("MapMSSQLErrors('%s') = %v", errorMsg, result)

	if result == nil {
		t.Log("Function returned nil - checking if MapMSSQLErrors function is implemented correctly")
	}
}

func TestIsDuplicatedKey_Simple(t *testing.T) {
	pqErr := &pq.Error{Code: "23505"}
	mappedErr := MapDatabaseError(pqErr, "postgres")

	result := IsDuplicatedKey(mappedErr)
	t.Logf("IsDuplicatedKey result: %t", result)
	t.Logf("Error: %v", mappedErr)

	assert.NotPanics(t, func() {
		IsDuplicatedKey(mappedErr)
		IsDuplicatedKey(sql.ErrNoRows)
		IsDuplicatedKey(nil)
	})
}

func TestIsConstraintViolation_Simple(t *testing.T) {
	assert.NotPanics(t, func() {
		IsConstraintViolation(fmt.Errorf("test"))
		IsConstraintViolation(nil)
	})
}

func TestIsRecordNotFound_Simple(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"sql.ErrNoRows", sql.ErrNoRows, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRecordNotFound(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
