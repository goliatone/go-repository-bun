package repository

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDeleteColumnIn_EmptySliceGuards(t *testing.T) {
	setupTestData(t)

	query := db.NewDelete().
		Model((*TestUser)(nil)).
		Apply(DeleteColumnIn("id", []uuid.UUID{}))

	sql := query.String()
	assert.True(t, strings.Contains(sql, "1=0") || strings.Contains(sql, "1 = 0"))
	assert.NotContains(t, sql, "IN ()")
	assert.NotContains(t, sql, "IN (NULL)")
}

func TestDeleteColumnIn_MatchesAnyValue(t *testing.T) {
	setupTestData(t)

	ids := []uuid.UUID{uuid.New(), uuid.New()}
	query := db.NewDelete().
		Model((*TestUser)(nil)).
		Apply(DeleteColumnIn("id", ids))

	sql := query.String()
	assert.Contains(t, sql, "IN")
}
