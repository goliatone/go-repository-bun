package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectSubquery_DefaultAlias(t *testing.T) {
	setupTestData(t)

	subq := db.NewSelect().Model((*TestUser)(nil)).Column("id")
	query := db.NewSelect().Model((*TestUser)(nil)).Apply(SelectSubquery(subq))

	sql := query.String()
	assert.Contains(t, sql, `AS "test_users"`)
}

func TestSelectSubquery_CustomAlias(t *testing.T) {
	setupTestData(t)

	subq := db.NewSelect().Model((*TestUser)(nil)).Column("id")
	query := db.NewSelect().Model((*TestUser)(nil)).Apply(SelectSubquery(subq, "custom_alias"))

	sql := query.String()
	assert.Contains(t, sql, `AS "custom_alias"`)
}
