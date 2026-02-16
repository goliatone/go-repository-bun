package repository

import (
	"strings"
	"testing"
)

func TestUpdateBy_InvalidColumnFailsClosed(t *testing.T) {
	setupTestData(t)

	query := db.NewUpdate().
		Model((*TestUser)(nil)).
		Apply(UpdateBy("id;DROP TABLE test_users", "=", "123"))

	sql := query.String()
	if !strings.Contains(sql, "1=0") && !strings.Contains(sql, "1 = 0") {
		t.Fatalf("expected fail-closed predicate, got SQL: %s", sql)
	}
}
