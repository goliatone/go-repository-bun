package repository

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeWhereExactWithAliases(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "cf.tenant_id", Org: "cf.org_id"},
		ScopeValues{TenantID: " tenant-1 ", OrgID: " org-1 "},
		ScopeExact,
	)

	require.True(t, ok)
	assert.Equal(t, "cf.tenant_id = ? AND cf.org_id = ?", expr)
	assert.Equal(t, []any{"tenant-1", "org-1"}, args)
}

func TestScopeWhereExactFailsClosedWhenScopeBlank(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "tenant_id", Org: "org_id"},
		ScopeValues{TenantID: "", OrgID: "org-1"},
		ScopeExact,
	)

	require.True(t, ok)
	assert.Equal(t, "1=0", expr)
	assert.Nil(t, args)
}

func TestScopeWhereGlobal(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "tenant_id", Org: "org_id"},
		ScopeValues{},
		ScopeGlobal,
	)

	require.True(t, ok)
	assert.Equal(t, "(tenant_id IS NULL OR tenant_id = '') AND (org_id IS NULL OR org_id = '')", expr)
	assert.Nil(t, args)
}

func TestScopeWhereExactOrGlobal(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "cf.tenant_id", Org: "cf.org_id"},
		ScopeValues{TenantID: "tenant-1", OrgID: "org-1"},
		ScopeExactOrGlobal,
	)

	require.True(t, ok)
	assert.Equal(t, "((cf.tenant_id = ? AND cf.org_id = ?) OR ((cf.tenant_id IS NULL OR cf.tenant_id = '') AND (cf.org_id IS NULL OR cf.org_id = '')))", expr)
	assert.Equal(t, []any{"tenant-1", "org-1"}, args)
}

func TestScopeWhereExactOrGlobalWithBlankScopeReturnsGlobalOnly(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "tenant_id", Org: "org_id"},
		ScopeValues{},
		ScopeExactOrGlobal,
	)

	require.True(t, ok)
	assert.Equal(t, "(tenant_id IS NULL OR tenant_id = '') AND (org_id IS NULL OR org_id = '')", expr)
	assert.Nil(t, args)
}

func TestScopeWhereInvalidColumnFailsClosed(t *testing.T) {
	expr, args, ok := ScopeWhere(
		ScopeColumns{Tenant: "tenant_id;DROP", Org: "org_id"},
		ScopeValues{TenantID: "tenant-1", OrgID: "org-1"},
		ScopeExact,
	)

	require.True(t, ok)
	assert.Equal(t, "1=0", expr)
	assert.Nil(t, args)
}

func TestScopeWhereNoColumnsNoops(t *testing.T) {
	expr, args, ok := ScopeWhere(ScopeColumns{}, ScopeValues{}, ScopeExact)

	assert.False(t, ok)
	assert.Empty(t, expr)
	assert.Nil(t, args)
}

func TestSelectScopeAppliesPredicate(t *testing.T) {
	setupTestData(t)

	query := db.NewSelect().
		Model((*TestUser)(nil)).
		Apply(SelectScope(
			ScopeColumns{Tenant: "company_id"},
			ScopeValues{TenantID: "company-1"},
			ScopeExact,
		))

	sql := query.String()
	assert.Contains(t, sql, "company_id = 'company-1'")
}

func TestSelectScopeGlobalFallbackAppliesPredicate(t *testing.T) {
	setupTestData(t)

	query := db.NewSelect().
		Model((*TestUser)(nil)).
		Apply(SelectScope(
			ScopeColumns{Tenant: "company_id"},
			ScopeValues{TenantID: "company-1"},
			ScopeExactOrGlobal,
		))

	sql := query.String()
	assert.Contains(t, sql, "company_id = 'company-1'")
	assert.True(t, strings.Contains(sql, `"test_user"."company_id" IS NULL`) || strings.Contains(sql, `company_id IS NULL`))
}

func TestDefaultScopeRecordPreservesExplicitValues(t *testing.T) {
	record := &testScopedRecord{tenantID: "tenant-explicit"}

	changed := DefaultScope(record, ScopeValues{TenantID: "tenant-default", OrgID: "org-default"})

	assert.True(t, changed)
	assert.Equal(t, "tenant-explicit", record.tenantID)
	assert.Equal(t, "org-default", record.orgID)
}

func TestDefaultScopeMapPreservesExplicitValues(t *testing.T) {
	values := map[string]any{
		"tenant_id": "tenant-explicit",
		"org_id":    " ",
	}

	changed := DefaultScopeMap(values, ScopeValues{TenantID: "tenant-default", OrgID: "org-default"})

	assert.True(t, changed)
	assert.Equal(t, "tenant-explicit", values["tenant_id"])
	assert.Equal(t, "org-default", values["org_id"])
}

type testScopedRecord struct {
	tenantID string
	orgID    string
}

func (r *testScopedRecord) GetTenantID() string {
	return r.tenantID
}

func (r *testScopedRecord) SetTenantID(value string) {
	r.tenantID = value
}

func (r *testScopedRecord) GetOrgID() string {
	return r.orgID
}

func (r *testScopedRecord) SetOrgID(value string) {
	r.orgID = value
}
