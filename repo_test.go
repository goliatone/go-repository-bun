package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	goerrors "github.com/goliatone/go-errors"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

type TestUser struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`

	ID        uuid.UUID `bun:"id,pk,notnull"`
	Name      string    `bun:"name,notnull"`
	Email     string    `bun:"email,notnull,unique"`
	CompanyID uuid.UUID `bun:"company_id,notnull"`

	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

type TestCompany struct {
	bun.BaseModel `bun:"table:test_companies,alias:c"`

	ID         uuid.UUID `bun:"id,pk,notnull"`
	Name       string    `bun:"name,notnull"`
	Identifier string    `bun:"identifier,notnull"`

	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

func newTestUserRepository(db *bun.DB, opts ...Option) Repository[*TestUser] {
	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "email"
		},
		GetIdentifierValue: func(record *TestUser) string {
			return record.Email
		},
	}
	return MustNewRepositoryWithOptions[*TestUser](db, handlers, opts...)
}

func newTestUserRepositoryWithConfig(db *bun.DB, dbOpts []Option, repoOpts ...RepoOption) Repository[*TestUser] {
	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "email"
		},
		GetIdentifierValue: func(record *TestUser) string {
			return record.Email
		},
	}
	return MustNewRepositoryWithConfig[*TestUser](db, handlers, dbOpts, repoOpts...)
}

func newTestUserRepositoryWithoutIdentifierWithConfig(db *bun.DB, dbOpts []Option, repoOpts ...RepoOption) Repository[*TestUser] {
	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
	}
	return MustNewRepositoryWithConfig[*TestUser](db, handlers, dbOpts, repoOpts...)
}

func newTestCompanyRepository(db *bun.DB) Repository[*TestCompany] {
	handlers := ModelHandlers[*TestCompany]{
		NewRecord: func() *TestCompany {
			return &TestCompany{}
		},
		GetID: func(record *TestCompany) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestCompany, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "identifier"
		},
		GetIdentifierValue: func(record *TestCompany) string {
			return record.Identifier
		},
	}
	return MustNewRepository[*TestCompany](db, handlers)
}

var db *bun.DB

func TestMain(m *testing.M) {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	sqldb.SetMaxOpenConns(1)
	sqldb.SetMaxIdleConns(1)
	defer sqldb.Close()

	db = bun.NewDB(sqldb, sqlitedialect.New())

	code := m.Run()

	os.Exit(code)
}

func TestNewRepositories(t *testing.T) {
	userRepo := newTestUserRepository(db)
	companyRepo := newTestCompanyRepository(db)
	assert.NotNil(t, userRepo)
	assert.NotNil(t, companyRepo)
}

func TestRepository_MustNewRepository_InvalidConfigPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustNewRepository[*TestUser](nil, ModelHandlers[*TestUser]{})
	})
}

func TestRepository_NewRepositoryWithConfig_NilDBDoesNotPanic(t *testing.T) {
	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser { return &TestUser{} },
		GetID:     func(record *TestUser) uuid.UUID { return record.ID },
		SetID:     func(record *TestUser, id uuid.UUID) { record.ID = id },
	}

	assert.NotPanics(t, func() {
		repo := NewRepositoryWithConfig[*TestUser](nil, handlers, nil)
		assert.NotNil(t, repo)

		validator, ok := repo.(Validator)
		assert.True(t, ok)
		assert.Error(t, validator.Validate())
	})
}

func TestRepository_ValidateReturnsValidationErrors(t *testing.T) {
	repo := &repo[*TestUser]{
		db:       nil,
		handlers: ModelHandlers[*TestUser]{},
	}

	err := repo.Validate()
	assert.Error(t, err)

	validationErrors, ok := goerrors.GetValidationErrors(err)
	assert.True(t, ok)
	assert.NotEmpty(t, validationErrors)
}

func TestRepository_Create(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)
	companyRepo := newTestCompanyRepository(db)

	company := &TestCompany{
		ID:   uuid.New(),
		Name: "Test Company",
	}
	createdCompany, err := companyRepo.CreateTx(ctx, db, company)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdCompany.ID)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "John Doe",
		Email:     "john.doe@example.com",
		CompanyID: createdCompany.ID,
	}

	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdUser.ID)
	assert.Equal(t, user.Name, createdUser.Name)
}

func TestRepository_Get(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Jane Smith",
		Email:     "jane.smith@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	// Retrieve the user by ID
	retrievedUser, err := userRepo.GetByIDTx(ctx, db, createdUser.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, createdUser.ID, retrievedUser.ID)
	assert.Equal(t, createdUser.Name, retrievedUser.Name)
}

func TestRepository_GetByIdentifier(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Test User",
		Email:     "test.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	// Retrieve the user by identifier (email)
	retrievedUser, err := userRepo.GetByIdentifier(ctx, user.Email)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, retrievedUser.ID)
	assert.Equal(t, user.Name, retrievedUser.Name)
	assert.Equal(t, user.Email, retrievedUser.Email)
}

func TestRepository_GetByIdentifierTx(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Transactional User",
		Email:     "transactional.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err = userRepo.CreateTx(ctx, tx, user)
	assert.NoError(t, err)

	// Retrieve the user by identifier (email) within the same transaction
	retrievedUser, err := userRepo.GetByIdentifierTx(ctx, tx, user.Email)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, retrievedUser.ID)
	assert.Equal(t, user.Name, retrievedUser.Name)
	assert.Equal(t, user.Email, retrievedUser.Email)

	err = tx.Commit()
	assert.NoError(t, err)
}

func TestRepository_GetByIdentifier_NotFound(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	// Attempt to retrieve a user with a non-existent email should fail
	nonExistentEmail := "non.existent@example.com"
	_, err := userRepo.GetByIdentifier(ctx, nonExistentEmail)
	assert.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRepository_GetByIdentifier_EmptyIdentifierReturnsNotFound(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	_, err := userRepo.GetByIdentifier(ctx, "   ")
	assert.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRepository_Scopes_SelectDefault(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)
	userRepo := newTestUserRepository(db)
	userRepo.(*repo[*TestUser]).resetScopes()

	const tenantScope = "tenant"

	userRepo.RegisterScope(tenantScope, ScopeByField(tenantScope, "company_id"))
	assert.NoError(t, userRepo.SetScopeDefaults(ScopeDefaults{
		Select: []string{tenantScope},
	}))

	tenantCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Tenant Co",
		Identifier: "tenant-co",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err := companyRepo.CreateTx(ctx, db, tenantCompany)
	assert.NoError(t, err)

	otherCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Other Co",
		Identifier: "other-co",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err = companyRepo.CreateTx(ctx, db, otherCompany)
	assert.NoError(t, err)

	users := []*TestUser{
		{
			ID:        uuid.New(),
			Name:      "Tenant User",
			Email:     "tenant@example.com",
			CompanyID: tenantCompany.ID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			ID:        uuid.New(),
			Name:      "Other User",
			Email:     "other@example.com",
			CompanyID: otherCompany.ID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	scopedCtx := WithScopeData(ctx, tenantScope, tenantCompany.ID)

	records, total, err := userRepo.List(scopedCtx)
	assert.NoError(t, err)
	assert.Equal(t, 1, total)
	if assert.Len(t, records, 1) {
		assert.Equal(t, tenantCompany.ID, records[0].CompanyID)
		assert.Equal(t, "Tenant User", records[0].Name)
	}
}

func TestRepository_Scopes_SelectDefaultRequiredFailsClosedWithoutData(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)
	userRepo.(*repo[*TestUser]).resetScopes()

	const tenantScope = "tenant"

	userRepo.RegisterScope(tenantScope, ScopeByFieldRequired(tenantScope, "company_id"))
	assert.NoError(t, userRepo.SetScopeDefaults(ScopeDefaults{
		Select: []string{tenantScope},
	}))

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Tenant User",
		Email:     "tenant.required@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	records, total, err := userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Len(t, records, 0)
}

func TestRepository_SetScopeDefaults_UnknownScope(t *testing.T) {
	setupTestData(t)

	repo := newTestUserRepository(db)

	err := repo.SetScopeDefaults(ScopeDefaults{
		Select: []string{"missing"},
	})
	assert.Error(t, err)

	validationErrors, ok := goerrors.GetValidationErrors(err)
	assert.True(t, ok)
	if assert.Len(t, validationErrors, 1) {
		assert.Contains(t, validationErrors[0].Message, "missing")
	}
}

func TestRepository_Scopes_UpdateRestriction(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)
	userRepo := newTestUserRepository(db)
	userRepo.(*repo[*TestUser]).resetScopes()

	const tenantScope = "tenant"

	userRepo.RegisterScope(tenantScope, ScopeByField(tenantScope, "company_id"))

	tenantCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Tenant Co",
		Identifier: "tenant-co",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err := companyRepo.CreateTx(ctx, db, tenantCompany)
	assert.NoError(t, err)

	otherCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Other Co",
		Identifier: "other-co",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err = companyRepo.CreateTx(ctx, db, otherCompany)
	assert.NoError(t, err)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Tenant User",
		Email:     "tenant@example.com",
		CompanyID: tenantCompany.ID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	created, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	wrongCtx := WithScopeData(ctx, tenantScope, otherCompany.ID)
	wrongCtx = WithUpdateScopes(wrongCtx, tenantScope)

	created.Name = "Updated Name"
	created.UpdatedAt = time.Now()
	_, err = userRepo.UpdateTx(wrongCtx, db, created)
	assert.Error(t, err)
	assert.True(t, IsSQLExpectedCountViolation(err))

	reloaded, err := userRepo.GetByID(ctx, created.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, "Tenant User", reloaded.Name)

	correctCtx := WithScopeData(ctx, tenantScope, tenantCompany.ID)
	correctCtx = WithUpdateScopes(correctCtx, tenantScope)

	reloaded.Name = "Updated Again"
	reloaded.UpdatedAt = time.Now()
	updated, err := userRepo.UpdateTx(correctCtx, db, reloaded)
	assert.NoError(t, err)
	assert.Equal(t, "Updated Again", updated.Name)
}

func TestRepository_GetByIdentifier_UUIDStringInCustomColumn(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)

	identifier := uuid.NewString()
	company := &TestCompany{
		ID:         uuid.New(),
		Name:       "UUID Company",
		Identifier: identifier,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	_, err := companyRepo.CreateTx(ctx, db, company)
	assert.NoError(t, err)

	retrievedCompany, err := companyRepo.GetByIdentifier(ctx, identifier)
	assert.NoError(t, err)
	assert.Equal(t, company.ID, retrievedCompany.ID)
	assert.Equal(t, company.Identifier, retrievedCompany.Identifier)
}

func TestRepository_Update(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Alice Johnson",
		Email:     "alice.johnson@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	createdUser.Email = "alice.j@example.com"
	updatedUser, err := userRepo.UpdateTx(ctx, db, createdUser)
	assert.NoError(t, err)
	assert.Equal(t, "alice.j@example.com", updatedUser.Email)
}

func TestRepository_Update2(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Alice Johnson",
		Email:     "alice.johnson@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)
	assert.Equal(t, user.ID.String(), createdUser.ID.String())

	payload := &TestUser{}
	userRepo.Handlers().SetID(payload, user.ID)
	payload.Email = "alice.j@example.com"
	payload.UpdatedAt = time.Now()
	updatedUser, err := userRepo.UpdateTx(ctx, db, payload, UpdateByID(user.ID.String()), UpdateColumns("email", "updated_at"))
	assert.NoError(t, err)
	assert.Equal(t, "alice.j@example.com", updatedUser.Email)

	reloaded, err := userRepo.GetByID(ctx, user.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, user.Name, reloaded.Name, "expected other fields to remain unchanged")
}

func TestRepository_Update_AllowsZeroValues(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "NonEmpty",
		Email:     "zero@example.com",
		CompanyID: uuid.New(),
	}
	created, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	created.Name = ""
	created.UpdatedAt = time.Now()

	updated, err := userRepo.UpdateTx(ctx, db, created)
	assert.NoError(t, err)

	reloaded, err := userRepo.GetByID(ctx, updated.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, "", reloaded.Name, "expected zero value to persist after update")
}

func TestRepository_Update_SkipZeroValuesCriterion(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "KeepMe",
		Email:     "skip-zero@example.com",
		CompanyID: uuid.New(),
	}
	created, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	payload := &TestUser{}
	userRepo.Handlers().SetID(payload, created.ID)
	payload.Name = ""
	payload.Email = "skip-zero@example.com"
	payload.UpdatedAt = time.Now()

	_, err = userRepo.UpdateTx(ctx, db, payload, UpdateByID(created.ID.String()), UpdateSkipZeroValues())
	assert.NoError(t, err)

	reloaded, err := userRepo.GetByID(ctx, created.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, "KeepMe", reloaded.Name, "expected name to remain when UpdateSkipZeroValues is applied")
}

func TestRepository_UpdateMany_AllowsZeroValues(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	users := []*TestUser{
		{
			ID:        uuid.New(),
			Name:      "First",
			Email:     "first@example.com",
			CompanyID: uuid.New(),
		},
		{
			ID:        uuid.New(),
			Name:      "Second",
			Email:     "second@example.com",
			CompanyID: uuid.New(),
		},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	users[0].Name = ""
	users[0].UpdatedAt = time.Now()
	users[1].Name = ""
	users[1].UpdatedAt = time.Now()

	updated, err := userRepo.UpdateManyTx(ctx, db, users)
	assert.NoError(t, err)
	assert.Len(t, updated, 2)

	for _, original := range users {
		reloaded, err := userRepo.GetByID(ctx, original.ID.String())
		assert.NoError(t, err)
		assert.Equal(t, "", reloaded.Name, "expected zero value to persist after bulk update")
	}
}

func TestRepository_Delete(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Bob Brown",
		Email:     "bob.brown@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	err = userRepo.DeleteTx(ctx, db, createdUser)
	assert.NoError(t, err)

	// Attempt to retrieve the deleted user should fail
	_, err = userRepo.GetByIDTx(ctx, db, createdUser.ID.String())
	assert.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRepository_GetOrCreate(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)

	companyID := uuid.New()
	company := &TestCompany{
		ID:   companyID,
		Name: "Unique Company",
	}

	// First call should create the company
	firstCallCompany, err := companyRepo.GetOrCreateTx(ctx, db, company)
	assert.NoError(t, err)
	assert.Equal(t, companyID, firstCallCompany.ID)

	// Second call should retrieve the existing company
	secondCallCompany, err := companyRepo.GetOrCreateTx(ctx, db, company)
	assert.NoError(t, err)
	assert.Equal(t, firstCallCompany.ID, secondCallCompany.ID)
}

func TestRepository_GetOrCreateTx_ReturnsExistingOnDuplicateRace(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)
	repoImpl := userRepo.(*repo[*TestUser])
	repoImpl.resetScopes()

	const blockerScope = "test-insert-blocker"
	repoImpl.RegisterScope(blockerScope, ScopeDefinition{
		Insert: func(ctx context.Context) []InsertCriteria {
			val, ok := ScopeData(ctx, blockerScope)
			if !ok {
				return nil
			}
			blocker, ok := val.(*insertBlocker)
			if !ok || blocker == nil {
				return nil
			}
			return []InsertCriteria{
				func(q *bun.InsertQuery) *bun.InsertQuery {
					blocker.ready <- struct{}{}
					<-blocker.proceed
					return q
				},
			}
		},
	})
	assert.NoError(t, repoImpl.SetScopeDefaults(ScopeDefaults{
		Insert: []string{blockerScope},
	}))

	blocker := &insertBlocker{
		ready:   make(chan struct{}, 1),
		proceed: make(chan struct{}, 1),
	}

	scopeCtx := WithScopeData(ctx, blockerScope, blocker)

	now := time.Now()

	record := &TestUser{
		ID:        uuid.New(),
		Name:      "Original Name",
		Email:     "duplicate-race@example.com",
		CompanyID: uuid.New(),
		CreatedAt: now,
		UpdatedAt: now,
	}

	var (
		result  *TestUser
		callErr error
		wg      sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := userRepo.GetOrCreateTx(scopeCtx, db, record)
		result = res
		callErr = err
	}()

	<-blocker.ready

	manual := &TestUser{
		ID:        uuid.New(),
		Name:      "Manual Insert",
		Email:     record.Email,
		CompanyID: record.CompanyID,
		CreatedAt: now.Add(1 * time.Millisecond),
		UpdatedAt: now.Add(1 * time.Millisecond),
	}
	_, err := userRepo.CreateTx(ctx, db, manual)
	assert.NoError(t, err)

	blocker.proceed <- struct{}{}
	wg.Wait()

	assert.NoError(t, callErr)
	if assert.NotNil(t, result) {
		assert.Equal(t, manual.Email, result.Email)
		assert.Equal(t, manual.Name, result.Name)
		assert.Equal(t, manual.ID, result.ID)
	}
}

func TestRepository_DeleteWhere(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	users := []*TestUser{
		{ID: uuid.New(), Name: "User One", Email: "user1@example.com", CompanyID: uuid.New()},
		{ID: uuid.New(), Name: "User Two", Email: "user2@example.com", CompanyID: uuid.New()},
		{ID: uuid.New(), Name: "User Three", Email: "user3@example.com", CompanyID: uuid.New()},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	err := userRepo.DeleteWhereTx(ctx, db, func(q *bun.DeleteQuery) *bun.DeleteQuery {
		return q.Where("email = ?", "user2@example.com")
	})
	assert.NoError(t, err)

	// Verify that only two users remain
	remainingUsers, err := userRepo.Raw(ctx, "SELECT * FROM test_users")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(remainingUsers))
}

func TestRepository_DeleteWhere_WithoutCriteriaBlockedByDefault(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{ID: uuid.New(), Name: "User One", Email: "user1@example.com", CompanyID: uuid.New()}
	_, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	err = userRepo.DeleteWhere(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsafe delete prevented")

	remainingUsers, err := userRepo.Raw(ctx, "SELECT * FROM test_users")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(remainingUsers))
}

func TestRepository_DeleteWhere_WithoutCriteriaAllowedWhenConfigured(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithConfig(db, nil, WithAllowFullTableDelete(true))

	users := []*TestUser{
		{ID: uuid.New(), Name: "User One", Email: "user1@example.com", CompanyID: uuid.New()},
		{ID: uuid.New(), Name: "User Two", Email: "user2@example.com", CompanyID: uuid.New()},
	}
	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	err := userRepo.DeleteWhere(ctx)
	assert.NoError(t, err)

	remainingUsers, err := userRepo.Raw(ctx, "SELECT * FROM test_users")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(remainingUsers))
}

func TestRepository_TransactionCommit(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Transactional User",
		Email:     "transact.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	createdUser, err := userRepo.CreateTx(ctx, tx, user)
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	// Verify that the user exists after the transaction is committed
	retrievedUser, err := userRepo.GetByIDTx(ctx, db, createdUser.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, createdUser.ID, retrievedUser.ID)
	assert.Equal(t, createdUser.Name, retrievedUser.Name)
}

func TestRepository_TransactionRollback(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Rollback User",
		Email:     "rollback.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	createdUser, err := userRepo.CreateTx(ctx, tx, user)
	assert.NoError(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	// Verify that the user does not exist after the transaction is rolled back
	_, err = userRepo.GetByIDTx(ctx, db, createdUser.ID.String())
	assert.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRepository_Raw(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	users := []*TestUser{
		{ID: uuid.New(), Name: "Raw User One", Email: "raw1@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: uuid.New(), Name: "Raw User Two", Email: "raw2@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: uuid.New(), Name: "Other User", Email: "other@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	query := "SELECT * FROM test_users WHERE email LIKE ?"
	rawUsers, err := userRepo.Raw(ctx, query, "raw%")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rawUsers))

	for _, user := range rawUsers {
		assert.Contains(t, user.Email, "raw")
	}
}

func TestRepository_Upsert(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Upsert User",
		Email:     "upsert.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	upsertedUser, err := userRepo.Upsert(ctx, user)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, upsertedUser.ID)
	assert.Equal(t, user.Name, upsertedUser.Name)

	upsertedUser.Name = "Upsert User Updated"
	upsertedUser.UpdatedAt = time.Now()

	upsertedUser, err = userRepo.Upsert(ctx, upsertedUser)
	assert.NoError(t, err)
	assert.Equal(t, "Upsert User Updated", upsertedUser.Name)

	retrievedUser, err := userRepo.GetByIdentifier(ctx, upsertedUser.Email)
	assert.NoError(t, err)
	assert.Equal(t, upsertedUser.Name, retrievedUser.Name)
}

func TestRepository_UpsertTx(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "UpsertTx User",
		Email:     "upserttx.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	upsertedUser, err := userRepo.UpsertTx(ctx, tx, user)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, upsertedUser.ID)
	assert.Equal(t, user.Name, upsertedUser.Name)

	upsertedUser.Name = "UpsertTx User Updated"
	upsertedUser.UpdatedAt = time.Now()

	upsertedUser, err = userRepo.UpsertTx(ctx, tx, upsertedUser)
	assert.NoError(t, err)
	assert.Equal(t, "UpsertTx User Updated", upsertedUser.Name)

	err = tx.Commit()
	assert.NoError(t, err)

	retrievedUser, err := userRepo.GetByIdentifier(ctx, upsertedUser.Email)
	assert.NoError(t, err)
	assert.Equal(t, upsertedUser.Name, retrievedUser.Name)
}

func TestRepository_Upsert_Insert(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	user := &TestUser{
		ID:        uuid.New(),
		Name:      "New Upsert User",
		Email:     "new.upsert.user@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	upsertedUser, err := userRepo.Upsert(ctx, user)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, upsertedUser.ID)
	assert.Equal(t, user.Name, upsertedUser.Name)

	// Verify that the user exists in the database
	retrievedUser, err := userRepo.GetByIdentifier(ctx, user.Email)
	assert.NoError(t, err)
	assert.Equal(t, user.ID, retrievedUser.ID)
}

func TestRepository_Upsert_UsesIdentifierWhenIDMissing(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	existing := &TestUser{
		ID:        uuid.New(),
		Name:      "Existing User",
		Email:     "existing@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	existing, err := userRepo.CreateTx(ctx, db, existing)
	assert.NoError(t, err)

	payload := &TestUser{
		Name:      "Updated Existing User",
		Email:     existing.Email,
		CompanyID: existing.CompanyID,
		UpdatedAt: time.Now(),
	}

	upserted, err := userRepo.Upsert(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, existing.ID, upserted.ID)
	assert.Equal(t, "Updated Existing User", upserted.Name)

	reloaded, err := userRepo.GetByID(ctx, existing.ID.String())
	assert.NoError(t, err)
	assert.Equal(t, "Updated Existing User", reloaded.Name)
}

func TestRepository_GetOrCreate_UsesIdentifierWhenIDMissing(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)

	existing := &TestCompany{
		ID:         uuid.New(),
		Name:       "Existing Company",
		Identifier: "company-123",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err := companyRepo.CreateTx(ctx, db, existing)
	assert.NoError(t, err)

	payload := &TestCompany{
		Name:       "Existing Company Updated",
		Identifier: existing.Identifier,
		UpdatedAt:  time.Now(),
	}

	found, err := companyRepo.GetOrCreate(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, existing.ID, found.ID)
	assert.Equal(t, "Existing Company", found.Name)
}

func TestRepository_Upsert_UsesRecordLookupResolverWhenIDAndIdentifierMissing(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("company_id", "=", record.CompanyID.String()),
			SelectBy("name", "=", record.Name),
		}
	}))

	existing := &TestUser{
		ID:        uuid.New(),
		Name:      "Composite User",
		Email:     "composite.old@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	existing, err := userRepo.CreateTx(ctx, db, existing)
	assert.NoError(t, err)

	payload := &TestUser{
		Name:      existing.Name,
		Email:     "composite.new@example.com",
		CompanyID: existing.CompanyID,
		UpdatedAt: time.Now(),
	}

	upserted, err := userRepo.Upsert(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, existing.ID, upserted.ID)
	assert.Equal(t, payload.Email, upserted.Email)
}

func TestRepository_GetOrCreate_UsesRecordLookupResolver(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("company_id", "=", record.CompanyID.String()),
			SelectBy("name", "=", record.Name),
		}
	}))

	existing := &TestUser{
		ID:        uuid.New(),
		Name:      "GetOrCreate Composite",
		Email:     "goc.composite.old@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	existing, err := userRepo.CreateTx(ctx, db, existing)
	assert.NoError(t, err)

	payload := &TestUser{
		Name:      existing.Name,
		Email:     "goc.composite.new@example.com",
		CompanyID: existing.CompanyID,
		UpdatedAt: time.Now(),
	}

	found, err := userRepo.GetOrCreate(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, existing.ID, found.ID)
	assert.Equal(t, existing.Email, found.Email)
}

func TestRepository_RecordLookupResolver_PriorityAfterID(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	resolverCalls := 0
	userRepo := newTestUserRepositoryWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		resolverCalls++
		return []SelectCriteria{
			SelectBy("name", "=", "resolver-target"),
		}
	}))

	primary := &TestUser{
		ID:        uuid.New(),
		Name:      "Primary",
		Email:     "priority.id.primary@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	primary, err := userRepo.CreateTx(ctx, db, primary)
	assert.NoError(t, err)

	resolverTarget := &TestUser{
		ID:        uuid.New(),
		Name:      "resolver-target",
		Email:     "priority.id.resolver@example.com",
		CompanyID: primary.CompanyID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	_, err = userRepo.CreateTx(ctx, db, resolverTarget)
	assert.NoError(t, err)

	payload := &TestUser{
		ID:        primary.ID,
		Name:      "Updated By ID",
		Email:     "priority.id.updated@example.com",
		CompanyID: primary.CompanyID,
		UpdatedAt: time.Now(),
	}

	upserted, err := userRepo.Upsert(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, primary.ID, upserted.ID)
	assert.Equal(t, 0, resolverCalls)
}

func TestRepository_RecordLookupResolver_PriorityAfterIdentifier(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	resolverCalls := 0
	userRepo := newTestUserRepositoryWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		resolverCalls++
		return []SelectCriteria{
			SelectBy("name", "=", "resolver-target"),
		}
	}))

	identifierTarget := &TestUser{
		ID:        uuid.New(),
		Name:      "Identifier Target",
		Email:     "priority.identifier.primary@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	identifierTarget, err := userRepo.CreateTx(ctx, db, identifierTarget)
	assert.NoError(t, err)

	resolverTarget := &TestUser{
		ID:        uuid.New(),
		Name:      "resolver-target",
		Email:     "priority.identifier.resolver@example.com",
		CompanyID: identifierTarget.CompanyID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	_, err = userRepo.CreateTx(ctx, db, resolverTarget)
	assert.NoError(t, err)

	payload := &TestUser{
		Name:      "Updated By Identifier",
		Email:     identifierTarget.Email,
		CompanyID: identifierTarget.CompanyID,
		UpdatedAt: time.Now(),
	}

	upserted, err := userRepo.Upsert(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, identifierTarget.ID, upserted.ID)
	assert.Equal(t, 0, resolverCalls)
}

func TestRepository_RecordLookupResolver_EmptyCriteriaIsNoOp(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		return nil
	}))

	existing := &TestUser{
		ID:        uuid.New(),
		Name:      "NoOp User",
		Email:     "noop.existing@example.com",
		CompanyID: uuid.New(),
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	existing, err := userRepo.CreateTx(ctx, db, existing)
	assert.NoError(t, err)

	payload := &TestUser{
		Name:      existing.Name,
		Email:     "noop.created@example.com",
		CompanyID: existing.CompanyID,
		UpdatedAt: time.Now(),
	}

	upserted, err := userRepo.Upsert(ctx, payload)
	assert.NoError(t, err)
	assert.NotEqual(t, existing.ID, upserted.ID)

	records, _, err := userRepo.List(ctx,
		SelectBy("company_id", "=", existing.CompanyID.String()),
		SelectBy("name", "=", existing.Name),
	)
	assert.NoError(t, err)
	assert.Len(t, records, 2)
}

func TestRepository_RecordLookupResolver_ErrorPropagation(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		return []SelectCriteria{
			SelectBy("column_that_does_not_exist", "=", "value"),
		}
	}))

	_, err := userRepo.Upsert(ctx, &TestUser{
		Name:      "Resolver Error User",
		Email:     "resolver.error@example.com",
		CompanyID: uuid.New(),
		UpdatedAt: time.Now(),
	})
	assert.Error(t, err)
	assert.False(t, IsRecordNotFound(err))
	assert.True(t, goerrors.IsCategory(err, CategoryDatabase))
}

func TestRepository_RecordLookupResolver_DeterministicSelection(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("company_id", "=", record.CompanyID.String()),
		}
	}))

	companyID := uuid.New()
	lowID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	highID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	low := &TestUser{
		ID:        lowID,
		Name:      "Low ID",
		Email:     "deterministic.low@example.com",
		CompanyID: companyID,
		CreatedAt: time.Now().Add(-2 * time.Hour),
		UpdatedAt: time.Now().Add(-2 * time.Hour),
	}
	high := &TestUser{
		ID:        highID,
		Name:      "High ID",
		Email:     "deterministic.high@example.com",
		CompanyID: companyID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	_, err := userRepo.CreateTx(ctx, db, high)
	assert.NoError(t, err)
	_, err = userRepo.CreateTx(ctx, db, low)
	assert.NoError(t, err)

	mark := time.Now()
	upserted, err := userRepo.Upsert(ctx, &TestUser{
		Name:      "Deterministically Updated",
		CompanyID: companyID,
		UpdatedAt: mark,
	}, UpdateColumns("name", "updated_at"))
	assert.NoError(t, err)
	assert.Equal(t, lowID, upserted.ID)

	lowReloaded, err := userRepo.GetByID(ctx, lowID.String())
	assert.NoError(t, err)
	highReloaded, err := userRepo.GetByID(ctx, highID.String())
	assert.NoError(t, err)
	assert.Equal(t, "Deterministically Updated", lowReloaded.Name)
	assert.Equal(t, "High ID", highReloaded.Name)
}

func TestRepository_GetOrCreate_ResolverUsedOnDuplicateRetryPath(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("email", "=", record.Email),
		}
	}))
	repoImpl := userRepo.(*repo[*TestUser])
	repoImpl.resetScopes()

	const blockerScope = "resolver-insert-blocker"
	repoImpl.RegisterScope(blockerScope, ScopeDefinition{
		Insert: func(ctx context.Context) []InsertCriteria {
			val, ok := ScopeData(ctx, blockerScope)
			if !ok {
				return nil
			}
			blocker, ok := val.(*insertBlocker)
			if !ok || blocker == nil {
				return nil
			}
			return []InsertCriteria{
				func(q *bun.InsertQuery) *bun.InsertQuery {
					blocker.ready <- struct{}{}
					<-blocker.proceed
					return q
				},
			}
		},
	})
	assert.NoError(t, repoImpl.SetScopeDefaults(ScopeDefaults{
		Insert: []string{blockerScope},
	}))

	blocker := &insertBlocker{
		ready:   make(chan struct{}, 1),
		proceed: make(chan struct{}, 1),
	}
	scopeCtx := WithScopeData(ctx, blockerScope, blocker)

	now := time.Now()
	record := &TestUser{
		ID:        uuid.New(),
		Name:      "Resolver Retry",
		Email:     "resolver.retry@example.com",
		CompanyID: uuid.New(),
		CreatedAt: now,
		UpdatedAt: now,
	}

	var (
		result  *TestUser
		callErr error
		wg      sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := userRepo.GetOrCreateTx(scopeCtx, db, record)
		result = res
		callErr = err
	}()

	<-blocker.ready

	manual := &TestUser{
		ID:        uuid.New(),
		Name:      "Manual Insert",
		Email:     record.Email,
		CompanyID: record.CompanyID,
		CreatedAt: now.Add(1 * time.Millisecond),
		UpdatedAt: now.Add(1 * time.Millisecond),
	}
	_, err := userRepo.CreateTx(ctx, db, manual)
	assert.NoError(t, err)

	blocker.proceed <- struct{}{}
	wg.Wait()

	assert.NoError(t, callErr)
	if assert.NotNil(t, result) {
		assert.Equal(t, manual.ID, result.ID)
		assert.Equal(t, manual.Email, result.Email)
	}
}

func TestRepository_UpsertMany_UsesRecordLookupResolver(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("company_id", "=", record.CompanyID.String()),
			SelectBy("name", "=", record.Name),
		}
	}))

	companyID := uuid.New()
	first := &TestUser{
		ID:        uuid.New(),
		Name:      "Batch User A",
		Email:     "batch.a.old@example.com",
		CompanyID: companyID,
		CreatedAt: time.Now().Add(-2 * time.Hour),
		UpdatedAt: time.Now().Add(-2 * time.Hour),
	}
	second := &TestUser{
		ID:        uuid.New(),
		Name:      "Batch User B",
		Email:     "batch.b.old@example.com",
		CompanyID: companyID,
		CreatedAt: time.Now().Add(-2 * time.Hour),
		UpdatedAt: time.Now().Add(-2 * time.Hour),
	}
	_, err := userRepo.CreateTx(ctx, db, first)
	assert.NoError(t, err)
	_, err = userRepo.CreateTx(ctx, db, second)
	assert.NoError(t, err)

	now := time.Now()
	payload := []*TestUser{
		{
			Name:      first.Name,
			CompanyID: first.CompanyID,
			UpdatedAt: now,
		},
		{
			Name:      second.Name,
			CompanyID: second.CompanyID,
			UpdatedAt: now,
		},
	}

	records, err := userRepo.UpsertMany(ctx, payload, UpdateColumns("updated_at"))
	assert.NoError(t, err)
	assert.Len(t, records, 2)
	assert.Equal(t, first.ID, records[0].ID)
	assert.Equal(t, second.ID, records[1].ID)
}

func TestRepository_RecordLookupResolver_RespectsSelectScopes(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	companyRepo := newTestCompanyRepository(db)
	userRepo := newTestUserRepositoryWithoutIdentifierWithConfig(db, nil, WithRecordLookupResolver(func(record *TestUser) []SelectCriteria {
		if record == nil {
			return nil
		}
		return []SelectCriteria{
			SelectBy("name", "=", record.Name),
		}
	}))
	userRepo.(*repo[*TestUser]).resetScopes()

	const tenantScope = "tenant"
	userRepo.RegisterScope(tenantScope, ScopeByField(tenantScope, "company_id"))
	assert.NoError(t, userRepo.SetScopeDefaults(ScopeDefaults{
		Select: []string{tenantScope},
	}))

	tenantCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Tenant Co",
		Identifier: "tenant-co-resolver-scope",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	otherCompany := &TestCompany{
		ID:         uuid.New(),
		Name:       "Other Co",
		Identifier: "other-co-resolver-scope",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	_, err := companyRepo.CreateTx(ctx, db, tenantCompany)
	assert.NoError(t, err)
	_, err = companyRepo.CreateTx(ctx, db, otherCompany)
	assert.NoError(t, err)

	tenantUser := &TestUser{
		ID:        uuid.New(),
		Name:      "Scoped Resolver Name",
		Email:     "scope.tenant@example.com",
		CompanyID: tenantCompany.ID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	otherUser := &TestUser{
		ID:        uuid.New(),
		Name:      tenantUser.Name,
		Email:     "scope.other@example.com",
		CompanyID: otherCompany.ID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}
	_, err = userRepo.CreateTx(ctx, db, tenantUser)
	assert.NoError(t, err)
	_, err = userRepo.CreateTx(ctx, db, otherUser)
	assert.NoError(t, err)

	updatedAt := time.Now()
	scopeCtx := WithScopeData(ctx, tenantScope, tenantCompany.ID)
	upserted, err := userRepo.Upsert(scopeCtx, &TestUser{
		Name:      tenantUser.Name,
		CompanyID: tenantCompany.ID,
		UpdatedAt: updatedAt,
	}, UpdateColumns("updated_at"))
	assert.NoError(t, err)
	assert.Equal(t, tenantUser.ID, upserted.ID)

	unscopedCtx := WithoutDefaultScopes(ctx)
	reloadedTenant, err := userRepo.GetByID(unscopedCtx, tenantUser.ID.String())
	assert.NoError(t, err)
	reloadedOther, err := userRepo.GetByID(unscopedCtx, otherUser.ID.String())
	assert.NoError(t, err)

	assert.True(t, reloadedTenant.UpdatedAt.Equal(updatedAt))
	assert.False(t, reloadedOther.UpdatedAt.Equal(updatedAt))
}

func TestRepository_WithRecordLookupResolver_TypeMismatchValidation(t *testing.T) {
	setupTestData(t)

	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "email"
		},
		GetIdentifierValue: func(record *TestUser) string {
			return record.Email
		},
	}

	mismatched := NewRepositoryWithConfig[*TestUser](db, handlers, nil, WithRecordLookupResolver(func(record *TestCompany) []SelectCriteria {
		return []SelectCriteria{SelectBy("identifier", "=", record.Identifier)}
	}))

	validator, ok := mismatched.(Validator)
	if !assert.True(t, ok) {
		return
	}

	err := validator.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolver type mismatch")

	validationErrors, hasValidationErrors := goerrors.GetValidationErrors(err)
	assert.True(t, hasValidationErrors)
	assert.NotEmpty(t, validationErrors)
}

func TestRepository_WithRecordLookupResolver_TypeMismatchFailsOperations(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "email"
		},
		GetIdentifierValue: func(record *TestUser) string {
			return record.Email
		},
	}

	userRepo := NewRepositoryWithConfig[*TestUser](db, handlers, nil, WithRecordLookupResolver(func(record *TestCompany) []SelectCriteria {
		return []SelectCriteria{SelectBy("identifier", "=", record.Identifier)}
	}))

	_, err := userRepo.Upsert(ctx, &TestUser{
		Name:      "Mismatch",
		Email:     "mismatch@example.com",
		CompanyID: uuid.New(),
		UpdatedAt: time.Now(),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolver type mismatch")
}

func TestRepository_MustNewRepositoryWithConfig_RecordLookupResolverTypeMismatchPanics(t *testing.T) {
	setupTestData(t)

	handlers := ModelHandlers[*TestUser]{
		NewRecord: func() *TestUser {
			return &TestUser{}
		},
		GetID: func(record *TestUser) uuid.UUID {
			return record.ID
		},
		SetID: func(record *TestUser, id uuid.UUID) {
			record.ID = id
		},
		GetIdentifier: func() string {
			return "email"
		},
		GetIdentifierValue: func(record *TestUser) string {
			return record.Email
		},
	}

	assert.Panics(t, func() {
		MustNewRepositoryWithConfig[*TestUser](db, handlers, nil, WithRecordLookupResolver(func(record *TestCompany) []SelectCriteria {
			return []SelectCriteria{SelectBy("identifier", "=", record.Identifier)}
		}))
	})
}

func TestRepository_List(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	now := time.Now()
	for i := 1; i <= 30; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	users, total, err := userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Should return legacy default limit of 25 users")
	assert.Equal(t, 30, total, "Total should reflect all records")

	// Test List with custom limit and offset
	criteria := SelectPaginate(10, 5)
	users, total, err = userRepo.List(ctx, criteria)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(users), "Should return 10 users")
	assert.Equal(t, 30, total, "Total should still reflect all records")

	assert.Equal(t, "User 6", users[0].Name, "First user should be 'User 6'")
	assert.Equal(t, "User 15", users[9].Name, "Last user should be 'User 15'")

	// Test List with selection criteria
	criteria = func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where("email LIKE ?", "user1%")
	}
	users, total, err = userRepo.List(ctx, criteria)
	assert.NoError(t, err)
	assert.Equal(t, 11, len(users), "Should return users with emails starting with 'user1'")
	assert.Equal(t, 11, total, "Total should reflect matching records")
}

func TestRepository_ListTx(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	now := time.Now()
	for i := 1; i <= 30; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	for i := 31; i <= 35; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, tx, user)
		assert.NoError(t, err)
	}

	users, total, err := userRepo.ListTx(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Should return legacy default limit of 25 users in tx")
	assert.Equal(t, 35, total, "Total should include records in transaction")

	err = tx.Commit()
	assert.NoError(t, err)

	// Verify that the new records are persisted
	users, total, err = userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Should return legacy default limit of 25 persisted users")
	assert.Equal(t, 35, total, "Total should reflect all records")
}

func TestRepository_List_WithConfigNoDefaultPagination(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithConfig(db, nil)

	now := time.Now()
	for i := 1; i <= 30; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	users, total, err := userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 30, len(users), "With config constructor and no repo defaults, list should return all users")
	assert.Equal(t, 30, total, "Total should reflect all records")

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	users, total, err = userRepo.ListTx(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, 30, len(users), "With config constructor and no repo defaults, ListTx should return all users")
	assert.Equal(t, 30, total, "ListTx total should reflect all records")
}

func TestRepository_List_DefaultPaginationConfigured(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithConfig(db, nil, WithDefaultListPagination(25, 0))

	now := time.Now()
	for i := 1; i <= 30; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	users, total, err := userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Configured default pagination should cap list results")
	assert.Equal(t, 30, total, "Total should reflect all records")

	users, total, err = userRepo.List(ctx, SelectPaginate(10, 5))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(users), "Explicit pagination should override configured defaults")
	assert.Equal(t, 30, total, "Total should remain full count")
	assert.Equal(t, "User 6", users[0].Name, "First user should be 'User 6'")
	assert.Equal(t, "User 15", users[9].Name, "Last user should be 'User 15'")

	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	users, total, err = userRepo.ListTx(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Configured default pagination should apply to ListTx")
	assert.Equal(t, 30, total, "ListTx total should remain full count")

	users, total, err = userRepo.ListTx(ctx, tx, SelectPaginate(8, 4))
	assert.NoError(t, err)
	assert.Equal(t, 8, len(users), "Explicit ListTx pagination should override configured defaults")
	assert.Equal(t, 30, total, "ListTx total should remain full count")
}

func TestRepository_SetDefaultListPagination(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepositoryWithConfig(db, nil)

	now := time.Now()
	for i := 1; i <= 30; i++ {
		user := &TestUser{
			ID:        uuid.New(),
			Name:      fmt.Sprintf("User %d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CompanyID: uuid.New(),
			CreatedAt: now,
			UpdatedAt: now,
		}
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	users, total, err := userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 30, len(users))
	assert.Equal(t, 30, total)

	paginator, ok := userRepo.(DefaultListPaginationConfigurer)
	assert.True(t, ok, "expected repository to implement DefaultListPaginationConfigurer")
	if !ok {
		return
	}

	paginator.SetDefaultListPagination(12, 2)

	users, total, err = userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 12, len(users))
	assert.Equal(t, 30, total)

	users, total, err = userRepo.List(ctx, SelectPaginate(10, 5))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(users), "Explicit pagination should override runtime default pagination")
	assert.Equal(t, 30, total)

	paginator.SetDefaultListPagination(0, 0)

	users, total, err = userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 30, len(users), "Setting limit <= 0 should disable default pagination")
	assert.Equal(t, 30, total)
}

func TestRepository_GetModelFields_InvalidatesPerModelType(t *testing.T) {
	setupTestData(t)

	toggle := false

	handlers := ModelHandlers[any]{
		NewRecord: func() any {
			if toggle {
				return &TestCompany{}
			}
			return &TestUser{}
		},
		GetID: func(record any) uuid.UUID {
			return uuid.Nil
		},
		SetID: func(record any, id uuid.UUID) {},
	}

	rawRepo := NewRepository(db, handlers)
	concreteRepo := rawRepo.(*repo[any])

	fieldsUser := concreteRepo.GetModelFields()
	toggle = true
	fieldsCompany := concreteRepo.GetModelFields()

	var hasEmail, hasIdentifier bool
	for _, field := range fieldsUser {
		if field.Name == "Email" || field.Name == "email" {
			hasEmail = true
			break
		}
	}
	for _, field := range fieldsCompany {
		if field.Name == "Identifier" || field.Name == "identifier" {
			hasIdentifier = true
			break
		}
	}

	assert.True(t, hasEmail, "expected user fields to include Email")
	assert.True(t, hasIdentifier, "expected company fields to include Identifier")
}

func setupTestData(t *testing.T) {
	ctx := context.Background()

	// Drop existing tables
	if err := dropSchema(ctx, db); err != nil {
		t.Fatalf("Failed to drop tables: %v", err)
	}

	// Create tables
	if err := createSchema(ctx, db); err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}
}

func createSchema(ctx context.Context, db *bun.DB) error {
	models := []any{
		(*TestCompany)(nil),
		(*TestUser)(nil),
	}

	for _, model := range models {
		if _, err := db.NewCreateTable().Model(model).Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func dropSchema(ctx context.Context, db *bun.DB) error {
	models := []any{
		(*TestUser)(nil),
		(*TestCompany)(nil),
	}

	for _, model := range models {
		if _, err := db.NewDropTable().Model(model).IfExists().Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

type insertBlocker struct {
	ready   chan struct{}
	proceed chan struct{}
}
