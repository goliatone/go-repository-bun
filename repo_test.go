package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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

func newTestUserRepository(db *bun.DB) Repository[*TestUser] {
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
	return MustNewRepository[*TestUser](db, handlers)
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
	updatedUser, err := userRepo.UpdateTx(ctx, db, payload, UpdateByID(user.ID.String()))
	assert.NoError(t, err)
	assert.Equal(t, "alice.j@example.com", updatedUser.Email)
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
	assert.Equal(t, 25, len(users), "Should return default limit of 25 users")
	assert.Equal(t, 30, total, "Total should reflect all records")

	// Test List with custom limit and offset
	criteria := func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Limit(10).Offset(5)
	}
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
	assert.Equal(t, 25, len(users), "Should return default limit of 25 users")
	assert.Equal(t, 35, total, "Total should include records in transaction")

	err = tx.Commit()
	assert.NoError(t, err)

	// Verify that the new records are persisted
	users, total, err = userRepo.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 25, len(users), "Should return default limit of 25 users")
	assert.Equal(t, 35, total, "Total should reflect all records")
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
