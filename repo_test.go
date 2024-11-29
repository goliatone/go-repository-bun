package repository

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

// TestUser represents a user in the system.
type TestUser struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`

	ID        uuid.UUID `bun:"id,pk,notnull"`
	Name      string    `bun:"name,notnull"`
	Email     string    `bun:"email,notnull,unique"`
	CompanyID uuid.UUID `bun:"company_id,notnull"`

	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// TestCompany represents a company in the system.
type TestCompany struct {
	bun.BaseModel `bun:"table:test_companies,alias:c"`

	ID         uuid.UUID `bun:"id,pk,notnull"`
	Name       string    `bun:"name,notnull"`
	Identifier string    `bun:"identifier,notnull"`

	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Define ModelHandlers for TestUser
func newTestUserRepository(db bun.IDB) Repository[*TestUser] {
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
	}
	return NewRepository[*TestUser](db, handlers)
}

// Define ModelHandlers for TestCompany
func newTestCompanyRepository(db bun.IDB) Repository[*TestCompany] {
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
	}
	return NewRepository[*TestCompany](db, handlers)
}

var db *bun.DB

func TestMain(m *testing.M) {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	defer sqldb.Close()

	db = bun.NewDB(sqldb, sqlitedialect.New())

	// Run tests
	code := m.Run()

	os.Exit(code)
}

func TestNewRepositories(t *testing.T) {
	userRepo := newTestUserRepository(db)
	companyRepo := newTestCompanyRepository(db)
	assert.NotNil(t, userRepo)
	assert.NotNil(t, companyRepo)
}

func TestRepository_Create(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)
	companyRepo := newTestCompanyRepository(db)

	// Create a company
	company := &TestCompany{
		ID:   uuid.New(),
		Name: "Test Company",
	}
	createdCompany, err := companyRepo.CreateTx(ctx, db, company)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdCompany.ID)

	// Create a user associated with the company
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

	// Create a user
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

	// Create a user
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

	// Begin a transaction
	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	// Create a user within the transaction
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

	// Commit the transaction
	err = tx.Commit()
	assert.NoError(t, err)
}

func TestRepository_GetByIdentifier_NotFound(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	// Attempt to retrieve a user with a non-existent email
	nonExistentEmail := "non.existent@example.com"
	_, err := userRepo.GetByIdentifier(ctx, nonExistentEmail)
	assert.Error(t, err)
	assert.True(t, IsRecordNotFound(err))
}

func TestRepository_Update(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	// Create a user
	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Alice Johnson",
		Email:     "alice.johnson@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	// Update the user's email
	createdUser.Email = "alice.j@example.com"
	updatedUser, err := userRepo.UpdateTx(ctx, db, createdUser)
	assert.NoError(t, err)
	assert.Equal(t, "alice.j@example.com", updatedUser.Email)
}

func TestRepository_Delete(t *testing.T) {
	setupTestData(t)

	ctx := context.Background()
	userRepo := newTestUserRepository(db)

	// Create a user
	user := &TestUser{
		ID:        uuid.New(),
		Name:      "Bob Brown",
		Email:     "bob.brown@example.com",
		CompanyID: uuid.New(),
	}
	createdUser, err := userRepo.CreateTx(ctx, db, user)
	assert.NoError(t, err)

	// Delete the user
	err = userRepo.DeleteTx(ctx, db, createdUser)
	assert.NoError(t, err)

	// Attempt to retrieve the deleted user
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

	// Create multiple users
	users := []*TestUser{
		{ID: uuid.New(), Name: "User One", Email: "user1@example.com", CompanyID: uuid.New()},
		{ID: uuid.New(), Name: "User Two", Email: "user2@example.com", CompanyID: uuid.New()},
		{ID: uuid.New(), Name: "User Three", Email: "user3@example.com", CompanyID: uuid.New()},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	// Delete users with email containing 'user2'
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
	defer tx.Rollback() // Ensure the transaction is rolled back if not committed

	// Create a user within the transaction
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

	// Commit the transaction
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
	defer tx.Rollback() // Ensure the transaction is rolled back

	// Create a user within the transaction
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

	// Roll back the transaction
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

	// Create multiple users
	users := []*TestUser{
		{ID: uuid.New(), Name: "Raw User One", Email: "raw1@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: uuid.New(), Name: "Raw User Two", Email: "raw2@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: uuid.New(), Name: "Other User", Email: "other@example.com", CompanyID: uuid.New(), CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	for _, user := range users {
		_, err := userRepo.CreateTx(ctx, db, user)
		assert.NoError(t, err)
	}

	// Use the Raw method to retrieve users with email containing 'raw'
	query := "SELECT * FROM test_users WHERE email LIKE ?"
	rawUsers, err := userRepo.Raw(ctx, query, "raw%")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rawUsers))

	// Verify that the returned users match the condition
	for _, user := range rawUsers {
		assert.Contains(t, user.Email, "raw")
	}
}

func setupTestData(t *testing.T) {
	ctx := context.Background()

	// Create tables
	if err := createSchema(ctx, db); err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}
}

func createSchema(ctx context.Context, db *bun.DB) error {
	models := []interface{}{
		(*TestCompany)(nil),
		(*TestUser)(nil),
	}

	for _, model := range models {
		if _, err := db.NewCreateTable().Model(model).IfNotExists().Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func uuidPtr(u uuid.UUID) *uuid.UUID {
	return &u
}
