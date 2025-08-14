# Generic Repository with Go Generics and Bun ORM

A generic implementation of a data access layer using Go generics and [Bun ORM](https://bun.uptrace.dev/).

## Features

- Generic repository pattern implementation
- CRUD operations with transactional support
- Dynamic query building with criteria functions
- Custom identifiers and UUID support
- Soft delete capabilities
- Raw query execution
- Bulk operations (`CreateMany`, `UpdateMany`, `UpsertMany`)
- `GetOrCreate` and `Upsert` convenience methods
- Multi-database support (PostgreSQL, SQLite, MSSQL, MySQL)
- Sophisticated error handling with categorized errors
- Model metadata extraction and field introspection
- Transaction management utilities
- Comprehensive test coverage

## Installation

```sh
go get github.com/goliatone/go-repository-bun
```

## Usage

### Import the package

```go
import (
    repository "github.com/goliatone/go-repository-bun"
    "github.com/uptrace/bun"
    "github.com/google/uuid"
)
```

### Defining Models

```go
type User struct {
    bun.BaseModel `bun:"table:users,alias:u"`

    ID        uuid.UUID  `bun:"id,pk,notnull"`
    Name      string     `bun:"name,notnull"`
    Email     string     `bun:"email,notnull,unique"`
    DeletedAt *time.Time `bun:"deleted_at,soft_delete"`
    CreatedAt time.Time  `bun:"created_at,notnull"`
    UpdatedAt time.Time  `bun:"updated_at,notnull"`
}
```

### Creating Repository

```go
// Define handlers for your model
handlers := repository.ModelHandlers[*User]{
    NewRecord: func() *User {
        return &User{}
    },
    GetID: func(record *User) uuid.UUID {
        if record == nil {
            return uuid.Nil
        }
        return record.ID
    },
    SetID: func(record *User, id uuid.UUID) {
        record.ID = id
    },
    GetIdentifier: func() string {
        return "email" // Unique identifier field
    },
}

// Create repository instance
userRepo := repository.NewRepository[*User](db, handlers)
```

### Basic Operations

```go
// Create
user := &User{Name: "John Doe", Email: "john.doe@example.com"}
created, err := userRepo.Create(ctx, user)

// Retrieve by ID
user, err := userRepo.GetByID(ctx, "user-uuid")

// Retrieve by identifier (email in this case)
user, err := userRepo.GetByIdentifier(ctx, "john.doe@example.com")

// Update
user.Name = "Jane Doe"
updated, err := userRepo.Update(ctx, user)

// Delete (soft delete if DeletedAt field exists)
err := userRepo.Delete(ctx, user)

// Force delete (permanent delete)
err := userRepo.ForceDelete(ctx, user)
```

### Bulk Operations

```go
// Create multiple records
users := []*User{
    {Name: "User 1", Email: "user1@example.com"},
    {Name: "User 2", Email: "user2@example.com"},
}
created, err := userRepo.CreateMany(ctx, users)

// Update multiple records
updated, err := userRepo.UpdateMany(ctx, users)

// Upsert multiple records
upserted, err := userRepo.UpsertMany(ctx, users)
```

### Convenience Methods

```go
// Get or create
user := &User{Email: "new@example.com", Name: "New User"}
result, err := userRepo.GetOrCreate(ctx, user)

// Upsert (update if exists, create if not)
user := &User{ID: someID, Name: "Updated Name", Email: "email@example.com"}
result, err := userRepo.Upsert(ctx, user)
```

### Query Criteria

```go
// List with pagination
users, total, err := userRepo.List(ctx,
    repository.SelectPaginate(10, 0),
    repository.OrderBy("created_at DESC"),
)

// Complex queries
users, total, err := userRepo.List(ctx,
    repository.SelectBy("status", "=", "active"),
    repository.SelectColumns("id", "name", "email"),
    repository.SelectRelation("Profile"),
    repository.WhereGroup(func(q *bun.SelectQuery) *bun.SelectQuery {
        return q.Where("created_at > ?", time.Now().Add(-24*time.Hour)).
                WhereOr("updated_at > ?", time.Now().Add(-1*time.Hour))
    }),
)

// Count records
count, err := userRepo.Count(ctx,
    repository.SelectBy("status", "=", "active"),
)

// Delete with criteria
err := userRepo.DeleteWhere(ctx,
    repository.DeleteBy("status", "=", "inactive"),
    repository.DeleteBefore("created_at", time.Now().Add(-365*24*time.Hour)),
)
```

### Transactions

```go
// Using manual transactions
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

user, err := userRepo.CreateTx(ctx, tx, user)
if err != nil {
    return err
}

// All repository methods have Tx variants
updated, err := userRepo.UpdateTx(ctx, tx, user)
if err != nil {
    return err
}

err = tx.Commit()
```

### Raw Queries

```go
// Execute raw SQL queries
users, err := userRepo.Raw(ctx,
    "SELECT * FROM users WHERE created_at > ? AND status = ?",
    time.Now().Add(-24*time.Hour),
    "active",
)
```

### Error Handling

The package provides categorized errors for better error handling:

```go
err := userRepo.Create(ctx, user)
if err != nil {
    if repository.IsRecordNotFound(err) {
        // Handle not found
    }
    if repository.IsDuplicateKeyError(err) {
        // Handle duplicate
    }
    if repository.IsConstraintViolation(err) {
        // Handle constraint violation
    }
    // Other error categories available
}
```

## Advanced Features

### Model Metadata

The package provides utilities to extract model metadata and field information:

```go
// Get model fields with database metadata
fields := repository.GetModelFields(db, &User{})
for _, field := range fields {
    fmt.Printf("Field: %s, PK: %v, Type: %s\n", field.Name, field.IsPK, field.SQLType)
}

// Generate complete model metadata including JSON tags and validations
meta := repository.GenerateModelMeta(&User{})
fmt.Printf("Table: %s\n", meta.TableName)
for _, field := range meta.Fields {
    fmt.Printf("Field: %s, Required: %v, Unique: %v\n", 
        field.Name, field.IsRequired, field.IsUnique)
}
```

### Transaction Management

The package includes a `TransactionManager` interface for managing database transactions:

```go
type TransactionManager interface {
    RunInTx(ctx context.Context, opts *sql.TxOptions, 
        f func(ctx context.Context, tx bun.Tx) error) error
}
```

## Database Support

The repository automatically detects and adapts to different database drivers:
- PostgreSQL
- SQLite
- MSSQL
- MySQL

Database driver detection is handled automatically via the `DetectDriver` function.

## Project Structure

- `repo.go` - Main repository implementation
- `errors.go` - Custom error types and handlers
- `meta.go` - Model metadata extraction utilities
- `types.go` - Type definitions and interfaces
- `utils.go` - Utility functions including `DetectDriver`
- `query_*_criteria.go` - Query builder criteria functions
- `examples/` - Example usage and model definitions

## License

MIT
