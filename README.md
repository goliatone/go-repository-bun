# Generic Repository with Go Generics and Bun ORM

A generic implementation of a data access layer using Go generics and [Bun ORM](https://bun.uptrace.dev/).

## Features

- Generic repository pattern implementation
- CRUD operations with transactional support
- Dynamic query building with criteria functions
- Custom identifiers and UUID support
- Soft delete capabilities
- Raw query execution
- Comprehensive test coverage

## Installation

```sh
go get github.com/goliatone/go-repository-bun
```

## Usage

### Defining Models

```go
type User struct {
    bun.BaseModel `bun:"table:users,alias:u"`

    ID        uuid.UUID `bun:"id,pk,notnull"`
    Name      string    `bun:"name,notnull"`
    Email     string    `bun:"email,notnull,unique"`
    CreatedAt time.Time `bun:"created_at,notnull"`
    UpdatedAt time.Time `bun:"updated_at,notnull"`
}
```

### Creating Repository

```go
// Define handlers for your model
handlers := ModelHandlers[*User]{
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
userRepo := NewRepository[*User](db, handlers)
```

### Basic Operations

```go
// Create
user := &User{Name: "John Doe", Email: "john.doe@example.com"}
created, err := userRepo.CreateTx(ctx, db, user)

// Retrieve by ID
user, err := userRepo.GetByIDTx(ctx, db, "user-uuid")

// Update
user.Name = "Jane Doe"
updated, err := userRepo.UpdateTx(ctx, db, user)

// Delete
err := userRepo.DeleteTx(ctx, db, user)
```

### Advanced Operations

#### Transactions

```go
tx, err := db.BeginTx(ctx, nil)
defer tx.Rollback()

user, err := userRepo.CreateTx(ctx, tx, user)
if err != nil {
    return err
}

err = tx.Commit()
```

#### Custom Criteria

```go
// Select with criteria
user, err := userRepo.Get(ctx,
    SelectBy("name", "=", "John"),
    SelectColumns("id", "name", "email"),
    OrderBy("created_at DESC"),
)

// Update with criteria
user, err := userRepo.Update(ctx, user,
    UpdateColumns("name", "email"),
    UpdateBy("status", "=", "active"),
)
```

#### Raw Queries

```go
users, err := userRepo.Raw(ctx,
    "SELECT * FROM users WHERE created_at > ?",
    time.Now().Add(-24*time.Hour),
)
```

## License

MIT
