package repository

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"
)

// TransactionManager generates transactions
type TransactionManager interface {
	RunInTx(ctx context.Context, opts *sql.TxOptions, f func(ctx context.Context, tx bun.Tx) error) error
}

type SQLExecuter interface {
}

// Validator enables everything is properly
// configured
type Validator interface {
	Validate() error
	MustValidate()
}
