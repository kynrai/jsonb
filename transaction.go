package jsonb

import (
	"context"

	"github.com/jackc/pgx/v4"
)

type contextKey string

const (
	txKey contextKey = `transaction`
)

func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

func TxFromContext(ctx context.Context) pgx.Tx {
	if tx, ok := ctx.Value(txKey).(pgx.Tx); ok {
		return tx
	}
	return nil
}
