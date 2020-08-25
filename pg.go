package gob

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type pg struct {
	*pgxpool.Pool
}

func newPg(connStr string) (db, error) {
	pool, err := pgxpool.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("gob: connect to PostgreSQL server: %w", err)
	}

	return &pg{Pool: pool}, nil
}

func (db *pg) close() {
	db.Close()
}

func (db *pg) upsert(ctx context.Context, upsertArgs UpsertArgs) error {
	if len(upsertArgs.Rows) == 0 {
		return nil
	}

	if len(upsertArgs.Keys) == 0 {
		return ErrEmptykeys
	}

	// start transaction
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return fmt.Errorf("gob: begin PostgreSQL tx: %w", err)
	}

	for _, row := range upsertArgs.Rows {
		if row.Len() == 0 {
			continue // ignore empty row
		}
		sql, args := db.rowToSQL(row, upsertArgs)
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("gob: execute upsert sql '%s' on PostgreSQL server: %w", sql, err)
		}
	}

	// commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("gob: commit PostgreSQL tx: %w", err)
	}

	return nil
}

func (db *pg) rowToSQL(row Row, upsertArgs UpsertArgs) (sql string, args []interface{}) {
	upsertSQL := "INSERT INTO %s(%s) VALUES(%s) ON CONFLICT (%s) %s"

	var (
		cols         []string
		values       []string
		updateClause []string
		action       string
		count        = 1
	)

	for _, column := range row.Columns() {
		cols = append(cols, column)
		values = append(values, fmt.Sprintf("$%d", count))
		if !upsertArgs.KeySet.Contains(column) {
			updateClause = append(updateClause, fmt.Sprintf("%s=$%d", column, count))
		}
		args = append(args, row.Value(column))
		count = count + 1
	}

	switch upsertArgs.ConflictAction {
	case ConflictActionUpdate:
		action = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updateClause, ","))
	default:
		action = "DO NOTHING"
	}

	sql = fmt.Sprintf(upsertSQL,
		upsertArgs.Model,
		strings.Join(cols, ","),
		strings.Join(values, ","),
		strings.Join(upsertArgs.Keys, ","),
		action,
	)

	return sql, args
}
