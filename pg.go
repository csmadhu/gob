package gob

import (
	"context"
	"fmt"
	"strings"

	"github.com/csmadhu/gob/utils"

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

func (db *pg) upsert(ctx context.Context, table string, keyColumns utils.StringSet, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	// start transaction
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return fmt.Errorf("gob: begin PostgreSQL tx: %w", err)
	}

	for _, row := range rows {
		if row.Len() == 0 {
			continue // ignore empty row
		}
		sql, args := db.rowToSQL(table, keyColumns, row)
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

func (db *pg) rowToSQL(table string, keyColumns utils.StringSet, row Row) (sql string, args []interface{}) {
	upsertSQL := "INSERT INTO %s(%s) VALUES(%s) ON CONFLICT (%s) DO UPDATE SET %s"
	var (
		cols         []string
		values       []string
		updateClause []string
		count        = 1
	)

	for _, column := range row.Columns() {
		cols = append(cols, column)
		values = append(values, fmt.Sprintf("$%d", count))
		if !keyColumns.Contains(column) {
			updateClause = append(updateClause, fmt.Sprintf("%s=$%d", column, count))
		}
		args = append(args, row.Value(column))
		count = count + 1
	}

	sql = fmt.Sprintf(upsertSQL,
		table,
		strings.Join(cols, ","),
		strings.Join(values, ","),
		strings.Join(keyColumns.ToSlice(), ","),
		strings.Join(updateClause, ","))

	return sql, args
}
