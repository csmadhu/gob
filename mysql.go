package gob

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	// import mysql driver
	_ "github.com/go-sql-driver/mysql"
)

type mysql struct {
	*sql.DB
}

func newMySQL(args connArgs) (db, error) {
	var (
		m   mysql
		err error
	)

	m.DB, err = sql.Open("mysql", args.connStr)
	if err != nil {
		return nil, fmt.Errorf("gob: connect to MySQL: %w", err)
	}

	if err := m.DB.Ping(); err != nil {
		return nil, fmt.Errorf("gob: ping to MySQL: %w", err)
	}

	m.DB.SetConnMaxIdleTime(args.connIdleTime)
	m.DB.SetConnMaxLifetime(args.connLifeTime)
	m.DB.SetMaxIdleConns(args.idleConns)
	m.DB.SetMaxOpenConns(args.openConns)

	return &m, nil
}

func (db *mysql) close() {
	db.Close()
}

func (db *mysql) upsert(ctx context.Context, upsertArgs UpsertArgs) error {
	// start transaction
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("gob: begin MySQL tx: %w", err)
	}

	for _, row := range upsertArgs.Rows {
		if row.Len() == 0 {
			continue // ignore empty row
		}
		sql, args := db.rowToSQL(row, upsertArgs)
		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("gob: execute upsert sql '%s' on MySQL server: %v rollback tx: %w", sql, err, rollbackErr)
			}
			return fmt.Errorf("gob: execute upsert sql '%s' on MySQL server: %w", sql, err)
		}
	}

	// commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gob: commit MySQL tx: %w", err)
	}

	return nil
}

func (db *mysql) rowToSQL(row Row, upsertArgs UpsertArgs) (sql string, args []interface{}) {
	upsertSQL := "INSERT %s INTO %s(%s) VALUES(%s) %s"

	var (
		cols         []string
		values       []string
		updateClause []string
		updateArgs   []interface{}
		ignoreAction string
		updateAction string
	)

	for _, column := range row.Columns() {
		cols = append(cols, column)
		values = append(values, "?")
		args = append(args, row.Value(column))

		updateClause = append(updateClause, fmt.Sprintf("%s=?", column))
		updateArgs = append(updateArgs, row.Value(column))
	}

	switch upsertArgs.ConflictAction {
	case ConflictActionUpdate:
		updateAction = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(updateClause, ","))
		args = append(args, updateArgs...)
	default:
		ignoreAction = "IGNORE"
	}

	sql = fmt.Sprintf(upsertSQL,
		ignoreAction,
		upsertArgs.Model,
		strings.Join(cols, ","),
		strings.Join(values, ","),
		updateAction,
	)

	// remove white space
	sql = strings.TrimSpace(sql)
	sql = strings.Join(strings.Fields(sql), " ")
	return sql, args
}
