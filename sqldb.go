package gob

import "database/sql"

type sqlDB struct {
	db *sql.DB
}

func newSQLDB(db *sql.DB) db {
	return &sqlDB{db: db}
}

// Insert rows to table
func (db *sqlDB) Insert(table string, rows []Row) error {
	return nil
}

// Update rows to table
func (db *sqlDB) Update(table string, rows []Row) error {
	return nil
}

// NewSQL returns Gob handler for relation database - Postgres, MySQL
//
// Gob upserts records using db connection
//
// options to customize default Gob
func NewSQL(db *sql.DB, options ...Option) *Gob {
	gob := gob(options...)
	gob.setDB(newSQLDB(db))

	return gob
}
