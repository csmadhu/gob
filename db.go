package gob

import (
	"context"
	"sort"
	"time"

	"github.com/csmadhu/gob/utils"
)

// Row of model
type Row map[string]interface{}

// NewRow return empty row
func NewRow() Row {
	return make(Row)
}

// Columns in sorted order
func (row Row) Columns() (cols []string) {
	for col := range row {
		cols = append(cols, col)
	}

	sort.Strings(cols)
	return cols
}

// Value of col nil if not found
func (row Row) Value(col string) interface{} {
	return row[col]
}

// Add column and value to row
func (row Row) Add(column string, value interface{}) {
	row[column] = value
}

// Len returns number of columns in row
func (row Row) Len() int {
	return len(row)
}

// ConflictAction specifies alternatives ON CONFLICT
type ConflictAction string

const (
	// ConflictActionNothing ignores conflict during INSERT
	ConflictActionNothing ConflictAction = "nothing"
	// ConflictActionUpdate resloves conflict by updating the row
	ConflictActionUpdate ConflictAction = "update"
)

// UpsertArgs to upsert rows
type UpsertArgs struct {
	ConflictAction                 // ON CONFLICT action
	Keys           []string        // indicate index column names
	keySet         utils.StringSet // keys converted to set
	Model          string          // table name
	Rows           []Row           // rows to be upserted
}

type db interface {
	// upsert rows to model with keys
	upsert(ctx context.Context, args UpsertArgs) error

	// close the resources
	close()
}

// DBProvider for storage
type DBProvider string

const (
	// DBProviderPg indicates relational database prvoided by PostgreSQL
	DBProviderPg DBProvider = "pg"
	// DBProviderMySQL indicates relational database provided by MySQL
	DBProviderMySQL DBProvider = "mysql"
)

type connArgs struct {
	connStr      string
	idleConns    int
	openConns    int
	connIdleTime time.Duration
	connLifeTime time.Duration
}
