package gob

import (
	"context"
	"sort"

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

type db interface {
	// upsert rows to model with keys
	upsert(ctx context.Context, model string, keys utils.StringSet, rows []Row) error

	// close the resources
	close()
}

const (
	// DBTypePg indicates relational database PostgreSQL server
	DBTypePg = "pg"
	// DBTypeMySQL indicates relational database MySQL server
	DBTypeMySQL = "mysql"
)
