package gob

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/csmadhu/gob/utils"
)

var (
	// ErrConnClosed when Gob.Upsert is called after closing the connection
	ErrConnClosed = errors.New("gob: conn closed;")

	// ErrEmptyModel when Gob.Upsert is called with empty model
	ErrEmptyModel = errors.New("gob: empty model;")
)

var (
	defaultBatchSize = 10000
	defaultDBType    = DBTypePg
	defaultConnStr   = "postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1"
)

// Gob provides APIs to upsert data in bulk
type Gob struct {
	batchSize int    // upsert rows in batches
	dbType    string // type of database
	connStr   string // database conn string

	db              // connection handler to database
	dbMu sync.Mutex // mutex to synchornize connection handler
}

// New returns Gob instance customized with options
func New(options ...Option) (*Gob, error) {
	gob := &Gob{
		batchSize: defaultBatchSize,
		dbType:    defaultDBType,
		connStr:   defaultConnStr,
	}

	for _, option := range options {
		if err := option(gob); err != nil {
			return nil, err
		}
	}

	var err error
	switch gob.dbType {
	case DBTypePg:
		gob.db, err = newPg(gob.connStr)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("gob: invalid dbType: %s", gob.dbType)
	}

	return gob, nil
}

func (gob *Gob) setBatchSize(size int) {
	gob.batchSize = size
}

func (gob *Gob) setDBType(dbType string) {
	gob.dbType = dbType
}

func (gob *Gob) setConnStr(connStr string) {
	gob.connStr = connStr
}

func (gob *Gob) getDB() db {
	gob.dbMu.Lock()
	defer gob.dbMu.Unlock()
	return gob.db
}

// Upsert rows to model
// keyColumns of model is reuqired to resolve confict during insert
func (gob *Gob) Upsert(ctx context.Context, model string, keyColumns []string, rows []Row) error {
	var gobDB db
	gobDB = gob.getDB()
	if gobDB == nil {
		return ErrConnClosed
	}

	if model == "" {
		return ErrEmptyModel
	}

	if len(keyColumns) == 0 {
		return fmt.Errorf("gob: empty keyColumns for model: %s", model)
	}

	var (
		start = 0
		end   = gob.batchSize
		keys  = utils.NewStringSet(keyColumns...)
		t0    = time.Now()
	)

	if len(rows) <= gob.batchSize {
		end = len(rows)
	}

	for start < len(rows) {
		if err := gob.upsert(ctx, model, keys, rows[start:end]); err != nil {
			return err
		}

		start = start + gob.batchSize
		end = end + gob.batchSize
		if end > len(rows) {
			end = len(rows)
		}
	}

	log.Printf("gob: upsert %d rows to model %s in %v", len(rows), model, time.Since(t0))
	return nil
}

// Close the resources
func (gob *Gob) Close() {
	gob.dbMu.Lock()
	defer gob.dbMu.Unlock()

	gob.db.close()
	gob.db = nil
}
