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

	// ErrEmptykeys when Gob.Upsert is called with empty keys
	ErrEmptykeys = errors.New("gob: empty keys;")
)

var (
	defaultBatchSize  = 10000
	defaultDBProvider = DBProviderPg
	defaultConnStr    = "postgres://postgres:postgres@localhost:5432/postgres?pool_max_conns=1"
)

// Gob provides APIs to upsert data in bulk
type Gob struct {
	batchSize  int        // upsert rows in batches
	dbProvider DBProvider // provider of database
	connStr    string     // database conn string

	db              // connection handler to database
	dbMu sync.Mutex // mutex to synchornize connection handler
}

// New returns Gob instance customized with options
func New(options ...Option) (*Gob, error) {
	gob := &Gob{
		batchSize:  defaultBatchSize,
		dbProvider: defaultDBProvider,
		connStr:    defaultConnStr,
	}

	for _, option := range options {
		if err := option(gob); err != nil {
			return nil, err
		}
	}

	var err error
	switch gob.dbProvider {
	case DBProviderPg:
		gob.db, err = newPg(gob.connStr)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("gob: invalid dbProvider: %s", gob.dbProvider)
	}

	return gob, nil
}

func (gob *Gob) setBatchSize(size int) {
	gob.batchSize = size
}

func (gob *Gob) setDBProvider(dbProvider DBProvider) {
	gob.dbProvider = dbProvider
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
func (gob *Gob) Upsert(ctx context.Context, model string, keys []string, conflictAction ConflictAction, rows []Row) error {
	var gobDB db
	gobDB = gob.getDB()
	if gobDB == nil {
		return ErrConnClosed
	}

	if model == "" {
		return ErrEmptyModel
	}

	var (
		start = 0
		end   = gob.batchSize
		t0    = time.Now()
		args  UpsertArgs
	)

	args.ConflictAction = conflictAction
	args.Model = model
	args.KeySet = utils.NewStringSet(keys...)
	args.Keys = args.KeySet.ToSlice()

	if len(rows) <= gob.batchSize {
		end = len(rows)
	}

	for start < len(rows) {
		args.Rows = rows[start:end]
		if err := gob.upsert(ctx, args); err != nil {
			return err
		}

		start = start + gob.batchSize
		end = end + gob.batchSize
		if end > len(rows) {
			end = len(rows)
		}
	}

	log.Printf("gob: upsert %d rows to model '%s' in %v", len(rows), model, time.Since(t0))
	return nil
}

// Close the resources
func (gob *Gob) Close() {
	gob.dbMu.Lock()
	defer gob.dbMu.Unlock()

	gob.db.close()
	gob.db = nil
}
