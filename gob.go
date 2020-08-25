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

// mysql password fY5SGU=t

var (
	// ErrConnClosed when Gob.Upsert is called after closing the connection
	ErrConnClosed = errors.New("gob: conn closed;")

	// ErrEmptyModel when Gob.Upsert is called with empty model
	ErrEmptyModel = errors.New("gob: empty model;")

	// ErrEmptykeys when Gob.Upsert is called with empty keys
	ErrEmptykeys = errors.New("gob: empty keys;")
)

var (
	defaultBatchSize    = 10000
	defaultDBProvider   = DBProviderPg
	defaultIdleConns    = 2
	defaultOpenConns    = 10
	defaultConnIdleTime = 3 * time.Second
	defaultconnLifeTime = 3 * time.Second
	defaultConnStr      = "postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1"
)

// Gob provides APIs to upsert data in bulk
type Gob struct {
	batchSize    int           // upsert rows in batches
	dbProvider   DBProvider    // provider of database
	connStr      string        // database conn string
	idleConns    int           // max number of conns idle in pool
	openConns    int           // max number of conns open to database
	connIdleTime time.Duration // max amount of time conn may be idle
	connLifeTime time.Duration // max amount of time conn may be reused

	db              // connection handler to database
	dbMu sync.Mutex // mutex to synchornize connection handler
}

// New returns Gob instance customized with options
func New(options ...Option) (*Gob, error) {
	gob := &Gob{
		batchSize:    defaultBatchSize,
		dbProvider:   defaultDBProvider,
		connStr:      defaultConnStr,
		idleConns:    defaultIdleConns,
		openConns:    defaultOpenConns,
		connIdleTime: defaultConnIdleTime,
		connLifeTime: defaultconnLifeTime,
	}

	for _, option := range options {
		if err := option(gob); err != nil {
			return nil, err
		}
	}

	var (
		err  error
		args connArgs
	)

	args = connArgs{
		connStr:      gob.connStr,
		idleConns:    gob.idleConns,
		openConns:    gob.openConns,
		connIdleTime: gob.connIdleTime,
		connLifeTime: gob.connLifeTime,
	}

	switch gob.dbProvider {
	case DBProviderPg:
		gob.db, err = newPg(args)
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

func (gob *Gob) setConnIdleTime(d time.Duration) {
	gob.connIdleTime = d
}

func (gob *Gob) setConnLifeTime(d time.Duration) {
	gob.connLifeTime = d
}

func (gob *Gob) setIdleConns(n int) {
	gob.idleConns = n
}

func (gob *Gob) setOpenConns(n int) {
	gob.openConns = n
}

func (gob *Gob) getDB() db {
	gob.dbMu.Lock()
	defer gob.dbMu.Unlock()
	return gob.db
}

// Upsert rows to model
func (gob *Gob) Upsert(ctx context.Context, args UpsertArgs) error {
	var gobDB db
	gobDB = gob.getDB()
	// conn closed
	if gobDB == nil {
		return ErrConnClosed
	}

	// model not specified
	if args.Model == "" {
		return ErrEmptyModel
	}

	// zero rows
	if len(args.Rows) == 0 {
		return nil
	}

	var (
		start      = 0
		end        = gob.batchSize
		t0         = time.Now()
		upsertArgs UpsertArgs // required to avoid copy of rows
	)

	upsertArgs.ConflictAction = args.ConflictAction
	upsertArgs.Model = args.Model
	upsertArgs.keySet = utils.NewStringSet(args.Keys...)
	upsertArgs.Keys = upsertArgs.keySet.ToSlice()

	if len(args.Rows) <= gob.batchSize {
		end = len(args.Rows)
	}

	for start < len(args.Rows) {
		upsertArgs.Rows = args.Rows[start:end]
		if err := gob.upsert(ctx, upsertArgs); err != nil {
			return err
		}

		start = start + gob.batchSize
		end = end + gob.batchSize
		if end > len(args.Rows) {
			end = len(args.Rows)
		}
	}

	log.Printf("gob: upsert %d rows to model '%s' in %v", len(args.Rows), args.Model, time.Since(t0))
	return nil
}

// Close the resources
func (gob *Gob) Close() {
	gob.dbMu.Lock()
	defer gob.dbMu.Unlock()

	gob.db.close()
	gob.db = nil
}
