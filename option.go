package gob

import (
	"fmt"
	"time"
)

// Option to customize Gob
type Option func(gob *Gob) error

// WithBatchSize sets batchSize of Gob to size
func WithBatchSize(size int) Option {
	return func(gob *Gob) error {
		if size <= 0 {
			return fmt.Errorf("gob: invalid batchSize: %d", size)
		}
		gob.setBatchSize(size)
		return nil
	}
}

// WithDBProvider sets provider of database to upsert rows
func WithDBProvider(provider DBProvider) Option {
	return func(gob *Gob) error {
		if provider == "" {
			return ErrEmptyDBProvider
		}

		gob.setDBProvider(provider)
		return nil
	}
}

// WithDBConnStr sets database conn string
func WithDBConnStr(connStr string) Option {
	return func(gob *Gob) error {
		if connStr == "" {
			return ErrEmptyConnStr
		}
		gob.setConnStr(connStr)
		return nil
	}
}

// WithConnIdleTime sets maximum amount of time conn may be idle
func WithConnIdleTime(d time.Duration) Option {
	return func(gob *Gob) error {
		gob.setConnIdleTime(d)
		return nil
	}
}

// WithConnLifeTime sets maximum amount of time conn may be reused
func WithConnLifeTime(d time.Duration) Option {
	return func(gob *Gob) error {
		gob.setConnLifeTime(d)
		return nil
	}
}

// WithIdleConns sets maximum number of connections idle in pool
func WithIdleConns(n int) Option {
	return func(gob *Gob) error {
		gob.setIdleConns(n)
		return nil
	}
}

// WithOpenConns sets maximum number of connections open to database
func WithOpenConns(n int) Option {
	return func(gob *Gob) error {
		gob.setOpenConns(n)
		return nil
	}
}
