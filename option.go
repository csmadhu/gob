package gob

import "fmt"

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
			return fmt.Errorf("gob: invalid dbProvider: %s", provider)
		}

		gob.setDBProvider(provider)
		return nil
	}
}

// WithDBConnStr sets type of database conn string
func WithDBConnStr(connStr string) Option {
	return func(gob *Gob) error {
		if connStr == "" {
			return fmt.Errorf("gob: invalid connStr: %s", connStr)
		}
		gob.setConnStr(connStr)
		return nil
	}
}
