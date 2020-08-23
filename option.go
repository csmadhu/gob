package gob

import "fmt"

// Option to customize Gob
type Option func(gob *Gob) error

// BatchSize sets batchSize of Gob to size
func BatchSize(size int) Option {
	return func(gob *Gob) error {
		if size <= 0 {
			return fmt.Errorf("gob: invalid batchSize: %d", size)
		}
		gob.setBatchSize(size)
		return nil
	}
}

// DBType sets type of database to upsert rows
func DBType(dbType string) Option {
	return func(gob *Gob) error {
		if dbType == "" {
			return fmt.Errorf("gob: invalid dbType: %s", dbType)
		}

		gob.setDBType(dbType)
		return nil
	}
}

// DBConnStr sets type of database conn string
func DBConnStr(connStr string) Option {
	return func(gob *Gob) error {
		if connStr == "" {
			return fmt.Errorf("gob: invalid connStr: %s", connStr)
		}
		gob.setConnStr(connStr)
		return nil
	}
}
