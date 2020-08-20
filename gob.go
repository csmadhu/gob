package gob

import (
	"database/sql"
	"fmt"
)

// Gob provides APIs to upsert data in bulk
type Gob struct {
	sqlDB        *sql.DB                // connection handler to Relation Database
	modelsByName map[string]interface{} // registered models
}

func (gob *Gob) registerModel(name string, model interface{}) error {
	if model == nil {
		return fmt.Errorf("nil model; name: %s", name)
	}

	gob.modelsByName[name] = model
	return nil
}

// Option to customize Gob
type Option func(gob *Gob) error

// NewSQL returns Gob handler for relation database - Postgres, MySQL
//
// Gob upserts records using db connection
//
// options
func NewSQL(db *sql.DB, options ...Option) (*Gob, error) {
	gob := &Gob{
		sqlDB:        db,
		modelsByName: make(map[string]interface{}),
	}

	for _, option := range options {
		if err := option(gob); err != nil {
			return nil, err
		}
	}

	return gob, nil
}

// RegisterModel to Gob during initialization
func RegisterModel(name string, model interface{}) Option {
	return func(gob *Gob) error {
		return gob.registerModel(name, model)
	}
}
