package gob

import "errors"

var (
	// ErrConnClosed when Gob.Upsert is called after closing the connection
	ErrConnClosed = errors.New("gob: conn closed;")

	// ErrEmptyModel when Gob.Upsert is called with empty model
	ErrEmptyModel = errors.New("gob: empty model;")

	// ErrEmptykeys when Gob.Upsert is called with empty keys
	ErrEmptykeys = errors.New("gob: empty keys;")

	// ErrEmptyKeyspace when keyspace is not provided in DB URL
	ErrEmptyKeyspace = errors.New("gob: empty keyspace;")

	// ErrEmptyConnStr when connection string is empty
	ErrEmptyConnStr = errors.New("gob: empty connection string;")

	// ErrEmptyDBProvider when db provider is empty
	ErrEmptyDBProvider = errors.New("gob: empty db provider;")

	// ErrEmptyConflictAction when conflict action not specified
	ErrEmptyConflictAction = errors.New("gob: empty conflict action;")
)
