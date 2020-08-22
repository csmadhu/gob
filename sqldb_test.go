package gob

import (
	"database/sql"
	"testing"
)

func TestNewSQLDB(t *testing.T) {
	g := NewSQL(&sql.DB{})
	if g.db == nil {
		t.Fatalf("gob db handler not set")
	}
}

func TestSQLInsert(t *testing.T) {
	g := NewSQL(&sql.DB{})

	if err := g.Insert("foo", nil); err != nil {
		t.Fatalf("insert nil rows err: %v", err)
	}

	if err := g.Update("foo", nil); err != nil {
		t.Fatalf("update nil rows err: %v", err)
	}
}
