package gob

import (
	"database/sql"
	"testing"
)

func TestNewSQL(t *testing.T) {
	modelFoo := struct{}{}

	if _, err := NewSQL(&sql.DB{}, RegisterModel("foo", &modelFoo)); err != nil {
		t.Fatalf("init gob err: %v", err)
	}

	if _, err := NewSQL(&sql.DB{}, RegisterModel("foo", &modelFoo), RegisterModel("bar", nil)); err == nil {
		t.Fatalf("expected nil model error")
	}
}
