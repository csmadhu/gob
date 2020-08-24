package gob

import (
	"context"
	"errors"
	"testing"
)

func TestNewGob(t *testing.T) {
	t.Run("defaultGob", func(t *testing.T) {
		got, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		want := &Gob{
			batchSize:  defaultBatchSize,
			dbProvider: DBProviderPg,
			connStr:    defaultConnStr,
		}

		testVerifyGob(t, got, want)
	})

	t.Run("customizedGob", func(t *testing.T) {
		want := &Gob{
			batchSize:  10,
			dbProvider: DBProviderPg,
			connStr:    defaultConnStr,
		}

		got, err := New(WithBatchSize(10), WithDBProvider(DBProviderPg), WithDBConnStr(defaultConnStr))
		if err != nil {
			t.Fatalf("init gob; err: %v", err)
		}

		testVerifyGob(t, got, want)
	})

	t.Run("invalidGob", func(t *testing.T) {
		if _, err := New(WithBatchSize(-1)); err == nil {
			t.Fatalf("init gob; want err")
		}

		if _, err := New(WithDBProvider("")); err == nil {
			t.Fatalf("init gob; want err")
		}

		if _, err := New(WithDBProvider("test")); err == nil {
			t.Fatalf("init gob; want err")
		}

		if _, err := New(WithDBConnStr("")); err == nil {
			t.Fatalf("init gob; want err")
		}

		if _, err := New(WithDBConnStr("postgres://postgres:postgres@localhost:5432/gob&pool_max_conns=1")); err == nil {
			t.Fatalf("init gob; want err")
		}
	})

	t.Run("closedGob", func(t *testing.T) {
		gob, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		gob.Close()
	})
}

func testVerifyGob(t *testing.T, got, want *Gob) {
	if got.batchSize != want.batchSize {
		t.Fatalf("batchSize got: %d want: %d", got.batchSize, want.batchSize)
	}

	if got.dbProvider != want.dbProvider {
		t.Fatalf("dbProvider got: %s want: %s", got.dbProvider, want.dbProvider)
	}

	if got.connStr != want.connStr {
		t.Fatalf("connStr got: %s want: %s", got.connStr, want.connStr)
	}

	if got.db == nil {
		t.Fatalf("nil db handler")
	}
}

func TestGobUpsert(t *testing.T) {
	t.Run("closedGob", func(t *testing.T) {
		gob, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		gob.Close()
		if err := gob.Upsert(context.Background(), "students", []string{"name"}, ConflictActionUpdate, nil); !errors.Is(err, ErrConnClosed) {
			t.Fatalf("error got: %v want: %v", err, ErrConnClosed)
		}
	})

	t.Run("emptyModel", func(t *testing.T) {
		gob, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		if err := gob.Upsert(context.Background(), "", []string{"name"}, ConflictActionUpdate, nil); !errors.Is(err, ErrEmptyModel) {
			t.Fatalf("error got: %v want: %v", err, ErrEmptyModel)
		}
	})

	t.Run("rowCountLessThanBatchsize", func(t *testing.T) {
		setupPgDB()
		gob, err := New(WithBatchSize(10))
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		rows := testGenStudentRows(1)
		if err := gob.Upsert(context.Background(), "students", []string{"name"}, ConflictActionUpdate, rows); err != nil {
			t.Fatalf("upsert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})

	t.Run("rowCountEqToBatchsize", func(t *testing.T) {
		setupPgDB()
		gob, err := New(WithBatchSize(10))
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		rows := testGenStudentRows(10)
		if err := gob.Upsert(context.Background(), "students", []string{"name"}, ConflictActionUpdate, rows); err != nil {
			t.Fatalf("upsert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})

	t.Run("rowCountGtThanBatchsize", func(t *testing.T) {
		setupPgDB()
		gob, err := New(WithBatchSize(10))
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		rows := testGenStudentRows(15)
		if err := gob.Upsert(context.Background(), "students", []string{"name"}, ConflictActionUpdate, rows); err != nil {
			t.Fatalf("upsert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})
}
