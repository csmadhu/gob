package gob

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/csmadhu/gob/utils"
)

func TestNewGob(t *testing.T) {
	t.Run("defaultGob", func(t *testing.T) {
		got, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		want := &Gob{
			batchSize:    defaultBatchSize,
			dbProvider:   DBProviderPg,
			connStr:      defaultConnStr,
			idleConns:    defaultIdleConns,
			openConns:    defaultOpenConns,
			connIdleTime: defaultConnIdleTime,
			connLifeTime: defaultconnLifeTime,
		}

		testVerifyGob(t, got, want)
	})

	t.Run("customizedGob", func(t *testing.T) {
		want := &Gob{
			batchSize:    10,
			dbProvider:   DBProviderPg,
			connStr:      defaultConnStr,
			idleConns:    defaultIdleConns,
			openConns:    defaultOpenConns,
			connIdleTime: defaultConnIdleTime,
			connLifeTime: defaultconnLifeTime,
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

	if got.idleConns != want.idleConns {
		t.Fatalf("idleConns got: %d want: %d", got.idleConns, want.idleConns)
	}

	if got.openConns != want.openConns {
		t.Fatalf("openConns got: %d want: %d", got.openConns, want.openConns)
	}

	if got.connIdleTime != want.connIdleTime {
		t.Fatalf("connIdleTime got: %v want: %v", got.connIdleTime, want.connIdleTime)
	}

	if got.connLifeTime != want.connLifeTime {
		t.Fatalf("connLifeTime got: %v want: %v", got.connLifeTime, want.connLifeTime)
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
		if err := gob.Upsert(context.Background(), UpsertArgs{}); !errors.Is(err, ErrConnClosed) {
			t.Fatalf("error got: %v want: %v", err, ErrConnClosed)
		}
	})

	t.Run("emptyModel", func(t *testing.T) {
		gob, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		if err := gob.Upsert(context.Background(), UpsertArgs{}); !errors.Is(err, ErrEmptyModel) {
			t.Fatalf("error got: %v want: %v", err, ErrEmptyModel)
		}
	})

	t.Run("emptyRow", func(t *testing.T) {
		gob, err := New()
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		if err := gob.Upsert(context.Background(), UpsertArgs{Model: "students"}); err != nil {
			t.Fatalf("empty row err: %v", err)
		}
	})

	t.Run("rowCountLessThanBatchsize", func(t *testing.T) {
		setupPgDB()
		gob, err := New(WithBatchSize(10))
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		rows := testGenStudentRowsPg(1)
		if err := gob.Upsert(context.Background(), UpsertArgs{
			Model:          "students",
			Rows:           rows,
			Keys:           []string{"name"},
			ConflictAction: ConflictActionUpdate,
		}); err != nil {
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

		rows := testGenStudentRowsPg(10)
		if err := gob.Upsert(context.Background(), UpsertArgs{
			Model:          "students",
			Rows:           rows,
			Keys:           []string{"name"},
			ConflictAction: ConflictActionUpdate,
		}); err != nil {
			t.Fatalf("upsert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})

	t.Run("rowCountGtThanBatchsize", func(t *testing.T) {
		setupPgDB()
		gob, err := New(WithBatchSize(1001))
		if err != nil {
			t.Fatalf("init default gob; err: %v", err)
		}

		rows := testGenStudentRowsPg(1501)
		if err := gob.Upsert(context.Background(), UpsertArgs{
			Model:          "students",
			Rows:           rows,
			Keys:           []string{"name"},
			ConflictAction: ConflictActionUpdate,
		}); err != nil {
			t.Fatalf("upsert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})
}

type student struct {
	ID       int            `json:"id"`
	Name     string         `json:"name"`
	Age      int            `json:"age"`
	Profile  studentProfile `json:"profile"`
	Subjects []string       `json:"subjects"`
	Birthday time.Time      `json:"birthday"`
}

type studentProfile struct {
	State   string `json:"state"`
	Street  string `json:"street"`
	ZipCode int    `json:"zipcode"`
}

func testUpsertDB(t *testing.T, dbConn db, genFn func(int) []Row, verifyFn func(t *testing.T, rows []Row)) {
	t.Run("zeroRows", func(t *testing.T) {
		if err := dbConn.upsert(context.Background(), UpsertArgs{
			Model:  "students",
			keySet: utils.NewStringSet("name"),
			Keys:   []string{"name"},
		}); err != nil {
			t.Fatalf("upsert zero rows err: %v", err)
		}
	})

	t.Run("emptyKeys", func(t *testing.T) {
		if err := dbConn.upsert(context.Background(), UpsertArgs{
			Model: "students",
			Rows:  genFn(10),
		}); !errors.Is(err, ErrEmptykeys) {
			t.Fatalf("emptyKeys got: %v want: %v", err, ErrEmptykeys)
		}
	})

	t.Run("conflictActionNothing", func(t *testing.T) {
		rows := genFn(10)
		if err := dbConn.upsert(context.Background(), UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			keySet:         utils.NewStringSet("name"),
			Keys:           []string{"name"},
			Rows:           rows,
		}); err != nil {
			t.Fatalf("insert rows err: %v", err)
		}

		verifyFn(t, rows)
	})

	t.Run("conflictActionUpdate", func(t *testing.T) {
		rows := genFn(15)
		if err := dbConn.upsert(context.Background(), UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			keySet:         utils.NewStringSet("name"),
			Keys:           []string{"name"},
			Rows:           rows,
		}); err != nil {
			t.Fatalf("insert rows err: %v", err)
		}

		verifyFn(t, rows)
	})
}

func testRowToSQL(t *testing.T, genFn func(int) []Row, rowToSQLfn func(Row, UpsertArgs) (string, []interface{}), wantSQLs []string, wantArgs [][]interface{}) {
	t.Run("conflictActionUpdate", func(t *testing.T) {
		row := genFn(1)[0]

		gotSQL, gotArgs := rowToSQLfn(row, UpsertArgs{
			ConflictAction: ConflictActionUpdate,
			Model:          "students",
			Keys:           []string{"name"},
			keySet:         utils.NewStringSet("name"),
		})

		if gotSQL != wantSQLs[0] {
			t.Fatalf("sql got: %s want: %s", gotSQL, wantSQLs[0])
		}

		for idx, arg := range wantArgs[0] {
			if arg == nil {
				wantArgs[0][idx] = gotArgs[1]
			}
		}

		if !reflect.DeepEqual(gotArgs, wantArgs[0]) {
			t.Fatalf("args got: %v want: %v", gotArgs, wantArgs[0])
		}
	})

	t.Run("conflictActionNothing", func(t *testing.T) {
		row := genFn(1)[0]

		gotSQL, gotArgs := rowToSQLfn(row, UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			Keys:           []string{"name"},
			keySet:         utils.NewStringSet("name"),
		})

		if gotSQL != wantSQLs[1] {
			t.Fatalf("sql got: %s want: %s", gotSQL, wantSQLs[1])
		}

		for idx, arg := range wantArgs[1] {
			if arg == nil {
				wantArgs[1][idx] = gotArgs[1]
			}
		}

		if !reflect.DeepEqual(gotArgs, wantArgs[1]) {
			t.Fatalf("args got: %v want: %v", gotArgs, wantArgs[1])
		}
	})

}
