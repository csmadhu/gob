package gob

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/csmadhu/gob/utils"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	testPgDB      *pgxpool.Pool
	testPgConnStr = "postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1"
)

func init() {
	if err := setupPgDB(); err != nil {
		log.Fatalf("setup postgres; err: %v", err)
	}
}

func setupPgDB() error {
	pool, err := pgxpool.Connect(context.Background(), testPgConnStr)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL server: %w", err)
	}

	if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS students"); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	if _, err := pool.Exec(context.Background(), `CREATE TABLE students(
		id SERIAL PRIMARY KEY, 
		name VARCHAR(255) NOT NULL, 
		age INT, 
		profile JSONB, 
		subjects TEXT[],
		birthday TIMESTAMP WITH TIME ZONE,
		UNIQUE (name)
		)`); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	testPgDB = pool
	return nil
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
	Street  string `json:"street"`
	State   string `json:"state"`
	ZipCode int    `json:"zipcode"`
}

func testGenStudentRows(count int) (rows []Row) {
	for i := 0; i < count; i++ {
		row := NewRow()
		row.Add("name", fmt.Sprintf("name-%d", i))
		row.Add("age", i)
		row.Add("profile", studentProfile{Street: fmt.Sprintf("street-%d", i), State: fmt.Sprintf("state-%d", i), ZipCode: i})
		row.Add("subjects", []string{"english", "calculus"})
		row.Add("birthday", time.Now())

		rows = append(rows, row)
	}

	return rows
}

func testVerifyStudentRowsPg(t *testing.T, rows []Row) {
	for _, want := range rows {
		dbRows, err := testPgDB.Query(context.Background(), "SELECT name, age, profile, subjects FROM students WHERE name=$1", want.Value("name"))
		if err != nil {
			t.Fatalf("read student row: %s; err: %v", want.Value("name"), err)
		}
		defer dbRows.Close()

		var got Row
		for dbRows.Next() {
			var (
				name     string
				age      int
				profile  studentProfile
				subjects []string
			)
			if err := dbRows.Scan(&name, &age, &profile, &subjects); err != nil {
				t.Fatalf("scan student row: %s; err: %v", want.Value("name"), err)
			}
			got = NewRow()
			got.Add("name", name)
			got.Add("age", age)
			got.Add("profile", profile)
			got.Add("subjects", subjects)
		}

		if got == nil || got.Len() == 0 {
			t.Fatalf("row got: empty row want: %v", want)
		}

		got["birthday"] = want.Value("birthday")

		if err := dbRows.Err(); err != nil {
			t.Fatalf("scan student rows; err: %v", err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("student row: %s got: %+v want: %+v", want.Value("name"), got, want)
		}
	}
}

func TestNewPG(t *testing.T) {
	db, err := newPg(testPgConnStr)
	if err != nil {
		t.Fatalf("init PostgreSQL server err: %v", err)
	}

	db.close()
}

func TestUpsertPg(t *testing.T) {
	setupPgDB()
	db, err := newPg(testPgConnStr)
	if err != nil {
		t.Fatalf("init PostgreSQL server err: %v", err)
	}
	defer db.close()

	t.Run("zeroRows", func(t *testing.T) {
		if err := db.upsert(context.Background(), UpsertArgs{
			Model:  "students",
			KeySet: utils.NewStringSet("name"),
			Keys:   []string{"name"},
		}); err != nil {
			t.Fatalf("upsert zero rows err: %v", err)
		}
	})

	t.Run("emptyKeys", func(t *testing.T) {
		if err := db.upsert(context.Background(), UpsertArgs{
			Model: "students",
			Rows:  testGenStudentRows(10),
		}); !errors.Is(err, ErrEmptykeys) {
			t.Fatalf("emptyKeys got: %v want: %v", err, ErrEmptykeys)
		}
	})

	t.Run("conflictActionNothing", func(t *testing.T) {
		rows := testGenStudentRows(10)
		if err := db.upsert(context.Background(), UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			KeySet:         utils.NewStringSet("name"),
			Keys:           []string{"name"},
			Rows:           rows,
		}); err != nil {
			t.Fatalf("insert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})

	t.Run("conflictActionUpdate", func(t *testing.T) {
		rows := testGenStudentRows(15)
		if err := db.upsert(context.Background(), UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			KeySet:         utils.NewStringSet("name"),
			Keys:           []string{"name"},
			Rows:           rows,
		}); err != nil {
			t.Fatalf("insert rows err: %v", err)
		}

		testVerifyStudentRowsPg(t, rows)
	})

}

func TestRowToSQLPg(t *testing.T) {
	t.Run("conflictActionUpdate", func(t *testing.T) {
		wantSQL := "INSERT INTO students(age,birthday,name,profile,subjects) VALUES($1,$2,$3,$4,$5) ON CONFLICT (name) DO UPDATE SET age=$1,birthday=$2,profile=$4,subjects=$5"
		wantArgs := []interface{}{0, nil, "name-0", studentProfile{Street: "street-0", State: "state-0"}, []string{"english", "calculus"}}

		pg := &pg{}
		row := testGenStudentRows(1)[0]

		gotSQL, gotArgs := pg.rowToSQL(row, UpsertArgs{
			ConflictAction: ConflictActionUpdate,
			Model:          "students",
			Keys:           []string{"name"},
			KeySet:         utils.NewStringSet("name"),
		})

		if gotSQL != wantSQL {
			t.Fatalf("sql got: %s want: %s", gotSQL, wantSQL)
		}

		wantArgs[1] = gotArgs[1]

		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Fatalf("args got: %v want: %v", gotArgs, wantArgs)
		}
	})

	t.Run("conflictActionNothing", func(t *testing.T) {
		wantSQL := "INSERT INTO students(age,birthday,name,profile,subjects) VALUES($1,$2,$3,$4,$5) ON CONFLICT (name) DO NOTHING"
		wantArgs := []interface{}{0, nil, "name-0", studentProfile{Street: "street-0", State: "state-0"}, []string{"english", "calculus"}}

		pg := &pg{}
		row := testGenStudentRows(1)[0]

		gotSQL, gotArgs := pg.rowToSQL(row, UpsertArgs{
			ConflictAction: ConflictActionNothing,
			Model:          "students",
			Keys:           []string{"name"},
			KeySet:         utils.NewStringSet("name"),
		})

		if gotSQL != wantSQL {
			t.Fatalf("sql got: %s want: %s", gotSQL, wantSQL)
		}

		wantArgs[1] = gotArgs[1]

		if !reflect.DeepEqual(gotArgs, wantArgs) {
			t.Fatalf("args got: %v want: %v", gotArgs, wantArgs)
		}
	})

}
