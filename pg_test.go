package gob

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	testPgDB      *pgxpool.Pool
	testPgConnStr = "postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1"
	testPgArgs    = connArgs{
		connStr:      testPgConnStr,
		idleConns:    1,
		openConns:    1,
		connIdleTime: 3 * time.Second,
		connLifeTime: 3 * time.Second,
	}
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

func testGenStudentRowsPg(count int) (rows []Row) {
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
	db, err := newPg(testPgArgs)
	if err != nil {
		t.Fatalf("init PostgreSQL server; err: %v", err)
	}

	db.close()
}

func TestUpsertPg(t *testing.T) {
	setupPgDB()
	db, err := newPg(testPgArgs)
	if err != nil {
		t.Fatalf("init PostgreSQL server err: %v", err)
	}
	defer db.close()

	testUpsertDB(t, db, testGenStudentRowsPg, testVerifyStudentRowsPg)
}

func TestRowToSQLPg(t *testing.T) {
	wantSQLs := []string{
		"INSERT INTO students(age,birthday,name,profile,subjects) VALUES($1,$2,$3,$4,$5) ON CONFLICT (name) DO UPDATE SET age=$1,birthday=$2,profile=$4,subjects=$5",
		"INSERT INTO students(age,birthday,name,profile,subjects) VALUES($1,$2,$3,$4,$5) ON CONFLICT (name) DO NOTHING",
	}

	wantArgs := [][]interface{}{
		[]interface{}{0, nil, "name-0", studentProfile{Street: "street-0", State: "state-0"}, []string{"english", "calculus"}},
		[]interface{}{0, nil, "name-0", studentProfile{Street: "street-0", State: "state-0"}, []string{"english", "calculus"}},
	}

	pg := &pg{}
	testRowToSQL(t, testGenStudentRowsPg, pg.rowToSQL, wantSQLs, wantArgs)
}
