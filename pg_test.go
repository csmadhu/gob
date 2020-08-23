package gob

import (
	"context"
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
	testPgConnStr = "postgres://postgres:postgres@localhost:5432/postgres?pool_max_conns=1"
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

	if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS foo"); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	if _, err := pool.Exec(context.Background(), `CREATE TABLE foo(
		id int PRIMARY KEY, 
		name VARCHAR(255) NOT NULL, 
		age INT, 
		profile JSONB, 
		subjects TEXT[],
		birthday TIMESTAMP WITH TIME ZONE
		)`); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	testPgDB = pool
	return nil
}

type foo struct {
	ID       int        `json:"id"`
	Name     string     `json:"name"`
	Age      int        `json:"age"`
	Profile  fooProfile `json:"profile"`
	Subjects []string   `json:"subjects"`
	Birthday time.Time  `json:"birthday"`
}

type fooProfile struct {
	Street  string `json:"street"`
	State   string `json:"state"`
	ZipCode int    `json:"zipcode"`
}

func testGenFooRows(count int) (rows []Row) {
	for i := 0; i < count; i++ {
		row := NewRow()
		row.Add("id", i)
		row.Add("name", fmt.Sprintf("foo-name-%d", i))
		row.Add("age", i)
		row.Add("profile", fooProfile{Street: fmt.Sprintf("street-%d", i), State: fmt.Sprintf("state-%d", i), ZipCode: i})
		row.Add("subjects", []string{"english", "calculus"})
		row.Add("birthday", time.Now())

		rows = append(rows, row)
	}

	return rows
}

func testVerifyFooRowsPg(t *testing.T, rows []Row) {
	for _, want := range rows {
		dbRows, err := testPgDB.Query(context.Background(), "SELECT id, name, age, profile, subjects FROM foo WHERE id=$1", want.Value("id"))
		if err != nil {
			t.Fatalf("read foo row: %d; err: %v", want.Value("id"), err)
		}
		defer dbRows.Close()

		var got Row
		for dbRows.Next() {
			var (
				id       int
				name     string
				age      int
				profile  fooProfile
				subjects []string
			)
			if err := dbRows.Scan(&id, &name, &age, &profile, &subjects); err != nil {
				t.Fatalf("scan foo row: %d; err: %v", want.Value("id"), err)
			}
			got = NewRow()
			got.Add("id", id)
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
			t.Fatalf("scan foo rows; err: %v", err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("foo row: %d got: %+v want: %+v", want.Value("id"), got, want)
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
	db, err := newPg(testPgConnStr)
	if err != nil {
		t.Fatalf("init PostgreSQL server err: %v", err)
	}
	defer db.close()

	if err := db.upsert(context.Background(), "foo", utils.NewStringSet("id"), nil); err != nil {
		t.Fatalf("upsert zero rows err: %v", err)
	}

	rows := testGenFooRows(10)
	if err := db.upsert(context.Background(), "foo", utils.NewStringSet("id"), rows); err != nil {
		t.Fatalf("insert rows err: %v", err.Error())
	}

	testVerifyFooRowsPg(t, rows)
}

func TestRowToSQLPg(t *testing.T) {
	wantSQL := "INSERT INTO foo(age,birthday,id,name,profile,subjects) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET age=$1,birthday=$2,name=$4,profile=$5,subjects=$6"
	wantArgs := []interface{}{0, nil, 0, "foo-name-0", fooProfile{Street: "street-0", State: "state-0"}, []string{"english", "calculus"}}

	pg := &pg{}
	row := testGenFooRows(1)[0]

	gotSQL, gotArgs := pg.rowToSQL("foo", utils.NewStringSet("id"), row)

	if gotSQL != wantSQL {
		t.Fatalf("sql got: %s want: %s", gotSQL, wantSQL)
	}

	wantArgs[1] = gotArgs[1]

	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("args got: %v want: %v", gotArgs, wantArgs)
	}

}
